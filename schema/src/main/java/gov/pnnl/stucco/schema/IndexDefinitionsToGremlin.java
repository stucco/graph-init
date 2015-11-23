package gov.pnnl.stucco.schema;

import gov.ornl.stucco.DBClient.DBConnection;
import gov.pnnl.stucco.utilities.CommandLine;
import gov.pnnl.stucco.utilities.CommandLine.UsageException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.rexster.client.RexProException;
import com.tinkerpop.rexster.client.RexsterClient;


/**
 * Parses the Stucco index specification, in order to issue Gremlin requests for
 * setting up indexes. The index specification format is assumed to be the 
 * following JSON:
 * 
 * <pre><block>
 * 
 * "indexes": [ 
 *   {
 *     type: ("NOTUNIQUE"|"FULLTEXT")
 *     keys: [
 *       {
 *         "name": propertyName
 *         "class": ("String"|"Character"|"Boolean"|"Byte"|"Short"|"Integer"|"Long"|"Float"|"Double"|"Decimal"|"Precision"|"Geoshape")
 *         "cardinality": ("SINGLE"|"LIST"|"SET")
 *       }
 *       ...
 *     ]
 *   }
 *   ...
 * ]
 *         
 * </block></pre>
 */
public class IndexDefinitionsToGremlin {
    private static final Logger logger = LoggerFactory.getLogger(IndexDefinitionsToGremlin.class);

    // Keys within the index specification JSON
    private static final String INDEXES = "indexes";
    private static final String TYPE = "type";
    private static final String KEYS = "keys";
    private static final String NAME = "name";
    private static final String CLASS = "class";
    private static final String CARDINALITY = "cardinality";
    
    // Other String constants
    private static final String STRING = "String";
    private static final String NOTUNIQUE = "NOTUNIQUE";
    private static final String FULLTEXT = "FULLTEXT";

    /** Number of seconds to pause after initiating connection. */
    private static final int WAIT_TIME = 1;
    
    /** Our gateway to the DB. */
    private DBConnection dbConnection;
    
    /** 
     * If true, use the test config for the DBConnection; otherwise
     * use the default config.
     */
    private boolean testMode;

    /** Counter used to generate unique index names. */
    private int indexNumber = 0;

    
    private IndexDefinitionsToGremlin() {
        // Instantiable but only from this file    
    }
    
    private void setTestMode(boolean flag) {
        testMode = flag;
    }
    
    /** 
     * Loads the index specification file, and parses it in order to prepare for
     * graph loading. The preparation includes declaration of indexes and of any
     * properties used in the indexes.
     */
    private void parse(File file) throws IOException {
        dbConnection = openRexsterConnection();
        
        // Load the schema file into a JSONObject
        String str = getTextFileContent(file);
        JSONObject root = new JSONObject(str);
        
        // Get the index entries from the JSON
        JSONArray indexes = root.optJSONArray(INDEXES);
        if (indexes == null) {
            logger.error("Expected 'indexes' key");
        }
        
        // Handle each index
        int count = indexes.length();
        for (int i = 0; i < count; i++) {
            JSONObject indexSpec = indexes.getJSONObject(i);
            parseIndexSpec(indexSpec);
        }
        
        closeRexsterConnection();
    }

    /**
     * Parses a single index specification, declaring it, and any properties it
     * uses, to DB. 
     */
    private void parseIndexSpec(JSONObject indexSpec) {
        StringBuilder request = new StringBuilder();
        
        String type = indexSpec.getString(TYPE);
        JSONArray keys = indexSpec.getJSONArray(KEYS);
        
        List<String> propertyNames = new ArrayList<String>();
        String propertyDeclarations = buildPropertyDeclarations(keys, propertyNames);
        request.append(propertyDeclarations);
        
        buildIndexDeclaration(type, propertyNames);
    }

    /**
     * Builds Gremlin declaration for the properties used in an index.
     * 
     * @param keys           The keys in JSON form
     * @param propertyNames  (OUT) The names of the keys, returned by side effect
     * 
     * @return Gremlin declaration of properties
     */
    private String buildPropertyDeclarations(JSONArray keys, List<String> propertyNames) {
        StringBuilder declarations = new StringBuilder();
        
        // For each key
        int keyCount = keys.length();
        for (int i = 0; i < keyCount; i++) {
            JSONObject keySpec = keys.getJSONObject(i);
            
            // Get the property name
            String property = keySpec.getString(NAME);
            propertyNames.add(property);
            
            // Get the class type, defaulting to String
            String classType = keySpec.optString(CLASS);
            if (classType == null) {
                classType = STRING;
            }
            
            // Get the cardinality, defaulting to SINGLE
            String cardinality = keySpec.optString(CARDINALITY);
            if (cardinality == null) {
                cardinality = "";
            }

            // Make the property declaration for the key
            String declaration = buildPropertyDeclaration(property, classType, cardinality);
            this.dbConnection.<OrientDynaElementIterable>executeSQL(declaration);
            declarations.append(declaration);
        }
        
        return declarations.toString();
    }
    
    /** 
     * Builds the declaration of a new property. 
     * 
     * @param propertyName  Name of property
     * @param classType     Class type of property
     * @param cardinality   Cardinality of property ("" means don't declare) 
     */
    private String buildPropertyDeclaration(String propertyName, String classType, String cardinality) {
        // Normalize to forms needed in request
        propertyName = propertyName.trim();
        classType = capitalize(classType.trim());
        cardinality = cardinality.trim().toUpperCase();

        // Build the declaration
        String declaration = String.format("CREATE PROPERTY V.%s %s", propertyName, classType);

        return declaration;
    }
    
    /** Builds a DB index declaration. */
    private void buildIndexDeclaration(String indexType, List<String> propertyKeys) {
        
        for(String key : propertyKeys) {
            // Make up a name
            String indexName = generateIndexName();        
            String declaration = String.format("CREATE INDEX %s ON V (%s) %s", indexName, key, indexType);
            this.dbConnection.<Integer>executeSQL(declaration);
        }
        
    }
    
    /** 
     * Builds the partial Gremlin declaration for adding property keys to an
     * index.
     */
    private String buildAddKeysDeclaration(List<String> propertyKeys) {
        StringBuilder declaration = new StringBuilder();
        for (String key : propertyKeys) {
            declaration.append(String.format(".addKey(m.getPropertyKey('%s'))", key));
        }
        
        return declaration.toString();
    }

    /** Auto-generates an index name. */
    private String generateIndexName() {
        return "index" + indexNumber++;
    }
    
    /** Converts a String to first character uppercase, remainder lowercase. */
    private static String capitalize(String str) {
        if (str.isEmpty()) {
            return str;
        }
        
        String first = str.substring(0, 1);
        String rest = str.substring(1);
        String capitalized = first.toUpperCase() + rest.toLowerCase();
        return capitalized;
    }

    /** Connects to Titan via the Rexster client interface. */
    public DBConnection openRexsterConnection() {
        DBConnection c = null;
        try {
            Configuration config = testMode?  DBConnection.getTestConfig() : DBConnection.getDefaultConfig();
            OrientGraph graph = DBConnection.getOrientGraph(config);
            c = new DBConnection( graph );
        }
        catch (Exception e){
            // don't really care
            e.printStackTrace();
        } 
        
        return c;
    }

    private void closeRexsterConnection() {
        OrientGraph client = dbConnection.getGraph();
        DBConnection.closeGraph(client);        
    }

    /** Sends a request to DB. */
    private void executeRexsterRequest(String request) {
        logger.info("Making DB request: " + request);
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                dbConnection.executeSQL(request);
                logger.info("    DB request succeeded");
                return;
            } 
            catch (OCommandExecutionException e) {
                logger.error("    DB request failed: " + e); 
            }
        }
        logger.error("    Skipping DB request after 3 failed attempts.");
    }
    
    /** Gets a text file's content as a String. */
    private String getTextFileContent(File textFile) throws IOException {
        try (BufferedReader in = new BufferedReader(new FileReader(textFile))) {
            StringBuilder builder = new StringBuilder();
            String eol = System.getProperty("line.separator");
            String line;
            while ((line = in.readLine()) != null) {
                builder.append(line);
                builder.append(eol);
            }
            
            String str = builder.toString();
            return str;
        }
    }
    
    public static void main(String[] args) {
        try {
            CommandLine parser = new CommandLine();
            parser.add0("-test");
            parser.add1("-spec");
            parser.parse(args);
            boolean useTestConfig = parser.found("-test");
            
            IndexDefinitionsToGremlin loader = new IndexDefinitionsToGremlin();
            if (useTestConfig) {
                loader.setTestMode(true);
            }
            
            String specFile = "stucco_indexing.json";
            File dir = null;
            if (parser.found("-spec")) {
                String specPath = parser.getValue();
                dir = new File(specPath);
            }
            else {
                // assume the default location
                System.err.println("Using default directory");
                dir = new File(".");
            }
            
            // load the schema and create the indexes
            loader.parse(new File(dir, specFile));
        } 
        catch (IOException e) {
            System.err.printf("Error in opening file, path or file does not exist: %s\n", e.toString());
            System.exit(-1);
        }
        catch (UsageException e) {
            System.err.println("Usage: [-test] -spec <directory>");
            System.exit(-1);
        }
    }

}
