package gov.pnnl.stucco.schema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.ornl.stucco.DBClient.DBConnection;

import com.tinkerpop.rexster.client.RexsterClient;


/**
 * Parses the Stucco ontology's JSON schema, in order to declare/initialize 
 * Gremlin properties and indexes.
 *   
 * @author Grant Nakamura, Feb 2015
 */
public class SchemaToGremlin {
    private static final Logger logger = LoggerFactory.getLogger(SchemaToGremlin.class);
    
    // Keys within Stucco's JSON Schema
    private static final String REF = "$ref";
    private static final String PROPERTIES = "properties";
    private static final String TITLE = "title";
    private static final String TYPE = "type";
    private static final String INDEXES = "indexes";
    private static final String COMPOSITE = "composite";
    private static final String MIXED = "mixed";
    private static final String TEXT = "text";
    private static final String KEYS = "keys";
    private static final String UNIQUE = "unique";
    private static final String DEFINITIONS = "definitions";
    private static final String ALL_OF = "allOf";
    private static final String MULTIPLICITY = "multiplicity";
    private static final String CARDINALITY = "cardinality";
    
    // $ref paths used to distinguish between vertices and edges 
    private static final String VERTEX_REF = "#/definitions/vertex";
    private static final String EDGE_REF = "#/definitions/edge";
    
    // JSON Schema data types
    private static final String OBJECT = "object";
    

    /** JSON Schema types (v4). Source: http://json-schema.org/latest/json-schema-core.html#anchor8. */
    private static String[] schemaTypes = { "array", "boolean", "integer", "number", OBJECT, "string" };
    
    /** Rexster types. Source: https://github.com/tinkerpop/rexster/wiki/Property-Data-Types. */
    private static String[] rexsterTypes = { "List", "Boolean", "Long", "Double", "Object", "String" };
//    /** JSON Schema types (v3). */
//    private static String[] schemaTypes = { "array", "boolean", "int", "long", "float", "decimal", "number", OBJECT, "string" };
//    
//    /** Rexster types. */
//    private static String[] rexsterTypes = { "List", "Boolean", "Integer", "Long", "Float", "Double", "Double", "Map", "String" };
        
    /** Map of JSONSchema property data types to Rexster types. */
    private static final Map<String, String> typeMap = new HashMap<String, String>();
    static {
        for (int i = 0; i < schemaTypes.length; i++) {
            typeMap.put(schemaTypes[i], rexsterTypes[i]);
        }
    }
    
    /** Number of seconds to pause after initiating connection. */
    private static final int WAIT_TIME = 1;
    
    /** Our gateway to Rexster. */
    private DBConnection dbConnection;
    
    /** Counter used to generate unique index names. */
    private int indexNumber = 0;

    /** Struct-like class for tracking what we've done with a property key. */
    private static class PropertyKeyMetadata {
        public String name;
        public String type;
        public String cardinality;
        public boolean declared;

        public PropertyKeyMetadata(String propertyName, String propertyType, String cardinality) {
            name = propertyName.trim();
            type = propertyType.trim();
            this.cardinality = normalizeCardinality(cardinality);
            declared = false;
        }
    }
    
    /** Map of property name to some parsing metadata for it. */
    private Map<String, PropertyKeyMetadata> propertyMap = new HashMap<String, PropertyKeyMetadata>();
    
//    private StringBuffer request = new StringBuffer();

    
    public SchemaToGremlin() {
    }
    
    /** 
     * Loads the ontology schema file, and parses it in order to prepare for
     * graph loading. The preparation includes declaration of properties and
     * initialization of indexes.
     * 
     * <p>The code uses knowledge of the general structure of Stucco's schema
     * file format, such as referenced base types and how allOf is being used.
     * It tries to avoid unnecessary references to individual node and edge 
     * entries.
     */
    public void parse(File ontologyFile) throws IOException {
        dbConnection = openRexsterConnection();
        
        // Load the schema file into a JSONObject
        String str = getOntologySchemaFileContent(ontologyFile);
        JSONObject root = new JSONObject(str);
        
        // NOTE: None of the ways of getting keys seems to retain the order from
        // the file. At a minimum, we want the base definitions first, because 
        // other entries could refer to them, so we make two passes.

//        startTransaction();
        
        // Scour the JSON for property types
        JSONObject definitions = root.getJSONObject(DEFINITIONS);
        for (Iterator<String> keys = definitions.keys(); keys.hasNext(); ) {
            // Get a key-value pair from the map.
            String key = keys.next();
            JSONObject item = definitions.optJSONObject(key);
            
            if (item != null) {
                if (item.has(PROPERTIES)) {
                    recordPropertiesOwnedBy(item);
                }
                if (item.has(ALL_OF)) {
                    recordPropertiesIn_AllOf(item);
                }
            }
        }
        
        // Declare labels and indexes + any properties the indexes need
        for (Iterator<String> keys = definitions.keys(); keys.hasNext(); ) {
            // Get a key-value pair from the map.
            String key = keys.next();
            JSONObject item = definitions.optJSONObject(key);
            
            if (item != null  &&  item.has(ALL_OF)) {
                process_AllOf_OwnedBy(item);
            }
        }

        // Declare any properties that weren't used in an index
        Collection<PropertyKeyMetadata> properties = propertyMap.values();
        for (PropertyKeyMetadata property : properties) {
            if (!property.declared) {
                declareProperty(property);
            }
        }

//        endTransaction();
        
        closeRexsterConnection();
    }
    
    /** Gets the ontology schema file content as a String. */
    private String getOntologySchemaFileContent(File schemaFile) throws IOException {
        try (BufferedReader in = new BufferedReader(new FileReader(schemaFile))) {
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
    
//    /** 
//     * Looks for properties declared at the top-level within a JSONObject. If 
//     * they are found, then parses them and any associated index declarations,
//     * translating into Gremlin declarations.
//     */
//    private void declarePropertiesOwnedBy(JSONObject propertiesOwner) {
//        JSONObject propertyMap = propertiesOwner.optJSONObject(PROPERTIES);
//        if (propertyMap != null) {
//            declarePropertiesIn(propertyMap);
//        }
//    }
    
    /**
     * Looks for properties declared at the top-level within a JSONObject, and
     * records their types.
     */
    private void recordPropertiesOwnedBy(JSONObject propertiesOwner) {
        JSONObject propertyMap = propertiesOwner.optJSONObject(PROPERTIES);
        if (propertyMap != null) {
            recordPropertiesIn(propertyMap);
        }
    }

    //TODO Handle possibility of edge indexes
    /** 
     * Looks for an allOf array declaration at the top level of a JSONObject. 
     * If one is found, then this method looks at each item within the array.
     * 
     * <p>Any vertex or edge references are processed to declare labels to
     * to Gremlin. 
     * 
     * <p>If any index entries are found, then this method determines which are
     * new. For each new index, the properties involved and the index are
     * declared to Gremlin in a single transaction. 
     * 
     * <p>Many indexes won't be needed because our current schema format lists
     * them redundantly. We had been thinking indexes could be specific to a 
     * vertex type, but that turns out to be impractical because of the 
     * requirement that the keys and index occur in a the same transaction. So
     * instead we are making the indexes general to all vertexes.
     */
    private void process_AllOf_OwnedBy(JSONObject arrayOwner) {
        String label = arrayOwner.optString(TITLE);

        JSONArray allOf = arrayOwner.optJSONArray(ALL_OF);
        int count = allOf.length();
        if (allOf != null  &&  count > 0) {
            // Look for $ref in first item of the allOf
            JSONObject refOwner = allOf.getJSONObject(0);
            String ref = refOwner.optString(REF);
            
            // Vertex and edge 
            if (ref.equals(VERTEX_REF)) { 
                declareVertexLabel(label);
            }
            else if (ref.equals(EDGE_REF)) {
                String multiplicity = arrayOwner.optString(MULTIPLICITY);
                declareEdgeLabel(label, multiplicity);
            }
            
            // Handle the properties section
            // (There should only be one, but we'll allow for more.)
            for (int i = 0; i < count; i++) {
                JSONObject propertiesOwner = allOf.getJSONObject(i);
                declareIndexesOwnedBy(propertiesOwner, label);
            }
        }
    }

    /**
     * Looks for properties declared within an allOf clause of a JSONObject, and
     * records their types.
     */
    private void recordPropertiesIn_AllOf(JSONObject arrayOwner) {
        JSONArray allOf = arrayOwner.optJSONArray(ALL_OF);
        int count = allOf.length();
        if (allOf != null  &&  count > 0) {           
            // Handle the properties section
            // (There should only be one, but we'll allow for more.)
            for (int i = 0; i < count; i++) {
                JSONObject propertiesOwner = allOf.getJSONObject(i);
                recordPropertiesOwnedBy(propertiesOwner);
            }
        }
    }
    
//    /**
//     * Issues declarations for the properties contained in (the first level of)
//     * the given JSONObject.
//     */
//    private void declarePropertiesIn(JSONObject propertyMap) {
//        // For each property
//        for (Iterator<String> iter = propertyMap.keys(); iter.hasNext();) {
//            String propertyName = iter.next();
//            
//            // Get cardinality and type from its metadata
//            JSONObject propertyMetadata = propertyMap.getJSONObject(propertyName);
//            String cardinality = propertyMetadata.optString(CARDINALITY);
//            String propertyType = propertyMetadata.optString(TYPE);
//            if (propertyType.isEmpty()) {
//                propertyType = OBJECT;
//            }
//            
//            // Create new property if it doesn't already exist
//            needProperty2(propertyName, propertyType, cardinality);
//        }       
//    }

    /**
     * Issues declarations for the properties contained in (the first level of)
     * the given JSONObject.
     */
    private void recordPropertiesIn(JSONObject propertyMap) {
        // For each property
        for (Iterator<String> iter = propertyMap.keys(); iter.hasNext();) {
            String propertyName = iter.next();
            
            // Get cardinality and type from its metadata
            JSONObject propertyMetadata = propertyMap.getJSONObject(propertyName);
            String cardinality = propertyMetadata.optString(CARDINALITY);
            String propertyType = propertyMetadata.optString(TYPE);
            if (propertyType.isEmpty()) {
                propertyType = OBJECT;
            }
            
            // Save property if we don't already have an entry for it
            recordProperty(propertyName, propertyType, cardinality);
        }       
    }
    
    /**
     * Records a property if it's new, or verifies consistency if a property
     * with that name has already been recorded.
     */
    private void recordProperty(String propertyName, String propertyType, String cardinality) {
        if (propertyName == null || propertyType == null || cardinality == null) {
            throw new NullPointerException();
        }
        
        PropertyKeyMetadata existing = propertyMap.get(propertyName);

        if (existing == null) {
            // Add new record
            PropertyKeyMetadata newRecord = new PropertyKeyMetadata(propertyName, propertyType, cardinality);
            propertyMap.put(propertyName, newRecord);
        }
        else {
            // Existing property; check for consistency
            cardinality = normalizeCardinality(cardinality);
            if (!propertyType.equals(existing.type)  ||  !cardinality.equals(existing.cardinality)) {
                throw new RuntimeException("Inconsistent property declaration");
            }
        }
    }
    
    /** Normalizes the value for cardinality. */
    private static String normalizeCardinality(String cardinality) {
        if (cardinality.isEmpty()) {
            cardinality = "single";
        }
        return cardinality;
    }

//    /** 
//     * Notes the need for a property. If the property doesn't , declaring it if it doesn't exist, or
//     * type-checking it if it does exist.
//     */
//    private boolean needProperty(String propertyName, String propertyType, String cardinality) {
//        if (propertyName == null || propertyType == null || cardinality == null) {
//            throw new NullPointerException();
//        }
//        
//        PropertyKeyMetadata metadata = propertyMap.get(propertyName);
//        String declaredType = getPropertyType(propertyName);
//        boolean needed = !propertyMap.containsKey(propertyName);
//        
//        if (declaredType == null) {
//            // New property
////            declareProperty(propertyName, propertyType, cardinality);
//            propertyMap.put(propertyName, propertyType);
//        }
//        else {
//            // Existing property; type check
//            if (!declaredType.equals(propertyType)) {
//                throw new RuntimeException("Property type mismatch");
//            }
//        }
//        
//        return needed;
//    }
//        
//    /** 
//     * Notes the need for a property, declaring it if it doesn't exist, or
//     * type-checking it if it does exist.
//     */
//    private boolean needProperty2(String propertyName, String propertyType, String cardinality) {
//        if (propertyName == null || propertyType == null || cardinality == null) {
//            throw new NullPointerException();
//        }
//        
//        String declaredType = propertyMap.get(propertyName);
//        boolean needed = !propertyMap.containsKey(propertyName);
//        
//        if (declaredType == null) {
//            // New property
//            declareProperty(propertyName, propertyType, cardinality);
//            propertyMap.put(propertyName, propertyType);
//        }
//        else {
//            // Existing property; type check
//            if (!declaredType.equals(propertyType)) {
//                throw new RuntimeException("Property type mismatch");
//            }
//        }
//        
//        return needed;
//    }
    
    /** Declares any needed indexes for the given ontology section. */
    private void declareIndexesOwnedBy(JSONObject indexesOwner, String vertexLabel) {
        JSONObject indexes = indexesOwner.optJSONObject(INDEXES);
        if (indexes != null) {
            JSONArray composite = indexes.optJSONArray(COMPOSITE);
            if (composite != null) {
                declareCompositeIndexes(composite, vertexLabel);
            }

            JSONArray mixed = indexes.optJSONArray(MIXED);
            if (mixed != null) {
                declareMixedIndexes(mixed);
            }
            
            JSONArray text = indexes.optJSONArray(TEXT);
            if (text != null) {
                // Text indexes are just mixed indexes
                declareMixedIndexes(text);
            }
        }
    }
    
    /** Declares any needed composite indexes. */
    private void declareCompositeIndexes(JSONArray composite, String vertexLabel) {
        int count = composite.length();
        for (int i = 0; i < count; i++) {
            JSONObject entry = (JSONObject) composite.get(i);
            JSONArray keyList = entry.getJSONArray(KEYS);
            boolean unique = entry.optBoolean(UNIQUE);
            
            String indexName = generateIndexName();
            declareCompositeIndex(indexName, keyList, unique, vertexLabel);
        }
    }

    /** Declares any needed mixed indexes. */
    private void declareMixedIndexes(JSONArray mixed) {
        int count = mixed.length();
        for (int i = 0; i < count; i++) {
            JSONObject entry = (JSONObject) mixed.get(i);
            JSONArray keyList = entry.getJSONArray(KEYS);
            
            String indexName = generateIndexName();
            declareMixedIndex(indexName, keyList);
        }
    }
    
    /** Auto-generates an index name. */
    private String generateIndexName() {
        return "index" + indexNumber++;
    }
    
//    /** 
//     * Clears the request buffer and writes the start of a new transaction
//     * to the buffer. 
//     */
//    private void startTransaction() {
//        request.setLength(0);
//        request.append("m = g.getManagementSystem();");
//    }
//    
//    private void endTransaction() {
//        request.append("m.commit();");
//        executeRexsterRequest(request.toString());
//    }

    /** Declares a new edge label. */
    private void declareEdgeLabel(String keyName, String multiplicity) {
        multiplicity = multiplicity.toUpperCase();
        
        StringBuilder request = new StringBuilder();
        request.append("m = g.getManagementSystem();");
        request.append(String.format("m.makeEdgeLabel('%s')", keyName));
        
        if (!multiplicity.isEmpty()) {
            request.append(String.format(".multiplicity(Multiplicity.%s)", multiplicity));
        }
        
        request.append(".make();");
        request.append("m.commit();");
        executeRexsterRequest(request.toString());
    }
    
    /** Declares a new vertex label. */
    private void declareVertexLabel(String keyName) {
        StringBuilder request = new StringBuilder();
        request.append("m = g.getManagementSystem();");
        request.append(String.format("m.makeVertexLabel('%s')", keyName));
        request.append(".make();");
        request.append("m.commit();");
        executeRexsterRequest(request.toString());
    }
    
    /** Declares a new property. */
    private void declareProperty(PropertyKeyMetadata property) {
        StringBuilder request = new StringBuilder();
        request.append("m = g.getManagementSystem();");
        
        String declaration = buildPropertyDeclaration(property);
        request.append(declaration);
        
        request.append("m.commit();");
        executeRexsterRequest(request.toString());
    }
    
    /** Builds the declaration of a new property. */
    private String buildPropertyDeclaration(PropertyKeyMetadata property) {
        String declaration = buildPropertyDeclaration(property.name, property.type, property.cardinality);
        return declaration;
    }
    
    /** Builds the declaration of a new property. */
    private String buildPropertyDeclaration(String propertyName, String propertyType, String cardinality) {
        // Convert to forms needed in request
        String className = typeMap.get(propertyType);
        cardinality = cardinality.trim().toUpperCase();

        StringBuilder declaration = new StringBuilder();
//        request.append("m = g.getManagementSystem();");
        declaration.append(String.format("m.makePropertyKey('%s')", propertyName));
        declaration.append(String.format(".dataType(%s.class)", className));
        
        if (!cardinality.isEmpty()) {
            declaration.append(String.format(".cardinality(Cardinality.%s)", cardinality));
        }
        declaration.append(".make();");
//        request.append("m.commit();");
        
//        executeRexsterRequest(request.toString());
        return declaration.toString();
    }

    /** Declares a new composite index. */
    private void declareCompositeIndex(String indexName, JSONArray keyList, boolean unique, String vertexLabel) {
        StringBuilder request = new StringBuilder();
        request.append("m = g.getManagementSystem();");
        
        String keysDeclaration = addPropertyKeysDeclaration(keyList);
        if (keysDeclaration.isEmpty()) {
            // Couldn't because one or more keys was already used
            return;
        }
        
        request.append(keysDeclaration);
        
        request.append(String.format("m.buildIndex('%s', Vertex.class)", indexName));
        
        int keyCount = keyList.length();
        for (int i = 0; i < keyCount; i++) {
            String key = keyList.getString(i);
            request.append(String.format(".addKey(m.getPropertyKey('%s'))", key));
        }
//        request.append(String.format(".indexOnly(m.getVertexLabel('%s'))", vertexLabel));
        
        if (unique) {
//            request.append(".unique()");
        }
        
        request.append(".buildCompositeIndex();");
        request.append("m.commit();");
        
        executeRexsterRequest(request.toString());
    }

    //TODO: Decide how to handle use of backend search index name
    //TODO: Decide what to do for "text" index
    /** Declares a new mixed index. */
    private void declareMixedIndex(String indexName, JSONArray keyList) {
        StringBuilder request = new StringBuilder();
        request.append("m = g.getManagementSystem();");
        
        String keysDeclaration = addPropertyKeysDeclaration(keyList);
        if (keysDeclaration.isEmpty()) {
            // Couldn't because one or more keys was already used
            return;
        }
        
        request.append(keysDeclaration);
       
        request.append(String.format("m.buildIndex('%s', Vertex.class)", indexName));
        
        int keyCount = keyList.length();
        for (int i = 0; i < keyCount; i++) {
            String key = keyList.getString(i);
            request.append(String.format(".addKey(m.getPropertyKey('%s'))", key));
        }

        request.append(".buildMixedIndex('search');");
        request.append("m.commit();");
        
        executeRexsterRequest(request.toString());
     }
    
    /** 
     * Adds property keys declarations to the Gremlin request being built.
     * 
     * @return Declaration (or "" if any key has already been used)
     */
    private String addPropertyKeysDeclaration(JSONArray keyList) {
        StringBuilder declaration = new StringBuilder();
        
        // For each key's metadata (which should exist since we already got all properties)
        int n = keyList.length();
        for (int i = 0; i < n; i++) {
            String propertyName = keyList.getString(i);
            PropertyKeyMetadata metadata = propertyMap.get(propertyName);
            
            // Check to see if the key has already been used
            if (metadata.declared) {
                return "";
            }
        }
        
        // All keys are new, so build the declaration for them
        
        for (int i = 0; i < n; i++) {
            String propertyName = keyList.getString(i);
            PropertyKeyMetadata metadata = propertyMap.get(propertyName);
            
            String propertyDeclaration = buildPropertyDeclaration(metadata);
            declaration.append(propertyDeclaration);
            
//            // Convert to forms needed in request
//            String className = typeMap.get(metadata.type);
//            String cardinality = metadata.cardinality.toUpperCase();
//    
//            declaration.append(String.format("m.makePropertyKey('%s')", propertyName));
//            declaration.append(String.format(".dataType(%s.class)", className));
//            if (!cardinality.isEmpty()) {
//                declaration.append(String.format(".cardinality(Cardinality.%s)", cardinality));
//            }
//            declaration.append(".make();");
            
            metadata.declared = true;
        }
        
        return declaration.toString();
    }

    /** Connects to Titan via the Rexster client interface. */
    public DBConnection openRexsterConnection() {
        DBConnection c = null;
        try {
            RexsterClient client = DBConnection.createClient(DBConnection.getTestConfig(), WAIT_TIME);
            c = new DBConnection( client );
        }
        catch (Exception e){
            // don't really care
            e.printStackTrace();
        } 
        
        return c;
    }
    
    /** Sends a request to Rexster. */
    private void executeRexsterRequest(String request) {
        logger.info("Making Rexster request: " + request);
        dbConnection.execute(request);
    }
    
    private void closeRexsterConnection() {
        RexsterClient client = dbConnection.getClient();
        DBConnection.closeClient(client);
    }

    public static void main(String... args) throws IOException {
        SchemaToGremlin loader = new SchemaToGremlin();
        File dir = new File("../ontology");
        loader.parse(new File(dir, "stucco_schema.json"));
    }
}






































