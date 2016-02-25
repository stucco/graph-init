package gov.pnnl.stucco.schema;

import gov.pnnl.stucco.dbconnect.DBConnectionFactory;
import gov.pnnl.stucco.dbconnect.DBConnectionIndexerInterface;

import java.io.IOException;

import com.orientechnologies.orient.core.exception.OCommandExecutionException;

public class IndexDefinitionsToDB {

    public IndexDefinitionsToDB() {
        // TODO Auto-generated constructor stub
    }
    public static void main(String[] args) {
        try {
            // get environment variables
            String type = System.getenv("STUCCO_DB_TYPE");
            if (type == null) {
                throw (new NullPointerException("Missing environment variable STUCCO_DB_TYPE"));
            }

            DBConnectionFactory factory = DBConnectionFactory.getFactory(DBConnectionFactory.Type.valueOf(type));

            String config = System.getenv("STUCCO_DB_CONFIG");
            if (config == null) {
                throw (new NullPointerException("Missing environment variable STUCCO_DB_CONFIG"));
            }
            factory.setConfiguration(config);
            
            String indexConfig = System.getenv("STUCCO_DB_INDEX_CONFIG");
            if (indexConfig == null) {
                throw (new NullPointerException("Missing environment variable STUCCO_DB_INDEX_CONFIG"));
            }
            
            DBConnectionIndexerInterface conn = factory.getDBConnectionIndexer();
            conn.open();
            
            // build the indexes
            conn.buildIndex(indexConfig);

        } catch (Exception e) {
            System.err.printf("Indexing failed\n");
            e.printStackTrace();
            System.exit(-1);
        }
    }

}
