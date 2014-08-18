package se.gbif.izeure;

import java.util.HashMap;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import org.apache.commons.httpclient.methods.*;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.text.ParseException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import org.apache.commons.httpclient.HttpClient;
import org.xml.sax.SAXException;
import se.gbif.bourgogne.models.Entities;
import se.gbif.bourgogne.utilities.Gz;

/**
 * 
 * @author korbinus
 */
public class Izeure 
{
    private static int datasetid;
    private static EntityManagerFactory emf;
    private static EntityManager em;
    private static Gson gson = new Gson();          // initialize it once
    private static Type bodyType = new TypeToken<HashMap<Integer, JsonObject>>() {
    }.getType();
    
    // solr with vanilla url and port
    private static final String solrURL = "http://localhost:8983/solr/update/json";
    private static final HttpClient httpClient = new HttpClient();
    
    
    public static void main( String[] args ) throws ParseException, ParserConfigurationException, SAXException, IOException, XPathExpressionException
    {
        // dev only
        boolean allDeletedRecords = false;
        
        int batchCommitSize = 2000;

        /*
         * local database setup
         */
        emf = javax.persistence.Persistence.createEntityManagerFactory("bourgognePU");
        em = emf.createEntityManager();
        
        List<Entities> entities;
        int errors = 0;                 // number of errors
        long offset = (long)0;          // where we are in the reading of the dataset/database
        int page = 50000;               // read batches of 50 000 records from Bourgogne
        Long totalNumberOfRecords;      // total number of records to process
        
        // if a configuration file is provided, we read it an start the indexing
        // otherwise we do an (re)indexing of the complete Bourgogne database.
        File f = null;
        if (args.length > 0) {
            System.out.println("Reading configuration for resource " + args[0]);
            f = new File(args[0]);
            // get the dataset id from configuration file
            Config config = new Config();
            config.read(f);
            datasetid = config.getId();
            
            // get the total number of records
            totalNumberOfRecords = getNumberOfRecords(datasetid);
            System.out.println("Total number of records to be treated: " + totalNumberOfRecords);
            
            while(offset < totalNumberOfRecords) {
                System.out.println("Treating offset " + offset);
                
                // get list of records
                entities = getDatasetRecords(datasetid, offset, page); 
                
                // send records to solr
                errors += entitiesToSolr(entities, batchCommitSize);
                System.out.println("Total number of errors so far: " + errors);
                offset += page;
            }
        } else {
            System.out.println("WARNING: no configuration provided, reindexing the whole bourgogne database");
            
            // get the total number of records
            totalNumberOfRecords = getNumberOfRecords();
            System.out.println("Total number of records to be treated: " + totalNumberOfRecords);
            
            while(offset < totalNumberOfRecords) {
                System.out.println("Treating offset " + offset);
                
                // get list of records
                entities = getAllRecords(offset, page); 
                
                // send records to solr
                errors += entitiesToSolr(entities, batchCommitSize);
                System.out.println("Total number of errors so far: " + errors);
                offset += page;
            }
            allDeletedRecords = true;
        }
        
        
        // Remove deleted records from the index
        // Warning: entities is now a list of deleted records
        if(allDeletedRecords) {
            entities = getAllDeletedRecords();
        } else {
            entities = getDatasetDeletedRecords(datasetid);
        }
        
        int i = 0;
        errors = 0;             // reset the number of errors
        int statusCode;
        
        if (entities.size() > 0) {
            System.out.println(entities.size() + " records to be removed from index");
            String occurrenceID;                // the occurrence ID as a string

            for(Entities ent : entities) {
                    i++;
                    occurrenceID = (new PID(ent.getId())).toString();

                    System.out.println("Removing " + occurrenceID);
                    // send order to remove the record from the index
                    statusCode = sendSolrQuery("{\"delete\":{\"query\":\"dwc_occurrenceID:"+ occurrenceID +"\"}}");
                    if(statusCode != 200) {
                        errors++;
                    }
               
                if(i % batchCommitSize == 0 && i > 0) {
                    commitSolr();
                }
            }
            
            // Commit remaining stuff
            commitSolr();
            getProcessStatus(i, errors);
        }

    }
    
    /**
     * Run through a list of records and send the last version of each one 
     * for indexing in solr
     * @param entities
     * @param batchCommitSize
     * @return 
     */
    private static int entitiesToSolr(List<Entities> entities, int batchCommitSize){
        int i = 0;
        int errors = 0;
        int size;
        int statusCode;

        JsonObject lastDwC;                                     // read: last DarwinCore
        HashMap<Integer, JsonObject> body  = new HashMap<>();   // map of version numbers and JsonObjects
        String dwcAdapted;                                      // we'll send it to solr
             
        for(Entities ent : entities) {
            try {
                i++;
                body = gson.fromJson(Gz.decompress(ent.getBody()), bodyType);
            
                // Get the last version of body
                size = body.size();
                lastDwC = body.get(size).getAsJsonObject("dwr:SimpleDarwinRecord");                    // get the last version
                
                // add datasetID in the JSONObject
                lastDwC.addProperty("datasetID", Integer.toString(datasetid));
                
                // add version number in the JSONObject
                lastDwC.addProperty("bourgogneVersion", size);

                // make Solr-compatible the different prefixes used by DarwinCore
                dwcAdapted = fixDwCPrefixes(lastDwC.toString());
                
                statusCode = sendSolrQuery("["+dwcAdapted+"]");
                if(statusCode != 200) {
                    errors++;
                }

            } catch (IOException e) {
                System.err.println("Error with record " + ent.getId().toString());
                System.err.println(e.getMessage());
                errors++;
            }
            
            if(i % batchCommitSize == 0 && i > 0) {
                commitSolr();
            }
        }

        // Commit remaining stuff
        commitSolr();
        
        getProcessStatus(i, errors);
        
        return errors;
        
    }
    
    /**
     * Commit changes in solr
     */
    private static void commitSolr(){
        PostMethod postMethod = new PostMethod(solrURL + "?commit=true");
        try {
            httpClient.executeMethod(postMethod);           
        }catch (IOException e) {
            System.err.println("Error with commit");
            System.err.println(e.getMessage());
        }        
    }
    
    /**
     * Send query to Solr
     * @param query
     * @return HTTP status returned by solr; any other value than 200 is an error
     */
    private static int sendSolrQuery(String query) {
        int statusCode;
        try {
            StringRequestEntity requestEntity = new StringRequestEntity(
                query,
                "application/json",
                "UTF-8");

             PostMethod postMethod = new PostMethod(solrURL);
             postMethod.setRequestEntity(requestEntity);

             statusCode = httpClient.executeMethod(postMethod);            
             if(statusCode != 200) {
                 System.out.println("Error: "+postMethod.getResponseBodyAsString());
             }   
        } catch(IOException e) {
            System.err.println("Exception: "+e.getMessage());
            statusCode = -1;        // whatever value different from 200
        }
        return statusCode;
    }
    
    /**
     * Display the status of the process: number of records treated and number
     * of errors encountered
     * @param treated
     * @param errors 
     */
    private static void getProcessStatus(int treated, int errors) {
        System.out.println(treated +" records treated.");
        if (errors > 0) {
            System.out.println(errors + " errors during indexing.");
        }        
    }
    
    /**
     * Helper for cleaning a DwC string from ':' that are incompatible with Solr
     * @param str
     * @return 
     */
    private static String fixDwCPrefixes(String str) {
        return str.replace("dc:", "dc_").replace("dwc:", "dwc_").replace("ext:", "ext_");
    }
    
    /**
     * Get the number of records in Bourgogne for the provided datasetid
     * @param datasetid int id of the dataset within Bourgogne to be indexed in solr
     * @return the number of records
     */
    private static Long getNumberOfRecords(int datasetid) {
        return em.createQuery("SELECT COUNT(e) FROM Entities e WHERE e.datasetid = ?1 AND e.deleted = ?2", Long.class)
                .setParameter(1, datasetid).setParameter(2, false)
                .setHint("eclipselink.read-only", true)
                .getSingleResult();
    }

    /**
     * Get the total number of records in Bourgogne
     * @return the number of records
     */
    private static Long getNumberOfRecords() {
        return em.createQuery("SELECT COUNT(e) FROM Entities e WHERE e.deleted = ?1", Long.class)
                .setParameter(1, false)
                .setHint("eclipselink.read-only", true)
                .getSingleResult();
    }
    
    /**
     * Get the records as a list for the provided dataset
     * @param em
     * @param datasetid
     * @return 
     */
    private static List<Entities> getDatasetRecords(int datasetid){
        return em.createQuery("SELECT e FROM Entities e WHERE e.datasetid = ?1 AND e.deleted = ?2", Entities.class)
                .setParameter(1, datasetid).setParameter(2, false)
                .setHint("eclipselink.read-only", true)
                .getResultList();
    }

    /**
     * Get the records as a list for the provided dataset with pagination
     * @param em
     * @param datasetid
     * @return 
     */
    private static List<Entities> getDatasetRecords(int datasetid, long offset, int size){
        return em.createNativeQuery("SELECT b.* FROM (SELECT e.added_id FROM entities e WHERE e.datasetid = ?1 AND e.deleted = ?2 LIMIT ?3,?4) a LEFT JOIN entities b USING(added_id)", Entities.class)                
                .setParameter(1, datasetid).setParameter(2, false)
                .setParameter(3, offset)
                .setParameter(4, size)
                .setHint("eclipselink.read-only", true)
                .getResultList();
    }
    
    /**
     * Get all the records for a complete (re)indexing
     * @param em
     * @return 
     */
    private static List<Entities> getAllRecords() {
        return em.createQuery("SELECT e FROM Entities e WHERE e.deleted = ?1", Entities.class)
                .setParameter(1, false)
                .setHint("eclipselink.read-only", true)
                .getResultList();
    }

    /**
     * Get all the records for a complete (re)indexing with pagination
     * @param em
     * @return 
     */
    private static List<Entities> getAllRecords(long offset, int size) {
        return em.createNativeQuery("SELECT b.* FROM (SELECT e.added_id FROM entities e WHERE e.deleted = ?1 LIMIT ?2,?3) a LEFT JOIN entities b USING(added_id)", Entities.class)
                .setParameter(1, false)
                .setParameter(2,offset)
                .setParameter(3,size)                
                .setHint("eclipselink.read-only", true)
                .getResultList();
    }
    
    /**
     * Get a list of records flagged as deleted for the provided dataset
     * @param em
     * @param datasetid
     * @return 
     */
    private static List<Entities> getDatasetDeletedRecords (int datasetid) {
        return em.createQuery("SELECT e FROM Entities e WHERE e.datasetid = ?1 AND e.deleted = ?2", Entities.class)
                .setParameter(1, datasetid).setParameter(2, true)
                .setHint("eclipselink.read-only", true)
                .getResultList();
    }
    
    /**
     * Get a list of all deleted records
     * @param em
     * @return 
     */
    private static List<Entities> getAllDeletedRecords () {
        return em.createQuery("SELECT e FROM Entities e WHERE e.deleted = ?1", Entities.class)
                .setParameter(1, true)
                .setHint("eclipselink.read-only", true)
                .getResultList();
    }    
}
