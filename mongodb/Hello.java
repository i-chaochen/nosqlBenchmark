import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;
import java.util.Arrays;


public class Hello {
    public static void main(String args[]){

        System.out.println("Hello");

        try{
            MongoClient mongoClient = new MongoClient();
            // connect to database
            DB db = mongoClient.getDB("test");
            System.out.println("Connect it !");

            // get collection
            DBCollection coll = db.getCollection("testlog");
            System.out.println("Collection is got!");

            // a 8 kb array
            // n*512 as n kb
            byte[] simpletest;
            simpletest = new byte[8*512];

            // insert document
            BasicDBObject doc = new BasicDBObject("1", simpletest)
                    ;

            coll.insert(doc);
           System.out.println("Document is inserted");

        }catch(Exception e){
            System.err.println(e.getClass().getName() + ":" + e.getMessage());
        }


    }
}
