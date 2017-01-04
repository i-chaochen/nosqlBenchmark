import java.io.BufferedInputStream; 
import java.io.FileInputStream; 
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
 
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IOUtils; 
import org.apache.hadoop.util.Progressable; 

import java.util.Date;
import java.sql.Timestamp;
import java.util.Calendar;

public class FileSystemCat {
     public static void main(String[] args) throws Exception {
         String uri = args[0];
         Configuration configuration = new Configuration();
         FileSystem fs = FileSystem.get(URI.create(uri), configuration);
         InputStream in = null;
         long start_t = System.currentTimeMillis();
         try{
             in = fs.open(new Path(uri));
             					   // change as buffer
             IOUtils.copyBytes(in, System.out, 512000, false);
      } finally {

             IOUtils.closeStream(in);
          // finish time at here
        long end_t = System.currentTimeMillis();
        System.out.println(end_t);
        long pass_t ;
        pass_t = end_t - start_t;
        System.out.println("total time: "+ pass_t);
         }
    }
}



