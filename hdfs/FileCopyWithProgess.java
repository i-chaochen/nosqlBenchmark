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


public class FileCopyWithProgress {

     public static void main(String[] args) throws Exception {
        String localSrc = args[0]; //"input file";
        String dst = args[1]; // "hdfs://10.0.0.51/user/hduser/hello.txt"
        long data_size = 8000000;
        
        // start time at here
        long start_t = System.currentTimeMillis();
        System.out.println(start_t);
        int req_x = 1000;
        int x = 0;
        while(x<req_x){
         InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
         Configuration conf = new Configuration();
         FileSystem fs = FileSystem.get(URI.create(dst), conf);

         OutputStream out = fs.create(new Path(dst), new Progressable() {
             public void progress() {
              //   System.out.print("Finish it!");
             }
         });

         IOUtils.copyBytes(in, out, data_size, true);
            x++;
        }

        // finish time at here
        long end_t = System.currentTimeMillis();
        System.out.println(end_t);
        long pass_t ;
        pass_t = end_t - start_t;
        System.out.println("total time: "+ pass_t);
        double each_t;

        each_t = data_size*req_x/pass_t;
        System.out.println("each time: "+each_t);
     }
 }



