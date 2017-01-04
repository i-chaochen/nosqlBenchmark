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
 

public class WriteToHadoopFileSystem { 
 
    /** 
     * @param args 
     */ 
    public static void main(String[] args)throws Exception{ 
        // TODO Auto-generated method stub 
         
        String localSrc = args[0]; 
        String dst= args[1]; 
         

        InputStream in = new BufferedInputStream( new FileInputStream("localhost://home/hduser/Write.java")); 
         

        Configuration conf = new Configuration(); 

        

        FileSystem fs = FileSystem.get(new Path("hdfs://10.0.0.51:9090/user/hduser"), conf); 

        OutputStream out = fs.create(new Path(dst) ); 

        IOUtils.copyBytes(in, out, 4096, true); 
         
        System.out.println("Done"); 
         
    } 
 
} 