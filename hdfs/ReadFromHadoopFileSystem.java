import java.net.URI; 
 
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FSDataInputStream; 
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IOUtils; 
 

public class ReadFromHadoopFileSystem { 
 
    /** 
     * @param args 
     */ 
    public static void main(String[] args) throws Exception{ 
        // TODO Auto-generated method stub 
         

        String uri = args[0]; 

        Configuration conf = new Configuration(); 
        conf.set("hadoop.job.ugi", "hadoop-user,hadoop-user"); 
         

        FileSystem fs = FileSystem.get(URI.create(uri),conf); 
        FSDataInputStream in = null; 
        try{ 

            System.out.println("exp 1: output the read file"); 

            in = fs.open( new Path(uri) ); 

            IOUtils.copyBytes(in, System.out,50,false);   
            System.out.println(); 
             
             
            System.out.println(" exp 2: test seek"); 
             
            for (int i=1;i<=3;i++){ 
                in.seek(0+20*(i-1)); 
                System.out.println("seek "+i+" time" ); 
                IOUtils.copyBytes(in, System.out,4096,false);  
            } 
        }finally{ 
            IOUtils.closeStream(in); 
        } 
 
    } 
 
} 