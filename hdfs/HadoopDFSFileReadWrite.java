import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class HadoopDFSFileReadWrite {
  
  static void usage () {
    System.out.println("Usage test: HadoopDFSFileReadWrite <inputfile> <output file>");
    System.exit(1);
  }
  
  static void printAndExit(String str) {
    System.err.println(str);
    System.exit(1);
  }

  public static void main (String[] argv) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    if (argv.length != 2)
      usage();
    
    // Hadoop DFS deals with Path
    Path inFile = new Path("hdfs://10.0.0.51/usr/joe/wordcount/input/file01");
    Path outFile = new Path("hdfs://10.0.0.51/usr/joe/wordcount/output/file01");
   
    // Check if input/output are valid
    if (!fs.exists(inFile))
      printAndExit("Input file not found");
    if (!fs.isFile(inFile))
      printAndExit("Input should be a file");
    if (fs.exists(outFile))
      printAndExit("Output already exists");

    // Read from and write to new file
    FSDataInputStream in = fs.open(inFile);
    FSDataOutputStream out = fs.create(outFile);
    byte buffer[] = new byte[256];
    try {
      int bytesRead = 0;
      while ((bytesRead = in.read(buffer)) > 0) {
        out.write(buffer, 0, bytesRead);
      }
    } catch (IOException e) {
      System.out.println("Error while copying file");
    } finally {
      in.close();
      out.close();
    }
  }
}
