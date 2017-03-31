import java.io.*;
import java.util.zip.*;

/**
 * This class defines two static methods for gzipping files and zipping
 * directories.  It also defines a demonstration program as a nested class.
 **/
public class Compress {
  public static void gzipFile(String from, String to) throws IOException {
    FileInputStream in = new FileInputStream("/home/hduser/workspace/DeDup/input/copy");
    GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream("/home/hduser/compress/test1"));
    byte[] buffer = new byte[4096];
    int bytes_read;
    while((bytes_read = in.read(buffer)) != -1) 
      out.write(buffer, 0, bytes_read);
    in.close();
    out.close();
  }

    public static void zipDirectory(String dir, String zipfile) 
       throws IOException, IllegalArgumentException {
        File d = new File(dir);
    if (!d.isDirectory())
      throw new IllegalArgumentException("Compress: not a directory:  " + dir);
    String[] entries = d.list();
    byte[] buffer = new byte[4096];  // Create a buffer for copying 
    int bytes_read;
    ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zipfile));
    for(int i = 0; i < entries.length; i++) {
      File f = new File(d, entries[i]);
      if (f.isDirectory()) continue;               // Don't zip sub-directories
      FileInputStream in = new FileInputStream(f); // Stream to read file
      ZipEntry entry = new ZipEntry(f.getPath());  // Make a ZipEntry
      out.putNextEntry(entry);                     // Store entry in zipfile
      while((bytes_read = in.read(buffer)) != -1)  // Copy bytes to zipfile
        out.write(buffer, 0, bytes_read);
      in.close();                                  // Close input stream
    }
    // When we're done with the whole loop, close the output stream
    out.close();
  }

  
  public static class Test {
    
    public static void main(String args[]) throws IOException {
      if ((args.length != 1) && (args.length != 2)) {  
        //System.err.println("Usage: java Compress$Test <from> [<>]");
        System.exit(0);
      }
      String from = args[0], to;
      File f = new File("/home/hduser/workspace/DeDup/input");
      boolean directory = f.isDirectory();   // Is it a file or directory?
      if (args.length == 2) to = args[1];    
      else {                                 // If destination not specified
        if (directory) to = from + ".zip";   //   use a .zip suffix
        else to = from + ".gz";              //   or a .gz suffix
      }

      if ((new File("/home/hduser/compress/test2")).exists()) { // Make sure not to overwrite anything
        System.err.println("Compress: won't overwrite existing file: " + to);
        System.exit(0);
      }

       if (directory) Compress.zipDirectory(from, to);
      else Compress.gzipFile(from, to);
    }
  }
}
