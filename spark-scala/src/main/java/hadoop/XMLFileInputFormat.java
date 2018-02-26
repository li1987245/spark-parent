package hadoop;

import java.io.*;
  
import java.util.ArrayList;  
  
  
import org.apache.hadoop.fs.*;  
import org.apache.hadoop.io.compress.*;  
import org.apache.hadoop.mapred.FileInputFormat;  
import org.apache.hadoop.mapred.FileSplit;  
import org.apache.hadoop.mapred.InputSplit;  
import org.apache.hadoop.mapred.JobConf;  
import org.apache.hadoop.mapred.JobConfigurable;  
import org.apache.hadoop.mapred.RecordReader;  
import org.apache.hadoop.mapred.Reporter;  
  
public class XMLFileInputFormat extends FileInputFormat<FileID, XMLFileStatus>
  implements JobConfigurable {  
  
  private CompressionCodecFactory compressionCodecs = null;  
    
  public void configure(JobConf conf) {  
    compressionCodecs = new CompressionCodecFactory(conf);  
  }  
    
  protected boolean isSplitable(FileSystem fs, Path file) {  
    return false;  
  }  
  
  /** Splits files returned by {@link #listStatus(JobConf)} when 
   * they're too big.*/   
  public InputSplit[] getSplits(JobConf job, int numSplits)  
    throws IOException {  
    FileStatus[] files = listStatus(job);  
      
    long totalSize = 0;                           // compute total size  
    for (FileStatus file: files) {                // check we have valid files  
      if (file.isDir()) {  
        throw new IOException("Not a file: "+ file.getPath());  
      }  
      totalSize += file.getLen();  
    }  
  
    // generate splits  
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);  
      
    for (FileStatus file: files) {  
      Path path = file.getPath();  
      FileSystem fs = path.getFileSystem(job);  
      long length = file.getLen();  
      BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);  
              
            if (length != 0) {  
        splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));  
      } else {   
        //Create empty hosts array for zero length files  
        splits.add(new FileSplit(path, 0, length, new String[0]));  
      }  
    }  
    LOG.debug("Total # of splits: " + splits.size());  
    return splits.toArray(new FileSplit[splits.size()]);  
  }  
  
  public RecordReader<FileID, XMLFileStatus> getRecordReader(
                                          InputSplit split,   
                                          JobConf job,  
                                          Reporter reporter)  
    throws IOException{  
      reporter.setStatus(split.toString());  
      return new XMLFileRecordReader(job, ((FileSplit) split).getPath());  
  }  
}  