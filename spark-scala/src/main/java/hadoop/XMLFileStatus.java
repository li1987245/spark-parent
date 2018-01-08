package hadoop;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

/**
 * Created by jinwei on 17-12-13.
 */
public class XMLFileStatus extends FileStatus {
    public FSDataInputStream fsInputStream;//change into public from private


    public FSDataInputStream getInputStream() {
        return fsInputStream;
    }

    public void setInputStream(FSDataInputStream in) {
        this.fsInputStream = in;
    }
}
