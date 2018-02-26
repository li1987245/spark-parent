package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;


public class XMLFileRecordReader implements RecordReader<FileID, XMLFileStatus> {
    private Path filePath;
    private FSDataInputStream fsInputStream;

    public Text temp;

    public XMLFileRecordReader(Configuration job, Path p) throws IOException {
        filePath = p;
        temp = new Text(p.toString());
        FileSystem fs = filePath.getFileSystem(job);
        fsInputStream = fs.open(filePath);
    }

    /**
     * Reads the next key/value pair from the input for processing.
     *
     * @param k     the key to read data into
     * @param value the value to read data into
     * @return true iff a key/value was read, false if at EOF
     */
    public boolean next(FileID k, XMLFileStatus value) throws IOException {
        if (filePath != null) {
            value.setInputStream(fsInputStream);
            filePath = null;
            return true;
        }
        return false;
    }

    /**
     * Create an object of the appropriate type to be used as a key.
     *
     * @return a new key object.
     */
    public FileID createKey() {
        return new FileID(temp);
    }

    /**
     * Create an object of the appropriate type to be used as a value.
     *
     * @return a new value object.
     */
    public XMLFileStatus createValue() {
        return new XMLFileStatus();
    }

    /**
     * Returns the current position in the input.
     *
     * @return the current position in the input.
     * @throws IOException
     */
    public long getPos() throws IOException {
        return 0;
    }

    /**
     * Close this {@link InputSplit} to future operations.
     *
     * @throws IOException
     */
    public void close() throws IOException {
    }

    /**
     * How much of the input has the {@link RecordReader} consumed i.e.
     * has been processed by?
     *
     * @return progress from <code>0.0</code> to <code>1.0</code>.
     * @throws IOException
     */
    public float getProgress() throws IOException {
        return 0.0f;
    }
}  