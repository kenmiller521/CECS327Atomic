//Ken Miller, 013068183
//Michael Zatlin, 011600158
//Bryan Di Nardo, 011795743
//CECS327 Atomic Commit
package CECS327Atomic;

import java.math.BigInteger;
import java.io.*;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;

public class Transaction implements Serializable  {
    public enum Operation { WRITE, DELETE}
    BigInteger TransactionId;
    Integer guid;
    Operation op;
    byte vote;
    FileStream fileStream;   
    
    Timestamp timestamp;
    public Transaction(Operation op, FileStream stream) throws NoSuchAlgorithmException, IOException
    {
        
        //id = md5(date + ip+port);
        this.op = op;
        fileStream = stream;
        timestamp = new Timestamp(System.currentTimeMillis());
    }
    public void setTimestamp(Timestamp stamp)
    {
        timestamp = stamp;
    }
    public Timestamp getTimestamp()
    {
        return timestamp;
    }   
}
