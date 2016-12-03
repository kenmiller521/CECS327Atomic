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
    
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    public Transaction(Operation op, FileStream stream) throws NoSuchAlgorithmException, IOException
    {
        
        //id = md5(date + ip+port);
        this.op = op;
        fileStream = stream;
    }
    public void setTimestamp(Timestamp stamp)
    {
        timestamp = stamp;
    }
    public Timestamp getTimestamp()
    {
        return timestamp;
    }
    /*
    public int md5(int i)            
    {
        byte[] intByteArray = intToByteArray(i);
        byte[] theDigest = md.digest(intByteArray);
        int toReturn = byteArrayToInt(theDigest);
        return toReturn;
    }
    //Function to convert integer to byte array to be digested
    public byte[] intToByteArray(int number)
    {
        byte[] bytes = new byte[4];
        for (int i = 0; i < 4; i++) {
            bytes[i] = (byte)(number >>> (i * 8));
        }
        return bytes;
    }
    public int byteArrayToInt(byte[] byteArray)
    {
        ByteBuffer buffer = ByteBuffer.wrap(byteArray);
        int toReturn = buffer.getInt();
        return toReturn;
    }*/
}
