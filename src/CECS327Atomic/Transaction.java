package CECS327Atomic;

import java.math.BigInteger;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Transaction implements Serializable  {
  public enum Operation { WRITE, DELETE}

  BigInteger TransactionId;
  Integer guid;
  Operation op;
  byte vote;
  FileStream fileStream;   
  public Transaction(Operation op) throws NoSuchAlgorithmException
  {
        MessageDigest md = MessageDigest.getInstance("MD5");

  	//id = md5(date + ip+port);
  	this.op = op;
  }  

}
