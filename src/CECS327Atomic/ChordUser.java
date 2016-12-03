package CECS327Atomic;

import java.rmi.*;
import java.net.*;
import java.util.*;
import java.io.*;import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Level;
import java.util.logging.Logger;
;

 
public class ChordUser
{
    
     int port;     
     private Boolean inputCorrect;
     public ChordUser(int p) {
        port = p;
	Timer timer1 = new Timer();
	timer1.scheduleAtFixedRate(new TimerTask() {
	    @Override
	    public void run() {
	      try {
	      
		Chord    chord = new Chord(port);
		
        
		Scanner scan= new Scanner(System.in);
		
		while (true)
		{
                  System.out.println("Usage: \n\tjoin <port>\n\twrite <file> (the file must be an integer stored in the working directory, i.e, ./port/file");
                  System.out.println("\tread <file>\n\tdelete <file>");
                  //if(chord.isCoordinator()==1)
                  //{
                  //    System.out.println("COORDINATOR USAGE: \n\tcanCommit");
                 // }
                  String delims = "[ ]+";
                  String command = "";
		  String text= scan.nextLine();
                  //If the coordinator sent a request to vote for commiting
                  /*
                  if(chord.isVoting())
                  {
                      inputCorrect = false;
                      while(!inputCorrect)
                      {
                          if(text.toUpperCase().equals("Y") || text.toUpperCase().equals("N"))
                          {
                              inputCorrect = true;
                              chord.sendVote(text.toUpperCase());
                          }
                          else
                          {
                              System.out.println("Enter 'Y' or 'N'");
                              text = scan.nextLine();
                          }
                      
                      }
                  }
                  //else continue with normal operations
                  else
                  {*/
                    String[] tokens = text.split(delims);
                      if (tokens[0].equals("join") && tokens.length == 2) {
                          try {
                            chord.joinRing("localhost", Integer.parseInt(tokens[1]));
                          } catch (IOException e) {
                                System.out.println("Error joining the ring!");
                          }
                      }
                      if (tokens[0].equals("print")) {
                          chord.Print();
                      }
                      if  (tokens[0].equals("write") && tokens.length == 2) {

                          try {	
                                  //int guid = Integer.parseInt(tokens[1]);
                                  //int guid = md5(tokens[1]);
                                  // If you are using windows you have to use
                                  //String path = ".\\"+  port +"\\repository\\"+tokens[1]; // path to file. user has to write '.txt' or the extension of the file
                                  //String path = ".\\"+  port +"\\"+Integer.parseInt(tokens[1])+".txt"; // path to file
                                  //String path = ".\\"+  port +"\\"+Integer.parseInt(tokens[1]); // path to file
                                  //String path = "./"+  port +"/"+guid; // path to file
                                  //FileStream file = new FileStream(path);
                                  //ChordMessageInterface peer = chord.locateSuccessor(guid);
                                  //peer.put(guid, file); // put file into ring
                                  chord.writeAtomic(tokens[1]);
                                  
                                  //This will be the coordinator
                                  
                                  
                                  
                          } catch (Exception e) {
                                  e.printStackTrace();
                          } 
                      }
                      if  (tokens[0].equals("read") && tokens.length == 2) {
                          try {	
                            //int guid = Integer.parseInt(tokens[1]);
                            int guid = md5(tokens[1]);
                            // If you are using windows you have to use
                           //String path = ".\\"+  port +"\\repository\\"+Integer.parseInt(tokens[1])+".txt"; // path to file
                           String path = ".\\"+  port +"\\repository\\"+tokens[1];
                                  //String path = ".\\"+  port +"\\"+Integer.parseInt(tokens[1])+".txt"; // path to file
                           // String path = "./"+  port +"/"+guid; // path to file
                            FileStream file = new FileStream(path);
                            ChordMessageInterface peer = chord.locateSuccessor(guid);
                            peer.put(guid, file); // put file into ring
                          } catch (Exception e) {
                                  e.printStackTrace();
                          } 
                      }
                      if  (tokens[0].equals("delete") && tokens.length == 2) {
                          try {
                              //everytime we use the cloud use the interger, client use the string
                            int guid = md5(tokens[1]);
                            chord.delete(guid);
                          } catch (Exception e) {
                                e.printStackTrace();
                          }
                      }
                      /*
                      if(tokens[0].toUpperCase().equals("ELECTION"))
                      {
                          chord.sendMessage(port,chord, Chord.enum_MSG.ELECT);
                      }
                      */
                      /*
                      if(chord.isCoordinator() == 1)
                      {
                        if(tokens[0].toUpperCase().equals("CANCOMMIT"))
                        {
                            System.out.println("SENDING CANCOMMIT MESSAGE");
                            chord.sendMessage(port, chord, Chord.enum_MSG.CANCOMMIT);
                        }
                      }*/
                    }                
		}
		catch(RemoteException e)
		{
                    e.printStackTrace();
		}		
	      }
	   }, 1000, 1000);
    }
    
    static public void main(String args[])
    {
	if (args.length < 1 ) {  
	  throw new IllegalArgumentException("Parameter: <port>");
      }
        try{
	  ChordUser chordUser=new ChordUser( Integer.parseInt(args[0]));
	}
	catch (Exception e) {
           e.printStackTrace();
           System.exit(1);
	}
     } 
    public int md5(String str) throws NoSuchAlgorithmException, UnsupportedEncodingException
    {
        MessageDigest md = MessageDigest.getInstance("MD5");
        //byte[] bytesOfMessage = str.getBytes("UTF-8");
        //byte[] thedigest = md.digest(bytesOfMessage);
        md.update(str.getBytes(),0,str.length());
        BigInteger bigInt = new BigInteger(1, md.digest());
        //use prime number to modulo
        int intToReturn = bigInt.intValue()%15000;
        return intToReturn;
        
        //first need to do md5.update
        
    }
}








