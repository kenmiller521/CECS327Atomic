//Ken Miller, 013068183
//Michael Zatlin, 011600158
//Bryan Di Nardo, 011795743
//CECS327 Atomic Commit
package CECS327Atomic;

import CECS327Atomic.ChordMessageInterface;
import static CECS327Atomic.Transaction.Operation.*;
import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.*;
import java.net.*;
import java.util.*;
import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Timestamp;
public class Chord extends java.rmi.server.UnicastRemoteObject implements ChordMessageInterface
{
    public static final int M = 2;
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    Registry registry;    // rmi registry for lookup the remote objects.
    public ChordMessageInterface coordinator;
    ChordMessageInterface successor;
    ChordMessageInterface predecessor;
    ChordMessageInterface[] finger;
    int nextFinger;
    int i;   		// GUID
    public String msgID;
    public enum_MSG msg;
    //private int isCoordinator;
    
    HashMap<Integer,Timestamp> LastWriteTime = new HashMap<Integer,Timestamp>(); 
    HashMap<Integer,Timestamp> LastReadTime = new HashMap<Integer,Timestamp>();
    List<ChordMessageInterface> nodeList = new ArrayList<ChordMessageInterface>();
    private Boolean currentlyVoting;
    private Boolean currentlyCommitting;
    /**
     *
     * @param trans
     * @return
     * @throws IOException
     * @throws RemoteException
     */
    @Override
    public boolean canCommit(Transaction trans) throws IOException, RemoteException 
    {
        //save the transaction in temp storage
            File file = new File(".\\"+  i +"\\temp\\"+trans.guid);
            if(file.createNewFile())
                System.out.println("Temp file made");
            
            System.out.println(trans.timestamp + "/" + LastWriteTime.get(trans.guid));
            if(LastReadTime.get(trans.guid)== null || LastWriteTime.get(trans.guid)== null)
            {
                System.out.println("TIMESTAMP IS NULL - NO ENTRIES IN HASHMAP THUS FIRST TIME READ/WRITE");
                return true;
            }
            
            //check local last written time
            else if(trans.timestamp.compareTo(LastWriteTime.get(trans.guid))>0)
            {
                System.out.println("Yes vote");
                return true;
            }
            else
            {
                System.out.println("No vote");
                return false;
            }
            
            /*
            else if(trans.timestamp.compareTo(LastReadTime.get(trans.guid))>0 && trans.timestamp.compareTo(LastWriteTime.get(trans.guid))>0)
            {
                return true;
            }
            else
                return false;*/
    }

    void writeAtomic(String fileName) throws NoSuchAlgorithmException, UnsupportedEncodingException, RemoteException, IOException 
    {
        FileStream stream = new FileStream(".\\"+  i +"\\"+fileName);        
        Transaction trans = new Transaction(Transaction.Operation.WRITE,stream);
        
        int guid1 = md5(fileName+1)%11;
        int guid2 = md5(fileName+2)%11;
        int guid3 = md5(fileName+3)%11;
        int guid4 = md5(fileName)%11;
        
        trans.guid = guid4;
        trans.timestamp = LastReadTime.get(guid4);
        ChordMessageInterface p1 = locateSuccessor(guid1);
        p1.setCoordinator(this);
        boolean v1 = p1.canCommit(trans);
        ChordMessageInterface p2 = locateSuccessor(guid2);
        p2.setCoordinator(this);
        boolean v2 = p2.canCommit(trans);
        ChordMessageInterface p3 = locateSuccessor(guid3);
        p3.setCoordinator(this);
        boolean v3 = p3.canCommit(trans);
        //ChordMessageInterface p4 = locateSuccessor(guid4);
        //boolean v4 = p4.canCommit(trans);
        if(v1&&v2&&v3)
        {
            p1.doCommit(trans,guid1,stream);
            stream.reset();
            p2.doCommit(trans,guid2,stream);
            stream.reset();
            p3.doCommit(trans,guid3,stream);
            stream.reset();            
        }
        else
        {
            p1.doAbort(trans);
            p2.doAbort(trans);
            p3.doAbort(trans);
        }
    }
    void readAtomic(String fileName) throws IOException, NoSuchAlgorithmException
    {
        FileStream stream = new FileStream(".\\"+  i +"\\repository\\"+md5(fileName)%11);
        //FileInputStream inputStream = new FileInputStream(".\\"+  i +"\\repository\\"+fileName);
        Transaction trans = new Transaction(Transaction.Operation.WRITE,stream);
        //get the current time and stamp it to the transaction
        trans.timestamp = new Timestamp(System.currentTimeMillis());
        trans.guid = md5(fileName)%11;
        System.out.println("Reading with timestamp " + trans.timestamp);
        LastReadTime.put(trans.guid, trans.getTimestamp());
        InputStream inputStream = get(trans.guid);
        //inputStream = (FileInputStream) get(md5(fileName)%11);
        try
        {
            if(inputStream.available()>0)
            {
                byte[] buffer = new byte[inputStream.available()];
                inputStream.read(buffer);
                File targetFile = new File(".\\"+  i +"\\"+fileName);
                OutputStream outStream = new FileOutputStream(targetFile);
                outStream.write(buffer);
                //System.out.println("READ:" + j);
            }
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
    }

    

    

    
  

    public enum enum_MSG 
    {
        //Start with join, accept, 
        ELECT, //0
        CANCOMMIT, //1
        COORDINATOR//2
    };
    
    public ChordMessageInterface rmiChord(String ip, int port)
    {	
        ChordMessageInterface chord = null;
        try{
            Registry registry = LocateRegistry.getRegistry(ip, port);
            chord = (ChordMessageInterface)(registry.lookup("Chord"));
            return chord;
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch(NotBoundException e){
            e.printStackTrace();
        }
        return null;
    }
    
    public Boolean isKeyInSemiCloseInterval(int key, int key1, int key2)
    {
       if (key1 < key2)
	return (key > key1 && key <= key2);
      else
	return (key > key1 || key <= key2);
    }

    public Boolean isKeyInOpenInterval(int key, int key1, int key2)
    {
      if (key1 < key2)
	return (key > key1 && key < key2);
      else
	return (key > key1 || key < key2);
    }
    
    
    public void put(int guid, InputStream stream) throws RemoteException {
	 //TODO Store the file at ./port/repository/guid
      try {
	 String fileName = ".\\"+i+"\\repository\\" + guid;
	 FileOutputStream output = new FileOutputStream(fileName);
	 while (stream.available() > 0)
	    output.write(stream.read());
       } catch (IOException e)
       {
	  System.out.println(e);
       } 
    }
    
    
    @Override
    public InputStream get(int guid) throws RemoteException {
	 String fileName = ".\\"+i+"\\repository\\" + guid;
	 FileStream file= null;
	 try{
	   file = new FileStream(fileName);
	 } catch (Exception e) {
	    e.printStackTrace();
	}
        return file;        
    }
    
    @Override
    public void delete(int guid) throws RemoteException {
	  String fileName = ".\\"+i+"\\repository\\" + guid;
          System.out.println("Attemping to delete file with GUID " + guid + " in " + ".\\"+i+"\\repository\\");
          File file = new File(fileName);
	  if(file.delete())              
            System.out.println("Delete successful");
          else
            System.out.println("Delete unsuccessful. Try again later.");
    }
    
    public int getId() throws RemoteException {
        return i;
    }
    public boolean isAlive() throws RemoteException {
	    return true;
    }
    
    public ChordMessageInterface getPredecessor() throws RemoteException {
	    return predecessor;
    }
    
    public ChordMessageInterface locateSuccessor(int key) throws RemoteException {
	    if (key == i)
            throw new IllegalArgumentException("Key must be distinct that  " + i);
	    if (successor.getId() != i)
	    {
	      if (isKeyInSemiCloseInterval(key, i, successor.getId()))
	        return successor;
	      ChordMessageInterface j = closestPrecedingNode(key);
	      
          if (j == null)
	        return null;
	      return j.locateSuccessor(key);
        }
        return successor;
    }
    
    public ChordMessageInterface closestPrecedingNode(int key) throws RemoteException {
	int count = M-1;	
	if (key == i)  throw new IllegalArgumentException("Key must be distinct that  " + i);	
	for (count = M-1; count >= 0; count--) {
	  if (finger[count] != null && isKeyInOpenInterval(finger[count].getId(), i, key)) 
	      return finger[count];
        }
        return successor;

    }
    
    public void joinRing(String ip, int port)  throws RemoteException {
        try{
            System.out.println("Get Registry to joining ring");
            Registry registry = LocateRegistry.getRegistry(ip, port);
            ChordMessageInterface chord = (ChordMessageInterface)(registry.lookup("Chord"));
            predecessor = null;
            successor = chord.locateSuccessor(this.getId());
	    System.out.println("Joining ring");
	 }
        catch(RemoteException | NotBoundException e){
            successor = this;
        }   
    }
    
    public void findingNextSuccessor()
    {
	int i;
	successor = this;
	for (i = 0;  i< M; i++)
	{
	  try 
	  {
	      if (finger[i].isAlive())
	      {
		  successor = finger[i];
	      }	    
	  }
	  catch(RemoteException | NullPointerException e)
	  {	    
	      finger[i] = null;
	  }
	}
    }
    
    public void stabilize() {
      boolean error = false;
      try {
	  if (successor != null)
	  {
	    ChordMessageInterface x = successor.getPredecessor();	
	   
	    if (x != null && x.getId() != this.getId() && isKeyInOpenInterval(x.getId(), this.getId(), successor.getId())) 
	    {  
	      successor = x;
	    }
	    if (successor.getId() != getId())
	    {
	      successor.notify(this);
	    }
	  }
      } catch(RemoteException | NullPointerException e1) 
      {
 	  error = true;
      }
      if (error)
 	findingNextSuccessor();
    }
    
    public void notify(ChordMessageInterface j) throws RemoteException {
         if (predecessor == null || (predecessor != null && isKeyInOpenInterval(j.getId(), predecessor.getId(), i)))
	 // TODO 
	 //transfer keys in the range [j,i) to j;
	  predecessor = j;
    }
    
    public void fixFingers() {
    
        int id= i;
        try {
	    int nextId;
	    if (nextFinger == 0) // && successor != null)
	      nextId = (this.getId() + (1 << nextFinger));	
	    else 
	      nextId = finger[nextFinger -1].getId();
	    finger[nextFinger] = locateSuccessor(nextId);
	    
	    if (finger[nextFinger].getId() == i)
   	      finger[nextFinger] = null;
	    else
	      nextFinger = (nextFinger + 1) % M;
       } catch(RemoteException | NullPointerException e){
	  finger[nextFinger] = null;
 	  e.printStackTrace();
      }
    }
    
    public void checkPredecessor() { 	
      try {
	if (predecessor != null && !predecessor.isAlive())
	      predecessor = null;
      } 
      catch(RemoteException e) 
      {
	  predecessor = null;
//           e.printStackTrace();
      }
    }
       
    public Chord(int port) throws RemoteException {
        int j;
        currentlyVoting = false;
	finger = new ChordMessageInterface[M];
        for (j=0;j<M; j++){
	    finger[j] = null;
	}
        i = port;
	
        predecessor = null;
	successor = this;
	Timer timer = new Timer();
	timer.scheduleAtFixedRate(new TimerTask() {
	    @Override
	    public void run() {
	      stabilize();
	      fixFingers();
	      checkPredecessor();
	    }
	}, 500, 500);
	try{
	// create the registry and bind the name and object.
	    System.out.println("Starting RMI at port="+port);
	    registry = LocateRegistry.createRegistry( port );
	  registry.rebind("Chord", this);
	}
	catch(RemoteException e){
	       throw e;
        } 
    }
    
    void Print()
    {   
	int i;
	try {
	  if (successor != null)
	      System.out.println("successor "+ successor.getId());
	  if (predecessor != null)
	      System.out.println("predecessor "+ predecessor.getId());
	  for (i=0; i<M; i++)
	  {
	    try {
	  	if (finger != null)
		  System.out.println("Finger "+ i + " " + finger[i].getId());
	    } catch(NullPointerException e)
	    {
	      finger[i] = null;
	   }
	  }
       }
        catch(RemoteException e){
	       System.out.println("Cannot retrive id");
        } 
    }
    public void setCoordinator(ChordMessageInterface j) throws IOException
    {
        if(coordinator == null)
        {
            coordinator = j;
            System.out.println("Coordinator id: " +j.getId());
        }        
    }
    public boolean isVoting()
    {
        return currentlyVoting;
    }
    
    public boolean isCommitting()
    {
        return currentlyCommitting;
    }
    
    @Override
    public void doCommit(Transaction trans, int guid,FileStream stream) throws IOException, RemoteException
    {
        if (trans.op == WRITE)
        {
            trans.timestamp = new Timestamp(System.currentTimeMillis());
            System.out.println("Doing commit with timestamp " + trans.timestamp);
            currentlyCommitting = true;
            put(guid, trans.fileStream);
            LastWriteTime.put(trans.guid, trans.timestamp);
            //LastReadTime.put(trans.guid, trans.timestamp);
            String fileName = ".\\"+i+"\\repository\\" + trans.guid;
            
            try
            {
                FileOutputStream fileOutput = new FileOutputStream(fileName);
                fileOutput.write(trans.fileStream.read());
            }catch(IOException e)
            {
                e.printStackTrace();
            }
        }
        coordinator.haveCommitted(trans,this);       
    }
    
    @Override
    public void doAbort(Transaction trans) throws IOException, RemoteException
    {
        currentlyCommitting = false;
        System.out.println("COORDINATOR: Transaction Aborted");
    }
    
    public void haveCommitted(Transaction trans, ChordMessageInterface j) throws IOException, RemoteException
    {
        System.out.println(j.getId() + " has commited. Timestamp: " + trans.timestamp);
        // TODO Call from paricipant to the the coord to confirm that is has commited the transaction
    }
    
    @Override
    public boolean getDecision() throws IOException, RemoteException
    {
        // TODO Yes/No: Call from participant to coord to ask for the decision on a transaction when it has voted
        // yes but has still had no reply after some delay. Used to recover from server crash or delayed messages
        return false;
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
