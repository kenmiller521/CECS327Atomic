package CECS327Atomic;

import CECS327Atomic.ChordMessageInterface;
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
    ChordMessageInterface coordinator;
ChordMessageInterface successor;
    ChordMessageInterface predecessor;
    ChordMessageInterface[] finger;
    int nextFinger;
    int i;   		// GUID
    public String msgID;
    public enum_MSG msg;
    private int isCoordinator;
    
    HashMap<Integer,Timestamp> LastWriteTime = new HashMap<Integer,Timestamp>(); 
    HashMap<Integer,Timestamp> LastReadTime = new HashMap<Integer,Timestamp>();
    List<ChordMessageInterface> nodeList = new ArrayList<ChordMessageInterface>();
    private Iterator iter;
    private ChordMessageInterface element;
    private Boolean currentlyVoting;
    private int yesVoteCounter,noVoteCounter;
    private Boolean currentlyCommitting;

    /**
     *
     * @param trans
     * @return
     * @throws IOException
     * @throws RemoteException
     */
    @Override
    public boolean canCommit(Transaction trans) throws IOException, RemoteException {
        //if(trans.timestamp > LastWriteTime.)
        //{
        //   return true;
        //}
       //else
            return false;
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    void writeAtomic(String fileName) throws NoSuchAlgorithmException, UnsupportedEncodingException, RemoteException, IOException 
    {
        //open up the file
        FileStream stream = new FileStream(".\\"+  i +"\\repository\\"+fileName);
        //open the file, transmit
        
        Transaction trans = new Transaction(Transaction.Operation.WRITE);
        
        int guid1 = md5(fileName+1);
        int guid2 = md5(fileName+2);
        int guid3 = md5(fileName+3);
        int guid4 = md5(fileName);
        trans.guid = guid4;
        ChordMessageInterface p1 = locateSuccessor(guid1);
        boolean v1 = p1.canCommit(trans);
        ChordMessageInterface p2 = locateSuccessor(guid2);
        boolean v2 = p2.canCommit(trans);
        ChordMessageInterface p3 = locateSuccessor(guid3);
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
           // p4.doCommit(trans,guid4);
            
        }
        else
        {
            p1.doAbort(trans);
            p2.doAbort(trans);
            p3.doAbort(trans);
            //p4.doAbort(trans);
        }
        //use guid4 to update the database
        
        
        //Find the peers which should be located, look for participants
        //can commit to all of them
        //do commit/do abort depending on votes
       // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    void readAtomic(String fileName)
    {
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
         //String fileName = "./"+i+"/repository/" + guid;
	 FileOutputStream output = new FileOutputStream(fileName);
	 while (stream.available() > 0)
	    output.write(stream.read());
       } catch (IOException e)
       {
	  System.out.println(e);
       } 
    }
    
    
    public InputStream get(int guid) throws RemoteException {
	 String fileName = ".\\"+i+"\\repository\\" + guid;
         //String fileName = "./"+i+"/repository/" + guid;
	 FileStream file= null;
	 try{
	   file = new FileStream(fileName);
	 } catch (Exception e) {
	    e.printStackTrace();
	}
        return file;        
    }
    
    public void delete(int guid) throws RemoteException {
	  String fileName = ".\\"+i+"\\repository\\" + guid;
          //String fileName = "./"+i+"/repository/" + guid;

          File file = new File(fileName);
	  file.delete();
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
          if(coordinator != null)
          {
              System.out.println("coordinator "+ coordinator.getId());              
          }          
          if(isCoordinator==1)
          {              
              System.out.println("participants ");
              iter = nodeList.iterator();
              while(iter.hasNext())
              {
                  element = (ChordMessageInterface) iter.next();
                  System.out.println("\t"+element.getId());
              }
          }
       }
        catch(RemoteException e){
	       System.out.println("Cannot retrive id");
        } 
    }
    /*
    //function that handles the election of the processes
    public void election(int port) throws IOException {
        if(port == i)
        {
            System.out.println("YOU ARE THE COORDINATOR");
            isCoordinator = 1;
            System.out.println("COORDINATOR PORT: " + this.i);
            successor.setCoordinator(this);
            
        }
        else if(port > i)
        {
            //If the passed port is bigger than this port, then send the passed port to the successor
            System.out.println("PASSING PREDECESSOR PORT TO SUCCESSOR");
            successor.election(port);
            isCoordinator = 0;
        }
        else
        {
            System.out.println("PASSING YOUR PORT TO SUCCESSOR");
            //if the port is smaller than this port, then send this port to the successor
            successor.election(i);
            isCoordinator = 0;
        }
    }
*/
    @Override
    public void answer(int port) throws IOException, RemoteException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        System.out.println("YOU ARE THE COORDINATOR TO USER " + port);
    }
      @Override
    public void receiveMessage(ChordMessageInterface j, enum_MSG msg) throws IOException, RemoteException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void sendMessage(int port,ChordMessageInterface j, enum_MSG msg) throws IOException, RemoteException {
        if(msg.toString().equals("ELECT"))
        {
            //successor.election(i);            
        } 
        else if(msg.toString().equals("CANCOMMIT"))
        {
            canCommit();
        }
    } 
    public int isCoordinator()
    {
        return isCoordinator;
    }
    public void setCoordinator(ChordMessageInterface j) throws IOException
    {
        if(coordinator == null)
        {
            coordinator = j;
            successor.setCoordinator(coordinator);
            coordinator.addChordObjectToCoordinatorList(this);
            
        }        
    }
    @Override
    public void addChordObjectToCoordinatorList(ChordMessageInterface j) throws IOException, RemoteException 
    {
        if(j.getId() != coordinator.getId())
            nodeList.add(j);
    }
    
    public void canCommit() throws IOException, RemoteException 
    {
        //reset counters
        noVoteCounter = 0;
        yesVoteCounter = 0;
        iter = nodeList.iterator();
        Timer tt = new Timer();
        //send the canCommit request to participants
        while(iter.hasNext())
        {
            element = (ChordMessageInterface) iter.next();
            //send the request
            element.sendCanCommitToParticipant();
        }
        //stay until all votes are yes or if there is one no counter
        tt.schedule(new TimerTask() 
        {
            @Override
            public void run() 
            {               
                if(noVoteCounter == 0 && (yesVoteCounter != nodeList.size()))
                { 
                    System.out.println("Waiting for participants to respond.");
                }
                else if(yesVoteCounter == nodeList.size())
                {
                    System.out.println("All participants ready to commit. Proceed with menu selection.");  
                    System.out.println("Usage: \n\tjoin <port>\n\twrite <file> (the file must be an integer stored in the working directory, i.e, ./port/file");
                    System.out.println("\tread <file>\n\tdelete <file>\n\tprint\n\telection");
                    if(isCoordinator()==1)
                    {
                        System.out.println("COORDINATOR USAGE: \n\tcanCommit");
                    }
                    tt.cancel();
                }
                else if(noVoteCounter != 0)
                {
                    System.out.println("Aborting. A participant is not ready to commit. Proceed with menu selection.");
                    System.out.println("Usage: \n\tjoin <port>\n\twrite <file> (the file must be an integer stored in the working directory, i.e, ./port/file");
                    System.out.println("\tread <file>\n\tdelete <file>\n\tprint\n\telection");   
                    if(isCoordinator()==1)
                    {
                        System.out.println("COORDINATOR USAGE: \n\tcanCommit");
                    }
                    tt.cancel();
                }
            }
        }, 0, 1000);           
    }
    public void cancelCanCommitRequest() throws IOException, RemoteException 
    {
        currentlyVoting = false;
        System.out.println("COORDINATOR: Commit cancelled. Proceed with menu selection.");
        System.out.println("Usage: \n\tjoin <port>\n\twrite <file> (the file must be an integer stored in the working directory, i.e, ./port/file");
        System.out.println("\tread <file>\n\tdelete <file>\n\tprint\n\telection");  
    }
    public void sendCanCommitToParticipant() throws IOException, RemoteException
    {
        currentlyVoting = true;
        System.out.println("COORDINATOR: Can you commit(Y/N)?"); 
        Timer t = new Timer();
        t.schedule(new TimerTask() 
        {
            int i = 10;
            @Override
            public void run() 
            {                
                System.out.println("Timeout in " + i + "."); 
                i--;
                if(i==0)
                {
                    try 
                    {
                        //send timeout to coordinator to resend cancommit request
                        coordinator.canCommitTimeout();
                    }
                    catch (IOException ex) 
                    {
                        Logger.getLogger(Chord.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    t.cancel();
                }
                else if(currentlyVoting == false)
                {
                    t.cancel();
                }
            }
        }, 0, 1000);     
    }
    @Override
    public void canCommitTimeout() throws IOException, RemoteException 
    {
        System.out.println("Timeout - Resending request.");
        canCommit();
    }
    public void sendCommitVoteToCoordinator(int vote,ChordMessageInterface j) throws IOException, RemoteException 
    {
        if(vote == 1)
        {
            System.out.println("YES VOTE FROM " + j.getId());
            yesVoteCounter++;
        }
        else
        {
            noVoteCounter++;
            System.out.println("NO VOTE FROM " + j.getId());
            //cancel commit
            iter = nodeList.iterator();
            //send the canCommit request to participants
            while(iter.hasNext())
            {
                element = (ChordMessageInterface) iter.next();
                //send the request
                element.cancelCanCommitRequest();
            }
        }            
    }
    public boolean isVoting()
    {
        return currentlyVoting;
    }
    public void sendVote(String text) throws IOException
    {
        if(text.equals("Y"))
        {
            //save work to storage
            coordinator.sendCommitVoteToCoordinator(1,this);
        }
        else if(text.equals("N"))
        {
            coordinator.sendCommitVoteToCoordinator(0,this);
        }
        currentlyVoting = false;
    
    }
    
    public boolean isCommitting()
    {
        return currentlyCommitting;
    }
    
    public void doCommit(Transaction trans, int guid,FileStream stream) throws IOException, RemoteException
    {
        currentlyCommitting = true;
        LastWriteTime.put(trans.guid, timestamp);
        put(guid,stream);
        //participant.put();
        // continue with transaction
    }
    
    public void doAbort(Transaction trans) throws IOException, RemoteException
    {
        currentlyCommitting = false;
        System.out.println("COORDINATOR: Transaction Aborted");
    }
    
    public void haveCommitted() throws IOException, RemoteException
    {
        // TODO Call from paricipant to the the coord to confirm that is has commited the transaction
    }
    
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
