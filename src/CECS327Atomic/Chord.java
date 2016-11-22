package CECS327Atomic;

import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.*;
import java.net.*;
import java.util.*;
import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;
 
public class Chord extends java.rmi.server.UnicastRemoteObject implements ChordMessageInterface
{
    public static final int M = 2;
    
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

    List<ChordMessageInterface> nodeList = new ArrayList<ChordMessageInterface>();
    private Iterator iter;
    private ChordMessageInterface element;
    private Boolean currentlyVoting;
    private int yesVoteCounter,noVoteCounter;

    

    

    
  

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
            successor.election(i);            
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
        //send the canCommit request to participants
        while(iter.hasNext())
        {
            element = (ChordMessageInterface) iter.next();
            //send the request
            element.sendCanCommitToParticipant();
        }
        //stay until all votes are yes or if there is one no counter
        while(noVoteCounter == 0 && (yesVoteCounter != nodeList.size()))
        {
            
        }
        if(yesVoteCounter == nodeList.size())
        {
            System.out.println("All participants ready to commit.");
        }
        else if(noVoteCounter != 0)
        {
            System.out.println("Aborting. A participant is not ready to commit. Proceed with menu selection.");
        }
            
    }
    public void cancelCanCommitRequest() throws IOException, RemoteException 
    {
        currentlyVoting = false;
        System.out.println("COORDINATOR: Commit cancelled");
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
    public Boolean isVoting()
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
}
