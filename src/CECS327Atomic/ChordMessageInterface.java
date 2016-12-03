//Ken Miller, 013068183
//Michael Zatlin, 011600158
//Bryan Di Nardo, 011795743
//CECS327 Atomic Commit
package CECS327Atomic;

import CECS327Atomic.Chord.enum_MSG;
import java.rmi.*;
import java.io.*;

public interface ChordMessageInterface extends Remote
{
    public ChordMessageInterface getPredecessor()  throws RemoteException;
    ChordMessageInterface locateSuccessor(int key) throws RemoteException;
    ChordMessageInterface closestPrecedingNode(int key) throws RemoteException;
    public void joinRing(String Ip, int port)  throws RemoteException;
    public void notify(ChordMessageInterface j) throws RemoteException;
    public boolean isAlive() throws RemoteException;
    public int getId() throws RemoteException;
    
    
    public void put(int guid, InputStream file) throws IOException, RemoteException;
    public InputStream get(int id) throws IOException, RemoteException;
    public void delete(int id) throws IOException, RemoteException;
    
    
    //need function to send haveCommited to the actual coordinator
    public void setCoordinator(ChordMessageInterface j) throws IOException, RemoteException;
    
    public boolean canCommit(Transaction trans) throws IOException, RemoteException;
    //Yes/No: Call from coord to participant to ask whether it can commit a transaction. Participant replies with its vote
    
    public void doCommit(Transaction trans,int guid,FileStream stream) throws IOException, RemoteException;
    // Call from coord to participant to tell participant to commit its part of a transaction
    
    public void doAbort(Transaction trans) throws IOException, RemoteException;
    // Call from the coord to participant to tell participant to abort its part of a transaction
    
    public void haveCommitted(Transaction trans, ChordMessageInterface j) throws IOException, RemoteException;
    // Call from participant to the the coord to confirm that is has commited the transaction
    
    public boolean getDecision() throws IOException, RemoteException;
    // Yes/No: Call from participant to coord to ask for the decision on a transaction when it has voted
    // yes but has still had no reply after some delay. Used to recover from server crash or delayed messages

}
