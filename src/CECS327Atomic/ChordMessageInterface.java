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
    
    
    
    public void election(int port) throws IOException, RemoteException;
    public void answer(int port) throws IOException, RemoteException;
    public void receiveMessage(ChordMessageInterface j, enum_MSG msg) throws IOException, RemoteException;
    public void sendMessage(int port,ChordMessageInterface j, enum_MSG msg)throws IOException, RemoteException;
    public void setCoordinator(ChordMessageInterface j) throws IOException, RemoteException;
    public void canCommit() throws IOException, RemoteException;
    public void cancelCanCommitRequest() throws IOException, RemoteException;
    public void sendCanCommitToParticipant() throws IOException, RemoteException;
    public void canCommitTimeout() throws IOException, RemoteException;
    public void sendCommitVoteToCoordinator(int vote,ChordMessageInterface j) throws IOException, RemoteException;
    public void addChordObjectToCoordinatorList(ChordMessageInterface j) throws IOException, RemoteException;

}
