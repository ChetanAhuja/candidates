package net.anands.skyline;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by kumar.
 */
public class StringMap {

    private static StringMap _self;
    private HashMap<String, HashMap<String,ArrayList>> stringMap;
    private static String directoryToWrite = "/tmp/";
    private static final String STRINGS = "strings";
    private static final String TOTAL_PACKET = "totalPackets";

    public static synchronized StringMap getInstance() throws Exception{
        if(_self == null){
            _self = new StringMap();
        }
        return _self;
    }


    private StringMap(){
        stringMap = new HashMap<String, HashMap<String,ArrayList>>(100);
    }

    public synchronized void initializeMap(String s) throws InvalidSessionException{
        String sessionId=s.trim();
//        System.out.println("Initializing map for sessionId " + sessionId);
        HashMap sessionMap = stringMap.get(sessionId);
        ArrayList existingStrings = null;
        if(sessionMap != null){
            existingStrings = (ArrayList)sessionMap.get(STRINGS);
            if(existingStrings != null) {
                throw new InvalidSessionException("SessionID already exists in the map");
            }
        }
        existingStrings = new ArrayList(100);
        sessionMap = new HashMap();
        sessionMap.put(STRINGS,existingStrings);
        stringMap.put(sessionId,sessionMap);
    }

    public synchronized void addStringToSession(String s, String string) throws Exception{
        String sessionId = s.trim();
        HashMap sessionMap = stringMap.get(sessionId);
        if(sessionMap == null) {
            throw new InvalidSessionException("No sessionMap found for session " + sessionId);
        }
        ArrayList existingStrings = (ArrayList)sessionMap.get(STRINGS);
        if(existingStrings == null) {
            throw new InvalidSessionException("No ArrayList found for session " + sessionId);
        }

//        Insert it at the right spot now.
        //            For each string, find out where to insert based on it's length.
//            System.out.println("currentString is " + currentString);
        Iterator iterator = existingStrings.iterator();
        int position = 0;
        while(iterator.hasNext()){
            String object = (String) iterator.next();
            if(object.length() < string.length()){
                position++;
            } else {
                break;
            }
        }
        existingStrings.add(position,string);
        System.out.println("ExistingStrings length is " + existingStrings.size());
        sessionMap.put(STRINGS,existingStrings);
        stringMap.put(sessionId,sessionMap);
//        Additionally check if done packet was received already
        Integer packetCount = (Integer)sessionMap.get(TOTAL_PACKET);
        if(packetCount != null){
            System.out.println("packetCount is " + packetCount);
        } else {
            System.out.println("packetCount is null");
        }
        if(packetCount != null && existingStrings.size() == packetCount.intValue()){
            System.out.println("Looks like done packet was received already. ");
            handleNinetyPercent(sessionId,existingStrings);
        }
    }

    public synchronized void doneWithSession(String s, int totalPacketCount) throws Exception{
        String sessionId = s.trim();
        System.out.println("***** doneWithSession called for " + sessionId);
        HashMap sessionMap = stringMap.get(sessionId);
        if(sessionMap == null){
            throw new InvalidSessionException("No SessionMap found for session " + sessionId);
        }
        ArrayList existingStrings = (ArrayList)sessionMap.get(STRINGS);
        if(existingStrings == null) {
            throw new InvalidSessionException("No ArrayList found for session " + sessionId);
        }
        sessionMap.put(TOTAL_PACKET, new Integer(totalPacketCount));
        stringMap.put(sessionId,sessionMap);
        int listLength = existingStrings.size();
        System.out.println("listLength is " + listLength + " and totalPacketCount is " + totalPacketCount);
        if(listLength < totalPacketCount) {
            System.out.println("Not all packets have been received.  Should wait.");
            return;
        }
        handleNinetyPercent(sessionId, existingStrings);
    }

    private void handleNinetyPercent(String sessionId, ArrayList existingStrings) throws FileNotFoundException, UnsupportedEncodingException {
        int listLength = existingStrings.size();
        System.out.println("List length is " + listLength);
//        find out the first 90% of the file
        double tenPercent = 0.1 * listLength;
        int ten = (int) Math.round(tenPercent);

        int topNinety = listLength - ten;
        System.out.println("Top ninety is " + topNinety);

        String fileName = directoryToWrite + sessionId + ".txt";

        PrintWriter writer = new PrintWriter(fileName, "UTF-8");
        int counter = 0;

        Iterator it = existingStrings.iterator();

        while(it.hasNext()){
            String line = (String) it.next();
            writer.println(line);
            counter++;
            if(counter >= topNinety) {
                break;
            }
        }
        writer.close();
    }

}
