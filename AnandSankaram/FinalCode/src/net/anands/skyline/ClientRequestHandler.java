package net.anands.skyline;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Random;

/**
 * Created by kumar.
 */
public class ClientRequestHandler implements Runnable {
    private DatagramSocket socket;
    private DatagramPacket packet;

    public ClientRequestHandler(DatagramSocket socket, DatagramPacket packet) {
        this.socket = socket;
        this.packet = packet;
    }

    public void run() {
        try {
//            System.out.println("packet.getLength() is " + packet.getLength());
            byte[] data = new byte[packet.getLength()];
            System.arraycopy(packet.getData(), packet.getOffset(), data, 0, packet.getLength());
            String sentence = new String(data);
//            System.out.println("Sentence is " + sentence);
            handleString(sentence);
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    private void handleString(String sentence) {
        try {
            StringMap stringMap = StringMap.getInstance();
//            System.out.println("In client request Handler, sentence is " + sentence);
            String[] stringsSentByClient = sentence.split("\\r?\\n");
//            System.out.println("split length " + stringsSentByClient.length);
            InetAddress IPAddress = null;
            int port = 0;
            if(packet != null) {
                IPAddress = packet.getAddress();
                port = packet.getPort();
            }
            if (stringsSentByClient.length > 0) {
                String firstLine = stringsSentByClient[0];
                if (firstLine.equals("hello")) {
                    System.out.println("Brand new session");
//                First call, return session ID
                    Random random = new Random();
                    int randomInt = random.nextInt(10000);
                    Integer ran = new Integer(randomInt);
                    String sessionId = ran.toString();
                    System.out.println("Initializing Map for sessionId '" + sessionId+"'");
                    stringMap.initializeMap(sessionId);
                    byte[] sendData = sessionId.getBytes();
//                This is the session id.
                    System.out.println("**** Sending data to the client " + sessionId);
                    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, port);
                    socket.send(sendPacket);
                    return;
                } else if (firstLine.equals("done")) {
                    System.out.println("Done block");
                    String sessionId = stringsSentByClient[1];
                    int totalPacketCount = Integer.parseInt(stringsSentByClient[2]);
                    System.out.println("***** Calling doneWithSession for session " + sessionId + " and totalPacketCount " + totalPacketCount);
                    stringMap.doneWithSession(sessionId,totalPacketCount);
                    byte[] sendData = "done".getBytes();
                    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, port);
                    socket.send(sendPacket);
                } else {
                    String sessionId = firstLine;
                    System.out.println("sessionId is '" + sessionId+"'");
                    for (int i = 1; i < stringsSentByClient.length; i++) {
                        String string = stringsSentByClient[i];
//                        System.out.println("Adding string to session " + sessionId + ":" + string);
                        stringMap.addStringToSession(sessionId, string);
                    }
                    byte[] sendData = "existingSession".getBytes();
                    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, port);
                    socket.send(sendPacket);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
//            System.out.println(e.getMessage());
        }
    }

    public static void main(String[] args) {
        ClientRequestHandler handler = new ClientRequestHandler(null,null);
        handler.handleString("hello");
    }
}
