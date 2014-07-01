package net.anands.skyline;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Created by kumar.
 */
public class UDPFileClient {

    private String filePath;
    private String hostname;
    private int port;
    private DatagramSocket datagramSocket;

    public UDPFileClient(String filename, String hostname, int port) throws Exception{
        this.filePath = filename;
        this.hostname = hostname;
        this.port = port;
    }

    public void sendFile() throws Exception{
//        Read the file to a byte[] first.
//        The requirement says read the file line by line.
//        First pass, assume that the file is small and can be read in memory.
//        Once the file is sent to the server and the server is done with it's thing,
//        change the protocol to read it line by line and come up with the server protocol
//        TODO don't read the entire file in one go, read it line by line and build the buffer.

        DatagramSocket socket = null;
        try {
            socket = new DatagramSocket();
            InetAddress serverAddress = InetAddress.getByName(hostname);

//            First send a "hello" packet and get a unique session id back.
            byte[] helloBytes = "hello".getBytes();
            DatagramPacket firstPacket = new DatagramPacket(helloBytes,helloBytes.length,serverAddress,port);
            firstPacket.setLength(firstPacket.getLength());
            socket.send(firstPacket);
            System.out.println("Sent first request");

            byte[] sessionIdBuffer = new byte[1024];
            DatagramPacket sessionIdPacket = new DatagramPacket(sessionIdBuffer,sessionIdBuffer.length);
            socket.receive(sessionIdPacket);
            int sessionIdLength = sessionIdPacket.getLength();
            String sessionId = new String(sessionIdBuffer);
            System.out.println("SessionID sent by the server is " + sessionId);


            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line;
            int packetCount = 0;
            while ((line = br.readLine()) != null) {
                String lineToSend = new String();
                lineToSend += sessionId;
                lineToSend += "\n";
                lineToSend += line;
                packetCount++;
//                System.out.println("Sending to server:\n "  + lineToSend);
                byte[] lineBytes = lineToSend.getBytes();
                DatagramPacket packet = new DatagramPacket(lineBytes,lineBytes.length,serverAddress,port);
                packet.setLength(lineBytes.length);
                socket.send(packet);
            }
            br.close();
//

//            Thread.currentThread().sleep(10000);
//            System.out.println("Sleeping for 10 seconds waiting for server to handle all our data first");
            String doneStringToSend = new String("done");
            doneStringToSend += "\n";
            doneStringToSend += sessionId;
            doneStringToSend += "\n";
            doneStringToSend += packetCount;

            byte[] doneBytes = doneStringToSend.getBytes();
            DatagramPacket donePacket = new DatagramPacket(doneBytes,doneBytes.length,serverAddress,port);
            donePacket.setLength(donePacket.getLength());
            socket.send(donePacket);
            System.out.println("Sent done request");


        } finally {
            if(socket != null) {
                socket.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if(args.length != 3) {
            System.out.println("Usage: java UDPFileClient filename host port");
            System.exit(1);
        }

        UDPFileClient client = new UDPFileClient(args[0],args[1],Integer.parseInt(args[2]));
        client.sendFile();
    }
}
