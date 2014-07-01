package net.anands.skyline;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Created by kumar.
 */
public class UDPFileServer implements Runnable {
    private int port;
    private static Logger LOGGER = Logger.getLogger(UDPFileServer.class.getName());
    private boolean stopped = false;
    private ExecutorService pool = null;
    private DatagramSocket serverSocket = null;

    public UDPFileServer(int port,int poolSize) throws Exception{
        serverSocket = new DatagramSocket(port);
        pool = Executors.newFixedThreadPool(poolSize);
    }

//    Assumption for pass 1.  All request comes in as a single packet.
//    This won't work for big files
//    TODO redesign this for big files
//    Work on the protocol.
    public void run(){
        try {
            while(!isStopped()) {
                byte[] buffer = new byte[65508];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                serverSocket.receive(packet);
//                System.out.println("Creating a new ClientRequestHandler");
                pool.execute(new ClientRequestHandler(serverSocket,packet));
            }
        } catch (Exception e) {
            if(isStopped()){
//                Expected error condition.
                return;
            }
            e.printStackTrace();
        }
    }

    private synchronized void shutdownAndAwaitTermination(){
//        Close the serverSocket first.
        //        Try to do graceful shutdown
        pool.shutdown();
        try{

            LOGGER.info("Calling awaitTermination with a 60 second timeout.");
//            Wait for graceful shutdown.
            if(!pool.awaitTermination(60, TimeUnit.SECONDS)){
                LOGGER.info("Pool did not terminate cleanly.  Calling shutdownNow.");
//                Force shutdown
                pool.shutdownNow();
                if(!pool.awaitTermination(60,TimeUnit.SECONDS)){
                    LOGGER.info("Pool did not shutdown Properly.");
                }
            }
            LOGGER.info("Terminated");
        }catch(Exception e){
            e.printStackTrace();
        }
    }


    private synchronized void stop(){
        stopped = true;
//        Close the serverSocket so the while loop in the run breaks.
        LOGGER.info("Closing serverSocket");
        try{
            this.serverSocket.close();
        }catch(Exception e){
            e.printStackTrace();
        }
//        Shut down the threadpool
        LOGGER.info("Application stopped.  Awaiting termination.");
        shutdownAndAwaitTermination();
        LOGGER.info("Termination done.");
    }

    private synchronized  boolean isStopped(){
//        LOGGER.info("stopped is " + stopped);
        return stopped;
    }


    public static void main(String[] args) throws Exception{
        if(args.length != 2){
            System.out.println("Usage java UDPFileServer port poolSize");
            System.exit(1);
        }

        UDPFileServer server = new UDPFileServer(Integer.parseInt(args[0]),Integer.parseInt(args[1]));

        Thread listenerThread = new Thread(server);
        listenerThread.start();
        System.out.println("*********************************");
        System.out.println("Type 'stop' to stop this server...");
        System.out.println("*********************************");

        //        Block the main thread waiting for user to type in stop
        BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
        String fromUser;
        while((fromUser = stdIn.readLine()) != null){
            LOGGER.info("FromUser:" + fromUser);
            if(fromUser.equals("stop")){
                server.stop();
                System.exit(0);
            }
        }
    }
}
