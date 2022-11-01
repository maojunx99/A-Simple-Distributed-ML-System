package service;

import core.Message;

import java.net.DatagramSocket;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Send messages to other processes
 * join - join the network
 * leave - leave the network
 * ack - response to ping
 */
public class Sender {
    private static final int corePoolSize = 5;
    private static final int maximumPoolSize = 10;
    public static DatagramSocket datagramSocket;
    public static ThreadPoolExecutor senderThreadPool = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 0L,
            TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

    public static void send(Message message, boolean onlyNeighbors) {
        if(new Random().nextDouble() < Main.lostRate){
            return;
        }
        senderThreadPool.execute(new SenderProcesser(message, onlyNeighbors));
    }
    public static void send(String hostname, int port, Message message){
        if(new Random().nextDouble() < Main.lostRate){
            return;
        }
        senderThreadPool.execute(new SendSingleProcessor(hostname, port, message));
    }
    public static void sendSDFS(String hostname, int port, Message message){
        senderThreadPool.execute(new SDFSSender(hostname, port, message));
    }

    public static void sendFile(String hostname, int port, String localFileName, String sdfsFileName){
        senderThreadPool.execute(new SDFSSender(hostname, port, localFileName, sdfsFileName));
    }
}

