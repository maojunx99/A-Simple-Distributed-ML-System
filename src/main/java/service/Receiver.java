package service;

import core.Command;
import core.Message;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * multi-threads receive messages from other processes
 */
public class Receiver {
    private DatagramSocket datagramSocket;
    private static final int corePoolSize = 10;
    private static int maximumPoolSize = Integer.MAX_VALUE/2;
    public Receiver() throws SocketException {
        this.datagramSocket = new DatagramSocket(Main.port);
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 0L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        byte[] data = new byte[1024];
        DatagramPacket packet = new DatagramPacket(data, data.length);
        try {
            datagramSocket.receive(packet);
            Message message = Message.parseFrom(data);
            threadPoolExecutor.execute(new Executor(message));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    static class Executor extends Thread{
        Message message;
        public Executor(Message message){
            this.message = message;
        }
        @Override
        public void run(){
            switch (this.message.getCommand()){
                case LEAVE:
                    // TODO remove certain process from list

                    break;
                case PING:
                    //TODO response to ping
                    break;
                case WELCOME:
                    // TODO update membershipList and send update message to neighbors
                    break;
                case ACK:
                    // TODO modify isAck
                    break;
                case UPDATE:
                    // TODO update membershipList according to message's membership list
                    break;
                case DISPLAY:
                    // TODO
                    break;
                case JOIN:
                    // TODO add this to membershipList and response with WELCOME message
                    break;
                default:
                    break;
            }
        }
    }
}
