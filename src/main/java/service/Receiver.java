package service;

import core.Command;
import core.Message;
import core.Process;
import utils.MemberListUpdater;
import utils.NeighborFilter;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * multi-threads receive messages from other processes
 */
public class Receiver extends Thread {
    private DatagramSocket datagramSocket;
    private static final int corePoolSize = 10;
    private static int maximumPoolSize = Integer.MAX_VALUE / 2;
    ThreadPoolExecutor threadPoolExecutor;

    public Receiver() throws SocketException {
        this.datagramSocket = new DatagramSocket(Main.port);
        threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 0L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

    }
    @Override
    public void run(){
        byte[] data = new byte[1024];
        DatagramPacket packet = new DatagramPacket(data, data.length);
        try {
            while(true){
                datagramSocket.receive(packet);
                Message message = Message.parseFrom(data);
                threadPoolExecutor.execute(new Executor(message));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static class Executor extends Thread {
        Message message;

        public Executor(Message message) {
            this.message = message;
        }

        @Override
        public void run() {
            System.out.println("get " + this.message.getCommand() + " command from "+ this.message.getHostName());
            switch (this.message.getCommand()) {
                case LEAVE:
                case WELCOME:
                    // update membership list
                    Main.membershipList = message.getMembershipList();
                    break;
                case PING:
                    // response to ping with ack
                    Main.timestamp = String.valueOf(Instant.now().getEpochSecond());
                    Sender.send(
                            message.getHostName(),
                            (int) message.getPort(),
                            Message.newBuilder()
                                    .setHostName(Main.hostName)
                                    .setTimestamp(Main.timestamp)
                                    .setPort(Main.port)
                                    .setCommand(Command.ACK)
                                    .build()
                    );
                    break;
                case ACK:
                    // modify isAck
                    List<Process> neighbor = NeighborFilter.getNeighbors();
                    for (int i = 0; i < neighbor.size(); i++) {
                        if(message.getHostName().equals(neighbor.get(i).getAddress())){
                            Main.isAck[i] = true;
                            break;
                        }
                    }
                    break;
                case UPDATE:
                    // update membershipList according to message's membership list
                    MemberListUpdater.update(message);
                    Sender.send(Message.newBuilder().setHostName(Main.hostName).setTimestamp(Main.timestamp).setPort(Main.port)
                            .addAllMembership(Main.membershipList).setCommand(Command.UPDATE).build());
                    break;
                case DISPLAY:
                    Main.display();
                    break;
                case JOIN:
                    // add this to membershipList and response with WELCOME message
                    MemberListUpdater.update(message);
                    Sender.send(
                            Message.newBuilder()
                                    .setCommand(Command.WELCOME)
                                    .setHostName(Main.hostName)
                                    .setTimestamp(Main.timestamp)
                                    .setPort(Main.port)
                                    .addAllMembership(Main.membershipList).build()
                    );
                    break;
                default:
                    break;
            }
        }
    }
}
