package service;

import core.Command;
import core.Message;
import core.Process;
import utils.MemberListUpdater;
import utils.NeighborFilter;

import java.io.IOException;
import java.net.*;
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
        try {
            this.datagramSocket = new DatagramSocket(Main.port, InetAddress.getByName(Main.hostName));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 0L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

    }
    @Override
    public void run(){
        byte[] data = new byte[1024];
        DatagramPacket packet = new DatagramPacket(data, data.length);
        try {
            while(true){
                System.out.println("waiting a package");
                datagramSocket.receive(packet);
                System.out.println("received a package");
                byte[] temp = new byte[packet.getLength()];
                System.arraycopy(data,packet.getOffset(),temp,0,packet.getLength());
                Message message = Message.parseFrom(temp);
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
                    MemberListUpdater.update(message);
                    break;
                case WELCOME:
                    // update membership list
                    Main.membershipList = new ArrayList<>();
                    for(Process process : message.getMembershipList()){
                        Main.membershipList.add(process);
                    }
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
                    if(MemberListUpdater.update(message)){
                        Sender.send(Message.newBuilder().setHostName(Main.hostName).setTimestamp(Main.timestamp).setPort(Main.port)
                            .addAllMembership(Main.membershipList).setCommand(Command.UPDATE).build());
                    }
                    break;
                case DISPLAY:
                    Main.display();
                    break;
                case JOIN:
                    // add this to membershipList and response with WELCOME message
                    MemberListUpdater.update(message);
                    Sender.send(
                            message.getHostName(),
                            (int)message.getPort(),
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
