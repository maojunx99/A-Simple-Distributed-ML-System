package service;

import core.Command;
import core.Message;
import core.Process;
import core.ProcessStatus;
import utils.LeaderFunction;
import utils.LogGenerator;
import utils.MemberListUpdater;
import utils.NeighborFilter;

import java.io.IOException;
import java.net.*;
import java.time.Instant;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * multi-threads receive messages from other processes
 */
public class Receiver extends Thread {
    private final DatagramSocket datagramSocket;
    private static final int corePoolSize = 10;
    ThreadPoolExecutor threadPoolExecutor;

    public Receiver() throws SocketException {
        try {
            this.datagramSocket = new DatagramSocket(Main.port_membership, InetAddress.getByName(Main.hostName));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        int maximumPoolSize = Integer.MAX_VALUE / 2;
        threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 0L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

    }

    @Override
    public void run() {
        byte[] data = new byte[1024];
        DatagramPacket packet = new DatagramPacket(data, data.length);
        try {
            while (true) {
                datagramSocket.receive(packet);
                byte[] temp = new byte[packet.getLength()];
                System.arraycopy(data, packet.getOffset(), temp, 0, packet.getLength());
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
            // if this process has left the group, then ignore all packages
            for (Process process : Main.membershipList) {
                if (process.getAddress().equals(Main.hostName) && process.getStatus() == ProcessStatus.LEAVED) {
                    return;
                } else {
                    break;
                }
            }
//            if (this.message.getCommand() != Command.PING && this.message.getCommand() != Command.ACK) {
//                System.out.println("[MESSAGE] get " + this.message.getCommand() + " command from "
//                        + this.message.getHostName() + "@" + this.message.getTimestamp());
//            }
            switch (this.message.getCommand()) {
                case LEAVE:
                    try {
                        MemberListUpdater.update(message);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    Sender.send(Message.newBuilder()
                                    .setHostName(Main.hostName)
                                    .setPort(Main.port_membership)
                                    .setTimestamp(Main.timestamp)
                                    .addAllMembership(Main.membershipList)
                                    .setCommand(Command.UPDATE)
                                    .build(),
                            true
                    );
                    if(Main.isLeader){
                        try {
                            LeaderFunction.reReplica(this.message.getHostName());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    break;
                case WELCOME:
                    // update membership list
                    Main.membershipList = new ArrayList<>();
                    for (Process process : message.getMembershipList()) {
                        Main.membershipList.add(process);
                        if (!process.getAddress().equals(Main.hostName)) {
                            try {
                                LogGenerator.loggingStatus(LogGenerator.LogType.JOIN, process.getAddress(), process.getTimestamp(), ProcessStatus.ALIVE);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
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
                                    .setPort(Main.port_membership)
                                    .setCommand(Command.ACK)
                                    .build()
                    );
                    break;
                case ACK:
                    // modify isAck
                    List<Process> neighbor = NeighborFilter.getNeighbors();
                    for (int i = 0; i < neighbor.size(); i++) {
                        if (message.getHostName().equals(neighbor.get(i).getAddress())) {
                            Main.isAck[i] = true;
                            break;
                        }
                    }
                    break;
                case UPDATE:
                    // update membershipList according to message's membership list
                    try {
                        if (MemberListUpdater.update(message)) {
                            Sender.send(Message.newBuilder().setHostName(Main.hostName).setTimestamp(Main.timestamp).setPort(Main.port_membership)
                                    .addAllMembership(Main.membershipList).setCommand(Command.UPDATE).build(), true);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                case DISPLAY:
                    Main.display();
                    break;
                case JOIN:
                    // add this to membershipList and response with WELCOME message
                    try {
                        MemberListUpdater.update(message);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    Sender.send(
                            message.getHostName(),
                            (int) message.getPort(),
                            Message.newBuilder()
                                    .setCommand(Command.WELCOME)
                                    .setHostName(Main.hostName)
                                    .setTimestamp(Main.timestamp)
                                    .setPort(Main.port_membership)
                                    .addAllMembership(Main.membershipList).build()
                    );
                    break;
                case ELECTED:
                    Main.leader = message.getMeta();
                    System.out.println("[INFO] Leader is " + Main.leader);
                default:
                    break;
            }
        }
    }
}
