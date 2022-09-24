package service;

import core.Command;
import core.Message;
import utils.MemberListUpdater;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.time.Instant;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * multi-threads receive messages from other processes
 */
public class Receiver {
    private DatagramSocket datagramSocket;
    private static final int corePoolSize = 10;
    private static int maximumPoolSize = Integer.MAX_VALUE / 2;

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

    static class Executor extends Thread {
        Message message;

        public Executor(Message message) {
            this.message = message;
        }

        @Override
        public void run() {
            switch (this.message.getCommand()) {
                case LEAVE:
                case WELCOME:
                    // update membership list
                    MemberListUpdater.update(message);
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
                    for (int i = -2; i < 3; i++) {
                        if (Main.membershipList.get(Main.index + i).getAddress().equals(message.getHostName())) {
                            Main.isAck[i < 0 ? i + 2 : i + 1] = true;
                        }
                    }
                    break;
                case UPDATE:
                    // update membershipList according to message's membership list
                    MemberListUpdater.update(message);
                    Sender.send(Message.newBuilder().setHostName(Main.hostName).setTimestamp(Main.timestamp).setPort(Main.port)
                            .addAllMembershipList(Main.membershipList).setCommand(Command.UPDATE).build());
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
                                    .addAllMembershipList(Main.membershipList).build()
                    );
                    break;
                default:
                    break;
            }
        }
    }
}
