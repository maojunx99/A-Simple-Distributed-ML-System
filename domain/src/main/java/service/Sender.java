package service;

import core.Command;
import core.Message;
import core.Process;
import service.Processor.SendSingleProcessor;
import service.Processor.SenderProcessor;
import utils.LogGenerator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
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
    public static ThreadPoolExecutor senderThreadPool = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 0L,
            TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

    private static final List<String> allMachines;

    static {
        Properties properties = new Properties();
        try {
            properties.load(Sender.class.getResourceAsStream("../network.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        allMachines = Collections.singletonList(properties.getProperty("vm_address"));
    }

    public static void send(Message message, boolean onlyNeighbors) {
        if (new Random().nextDouble() < Main.lostRate) {
            return;
        }
        senderThreadPool.execute(new SenderProcessor(message, onlyNeighbors));
    }

    public static void send(String hostname, int port, Message message) {
        if (new Random().nextDouble() < Main.lostRate) {
            return;
        }
        senderThreadPool.execute(new SendSingleProcessor(hostname, port, message));
    }

    public static void sendSDFS(String hostname, int port, Message message) {
        senderThreadPool.execute(new SDFSSender(hostname, port, message));
    }

    public static void sendFile(String hostname, int port, String localFileName, String sdfsFileName) throws IOException {
        LogGenerator.loggingInfo(LogGenerator.LogType.INFO, "Sending file " + sdfsFileName + " to " + hostname);
        senderThreadPool.execute(new SDFSSender(hostname, port, localFileName, sdfsFileName));
    }

    public static void broadcastNewIntroducer(String introducer) {
        for (String address : allMachines) {
            senderThreadPool.execute(new SendSingleProcessor(
                    address,
                    Main.port_dns,
                    Message.newBuilder()
                            .setMeta(introducer)
                            .setCommand(Command.UPDATE)
                            .setHostName(Main.hostName)
                            .setPort(Main.port_dns)
                            .build()
            ));
        }
    }

    public static void electedNewLeaderAs(String leader) {
        for (Process process : Main.membershipList) {
            senderThreadPool.execute(new SendSingleProcessor(
                    process.getAddress(),
                    Main.port_membership,
                    Message.newBuilder()
                            .setMeta(leader)
                            .setCommand(Command.ELECTED)
                            .setHostName(Main.hostName)
                            .setPort(Main.port_membership)
                            .build()
            ));
        }
    }
}

