package service;

import core.Command;
import core.Message;
import core.Process;
import core.ProcessStatus;
import grep.client.Client;
import utils.LogGenerator;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.SocketException;
import java.time.Instant;
import java.util.*;

/**
 * Main class, response to console command
 */
public class Main {
    // membership list
    public volatile static List<Process> membershipList = null;
    // properties file path
    static String propertiesPath = "../setting.properties";
    // ack list
    public static boolean[] isAck;
    public static int monitorRange;
    public static String introducer;
    public static int port;
    public static int index;
    public static final String hostName;
    private static final Receiver receiver;
    static{
        try{
            receiver = new Receiver();
        }catch(SocketException e){
            throw new RuntimeException(e);
        }
    }

    static {
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public static String timestamp;

    private Main() throws IOException {
        Thread receiver = new Thread(new Receiver());
        receiver.start();
        Properties properties = new Properties();
        properties.load(this.getClass().getResourceAsStream(propertiesPath));
        monitorRange = Integer.parseInt(properties.getProperty("monitor_range"));
        isAck = new boolean[monitorRange];
        introducer = properties.getProperty("introducer");
        port = Integer.parseInt(properties.getProperty("port"));
        Instant time = Instant.now();
        timestamp = String.valueOf(time.getEpochSecond());
        membershipList = new ArrayList<>();
        membershipList.add(Process.newBuilder()
                                .setAddress(hostName)
                                .setPort(port)
                                .setTimestamp(timestamp)
                                .setStatus(ProcessStatus.ALIVE)
                                .build()
        );
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        Main main = new Main();
        Scanner scanner = new Scanner(System.in);
        String command;
        while (!(command = scanner.nextLine()).equals("EOF")) {
            switch (command) {
                case "join":
                    main.join();
                    break;
                case "leave":
                    main.leave();
                    break;
                case "list_mem":
                    display();
                    break;
                case "list_self":
                    main.print();
                    break;
                case "grep":
                    System.out.println("please input grep content");
                    String query = scanner.nextLine();
                    try {
                        Client.callServers("grep", query);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                default:
                    System.out.println("wrong command, please re-input");
            }
        }
        main.join();
    }

    public static void display() {
        // inform other processes to print their membership list
        Sender.send(Message.newBuilder().setCommand(Command.DISPLAY).build());
        // display local membership list
        System.out.println("-----------------------------------------");
        System.out.println("-            membership list            -");
        System.out.println("-----------------------------------------");
        for (Process process : membershipList) {
            System.out.println(process);
        }
        System.out.println("-----------------------------------------");
    }

    private void print() {
        System.out.println("Self ID: " + hostName + "@" + timestamp);
    }

    private void leave() {
        //call sender to inform others
        Sender.send(
                Message.newBuilder()
                        .setHostName(hostName)
                        .setPort(port)
                        .setTimestamp(timestamp)
                        .setCommand(Command.LEAVE)
                        .build()
        );
        System.out.println("[INFO] Left the group!");
    }

    private void join() throws InterruptedException {
        //call introducer to get list
        Sender.send(
                introducer,
                port,
                Message.newBuilder()
                        .setHostName(hostName)
                        .setPort(port)
                        .setTimestamp(timestamp)
                        .setCommand(Command.JOIN)
                        .build()
        );
        //call sender to inform others
        while (membershipList.size() == 1) {
            Thread.sleep(1000);
        }
        Sender.send(
                Message.newBuilder()
                        .setHostName(hostName)
                        .setPort(port)
                        .setCommand(Command.JOIN)
                        .setTimestamp(timestamp)
                        .build()
        );
        System.out.println("[INFO] Joined into the group!");
    }
}
