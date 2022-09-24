package service;

import core.Command;
import core.Message;
import core.Process;
import core.ProcessStatus;
import grep.client.Client;
import utils.LogGenerator;
import utils.MemberListUpdater;

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
    public static List<Process> membershipList = null;
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
        Thread receiver = new Thread(new Receiver());
        receiver.start();
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
                    listMem();
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
    public static void listMem(){
        // inform other processes to print their membership list
        Sender.send(Message.newBuilder().setCommand(Command.DISPLAY).build());
        display();
    }

    public static void display() {
        // display local membership list
        System.out.println("-----------------------------------------");
        System.out.println("-            membership list            -");
        System.out.print("-----------------------------------------");
        for (Process process : membershipList) {
            System.out.print("\n" + process);
            System.out.println("status: " + process.getStatus());
        }
        System.out.println("-----------------------------------------");
    }

    private void print() {
        System.out.println("Self ID: " + hostName + "@" + timestamp);
    }

    private void leave() {
        //call sender to inform others
        for(int i = 0; i < Main.membershipList.size(); i++){
            Process process = Main.membershipList.get(i);
            if(process.getAddress().equals(hostName)){
                timestamp = Instant.now().getEpochSecond()+"";
                Main.membershipList.set(i, process.toBuilder().setStatus(ProcessStatus.LEAVED).setTimestamp(timestamp).build());
                break;
            }
        }
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
        timestamp = Instant.now().getEpochSecond() + "";
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
        int cnt = 0;
        while (membershipList.size() == 1) {
            Thread.sleep(1000);
            cnt++;
            if(cnt > 5){
                System.out.println("[ERROR] the introducer is down!");
                return;
            }
        }
        Sender.send(
                Message.newBuilder()
                        .setHostName(hostName)
                        .setPort(port)
                        .setCommand(Command.UPDATE)
                        .setTimestamp(timestamp)
                        .build()
        );
        System.out.println("[INFO] Joined into the group!");
    }
}
