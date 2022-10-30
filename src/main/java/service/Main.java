package service;

import core.Command;
import core.Message;
import core.Process;
import core.ProcessStatus;
import grep.client.Client;
import grep.server.Server;
import utils.LogGenerator;
import utils.MemberListUpdater;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
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
    public static int timeBeforeCrash;
    public static final String hostName;
    public static double lostRate;
    private static final Receiver receiver;

    private static String localDirectory;

    private static String sdfsDirectory;

    private static String leader;

    public static int copies;

    private static boolean isLeader = false;

    public static List<Process> nodeList;

    // filename -> version 1, 2, 3
    public static Map<String, Integer> storageList;

    public static Map<String, List<String>> totalStorage;


    static {
        try {
            receiver = new Receiver();
        } catch (SocketException e) {
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
        isAck = new boolean[monitorRange * 2];
        introducer = properties.getProperty("introducer");
        port = Integer.parseInt(properties.getProperty("port"));
        lostRate = Double.parseDouble(properties.getProperty("lost_rate"));
        timeBeforeCrash = Integer.parseInt(properties.getProperty("time_before_crash"));
        localDirectory = properties.getProperty("local_directory");
        sdfsDirectory = properties.getProperty("sdfs_directory");
        copies = Integer.parseInt(properties.getProperty("copies"));
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
        Thread monitor = new Thread(new Monitor());
        monitor.start();
        Thread server = new Thread(new Server());
        server.start();
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
                case "grep -c":
                case "grep -Ec":
                case "grep":
                    System.out.println("please input grep content");
                    String query = scanner.nextLine();
                    try {
                        Client.callServers("grep", query);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                case "put":
                    // TODO: implement upload
                    // - is leader
                    //   - decide which nodes store the file (may include leader itself)
                    //   - send UPLOAD message to these nodes
                    // - isn't leader
                    //   - send UPLOAD_REQUEST to the leader
                    //   - send UPLOAD message to members in the list returned by leader
                    String localFileName = scanner.next();
                    String sdfsFileName = scanner.next();
                    main.uploadFile(localFileName, sdfsFileName);


                    break;
                case "get":
                    // TODO: implement get
                    // - is leader
                    //   - find which nodes store the file
                    // - isn't leader
                    //   - send get request to leader and wait for reply
                    // - fetch files from these nodes
                    // - compare version and display the latest one
                    // - inform nodes that has old versions to update files
                    break;
                case "delete":
                    // TODO: implement delete file
                    // - is leader
                    //   - find which nodes store the file
                    //   - inform these nodes to delete all versions of the certain file
                    // - isn't leader
                    //   - send get request to the leader
                    break;
                case "ls":
                    // TODO: implement list storage of a file
                    // - is leader
                    //   - find which nodes store the file
                    // - isn't leader
                    //   - send request to leader and wait for the response
                    //   - display the list in console
                    break;
                case "store":
                    // TODO: implement store
                    // no difference between the leader and trivial nodes
                    // simply list out all files stored on this machine
                    break;
                case "get-version":
                    // TODO: implement get versions of a file
                    // - is leader
                    // - isn't leader
                    break;
                default:
                    System.out.println("[WARNING] Wrong command, please re-input");
            }
        }
        main.join();
    }

    public static void listMem() {
        // inform other processes to print their membership list
        Sender.send(Message.newBuilder()
                        .setHostName(hostName)
                        .setTimestamp(timestamp)
                        .setCommand(Command.DISPLAY)
                        .build(),
                false
        );
        display();
    }

    public static void display() {
        // display local membership list
        System.out.println("-----------------------------------------");
        System.out.println("-            membership list            -");
        System.out.print("-----------------------------------------");
        for (Process process : membershipList) {
            System.out.print("\n" + process);
        }
        System.out.println("-----------------------------------------");
    }

    private void print() {
        System.out.println("Self ID: " + hostName + "@" + timestamp);
    }

    private void leave() {
        //call sender to inform others
        for (int i = 0; i < Main.membershipList.size(); i++) {
            Process process = Main.membershipList.get(i);
            if (process.getAddress().equals(hostName)) {
                timestamp = Instant.now().getEpochSecond() + "";
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
                        .build(),
                true
        );
        System.out.println("[INFO] Left the group!");
    }

    private void join() throws InterruptedException {
        //call introducer to get list
        timestamp = Instant.now().getEpochSecond() + "";
        Main.membershipList.set(0, Main.membershipList.get(0).toBuilder().setStatus(ProcessStatus.ALIVE).build());
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
            if (cnt > 5) {
                System.out.println("[ERROR] the introducer is down!");
                return;
            }
        }
        Sender.send(
                Message.newBuilder()
                        .setHostName(hostName)
                        .setPort(port)
                        .setCommand(Command.UPDATE)
                        .addAllMembership(membershipList)
                        .setTimestamp(timestamp)
                        .build(),
                true
        );
        System.out.println("[INFO] Joined into the group!");
    }

    private boolean uploadFile(String localFileName, String sdfsFileName) {
        if(!isLeader){
            if(!uploadRequest(localFileName, sdfsFileName)){
                return false;
            }
        }
        // read file from local directory
        try {
            BufferedReader in = new BufferedReader(new FileReader(localDirectory + localFileName));
            System.out.println(in.readLine());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // TODO: upload
        return false;
    }

    private boolean uploadRequest(String localFileName, String sdfsFileName) {
        if (leader == null) {
            System.out.println("[WARNING] No leader currently! Please wait for a while and try again later!");
            return false;
        }
        // TODO: send UPLOAD_REQUEST to leader

        return true;
    }
}
