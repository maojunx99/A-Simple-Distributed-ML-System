package service;

import core.*;
import core.Process;
import dns.DNS;
import grep.client.Client;
import grep.server.Server;
import utils.LeaderFunction;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

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
    public static int port_membership;
    public static int port_sdfs;
    public static int port_dns;
    public static int timeBeforeCrash;
    public static final String hostName;
    public static double lostRate;

    public static String localDirectory;

    public static String sdfsDirectory;

    public static String leader;

    public static int copies;

    static boolean isLeader = false;

    public static List<Process> nodeList = null;

    // filename -> version 1, 2, 3
    public static Map<String, Integer> storageList;

    public static Map<String, List<String>> totalStorage;

    public static final int SLEEPING_INTERVAL = 5;

    public static final int MAX_SLEEPING_CYCLE = 10000;


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
        port_membership = Integer.parseInt(properties.getProperty("port_membership"));
        port_sdfs = Integer.parseInt(properties.getProperty("port_sdfs"));
        port_dns = Integer.parseInt(properties.getProperty("port_dns"));
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
                .setPort(port_membership)
                .setTimestamp(timestamp)
                .setStatus(ProcessStatus.ALIVE)
                .build()
        );
        introducer = DNS.getIntroducer();
        totalStorage = new HashMap<>();
        Thread receiver = new Thread(new Receiver());
        receiver.start();
        Thread monitor = new Thread(new Monitor());
        monitor.start();
        Thread server = new Thread(new Server());
        server.start();
        Thread sdfsReceiver = new Thread(new SDFSReceiver());
        sdfsReceiver.start();
        Thread introducerChecker = new IntroducerChecker();
        introducerChecker.start();
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
                    String localFileName = scanner.next();
                    String sdfsFileName = scanner.next();
                    if (main.uploadFile(localFileName, sdfsFileName)) {
                        System.out.println("[INFO] Upload success!");
                    } else {
                        System.out.println("[INFO] Upload aborted!");
                    }
                    break;
                case "get":
                    String fileName = scanner.next();
                    if (main.getFile(fileName)) {
                        System.out.println("[INFO] Upload success!");
                    } else {
                        System.out.println("[INFO] Upload aborted!");
                    }
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
                        .setPort(port_membership)
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
        introducer = DNS.getIntroducer();
        Sender.send(
                introducer,
                port_membership,
                Message.newBuilder()
                        .setHostName(hostName)
                        .setPort(port_membership)
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
                // no introducer then I become the introducer
                Sender.broadcastNewIntroducer(Main.hostName);
                return;
            }
        }
        boolean isLargest = true;
        for (Process process : membershipList) {
            System.out.println("[INFO] compare with process" + process.getAddress());
            if (process.getAddress().compareTo(Main.hostName) > 0) {
                isLargest = false;
                break;
            }
        }
        if (isLargest) {
            System.out.println("[ELECTED] Send out ELECTED message: " + Main.hostName);
            // end out elected message to other processes
            Sender.electedNewLeaderAs(Main.hostName);
            isLeader = true;
        }
        Sender.send(
                Message.newBuilder()
                        .setHostName(hostName)
                        .setPort(port_membership)
                        .setCommand(Command.UPDATE)
                        .addAllMembership(membershipList)
                        .setTimestamp(timestamp)
                        .build(),
                true
        );
        System.out.println("[INFO] Joined into the group!");
    }

    private boolean uploadFile(String localFileName, String sdfsFileName) {
        // - is leader
        //   - decide which nodes store the file (may include leader itself)
        //   - send UPLOAD message to these nodes
        // - isn't leader
        //   - send UPLOAD_REQUEST to the leader
        //   - send UPLOAD message to members in the list returned by leader
        if (!isLeader) {
            if (!uploadRequest(localFileName, sdfsFileName)) {
                return false;
            }
        } else {
            Main.nodeList = LeaderFunction.getDataNodesToStoreFile(sdfsFileName)
                    .stream()
                    .map(
                            (address) -> Process.newBuilder()
                                    .setAddress(address)
                                    .setPort(Main.port_sdfs)
                                    .build()
                    )
                    .collect(Collectors.toList());
        }
        // waiting for response from the server
        if (waiting4NodeList()) {
            System.out.println("[WARNING] Haven't got the node list from leader!");
            return false;
        }
        System.out.println("[INFO] Got the node list from leader!");
        for (Process process : Main.nodeList) {
            Sender.sendFile(process.getAddress(), (int) process.getPort(), localFileName, sdfsFileName);
        }
        Main.nodeList = null;
        return true;
    }

    private boolean uploadRequest(String localFileName, String sdfsFileName) {
        if (leader == null) {
            System.out.println("[WARNING] No leader currently! Please wait for a while and try again later!");
            return false;
        }
        Sender.sendSDFS(
                leader, Main.port_sdfs, Message.newBuilder()
                        .setCommand(Command.UPLOAD_REQUEST)
                        .setHostName(Main.hostName)
                        .setPort(Main.port_sdfs)
                        .setFile(FileOuterClass.File.newBuilder().setFileName(sdfsFileName))
                        .build()
        );
        return true;
    }

    private boolean getFile(String fileName) {
        // - is leader
        //   - find which nodes store the file
        // - isn't leader
        //   - send get request to leader and wait for reply
        // - fetch files from these nodes
        // - compare version and display the latest one
        // - TODO: inform nodes that have old versions to update files
        if (isLeader) {
            Main.nodeList = LeaderFunction.getDataNodesToStoreFile(fileName)
                    .stream()
                    .map(
                            (address) -> Process.newBuilder()
                                    .setAddress(address)
                                    .setPort(Main.port_sdfs)
                                    .build()
                    )
                    .collect(Collectors.toList());
        } else {
            if (!getRequest(fileName)) {
                return false;
            }
        }
        // waiting for response from the server
        if (waiting4NodeList()) {
            System.out.println("[WARNING] Haven't got the node list from leader!");
            return false;
        }
        System.out.println("[INFO] Got the node list from leader!");

        for (Process process : Main.nodeList) {
            Sender.sendSDFS(
                    process.getAddress(),
                    port_sdfs,
                    Message.newBuilder()
                            .setHostName(Main.hostName)
                            .setPort(port_sdfs)
                            .setCommand(Command.DOWNLOAD)
                            .setFile(FileOuterClass.File.newBuilder()
                                    .setFileName(fileName)
                                    .setVersion(String.valueOf(1)))
                            .build()
            );
        }
        Main.nodeList = null;
        return true;
    }

    private boolean getRequest(String fileName) {
        if (leader == null) {
            System.out.println("[WARNING] No leader currently! Please wait for a while and try again later!");
            return false;
        }
        // file version refer to recent nums of versions
        Sender.sendSDFS(
                leader, Main.port_sdfs, Message.newBuilder()
                        .setCommand(Command.UPLOAD_REQUEST)
                        .setHostName(Main.hostName)
                        .setPort(Main.port_sdfs)
                        .setFile(FileOuterClass.File.newBuilder().setFileName(fileName).setVersion(String.valueOf(1)))
                        .build()
        );
        return true;
    }

    private boolean waiting4NodeList() {
        int cnt = 0;
        while (Main.nodeList == null && cnt < MAX_SLEEPING_CYCLE) {
            try {
                Thread.sleep(SLEEPING_INTERVAL);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            cnt++;
        }
        return Main.nodeList == null || Main.nodeList.size() == 0;
    }
}
