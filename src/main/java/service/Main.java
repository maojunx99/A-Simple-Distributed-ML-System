package service;

import com.google.protobuf.Timestamp;
import core.Process;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Scanner;

/**
 * Main class, response to console command
 */
public class Main {
    // membership list
    static LinkedList<Process> membershipList = new LinkedList<>();
    // properties file path
    static String propertiesPath = "/resources/setting.properties";
    // ack list
    static boolean[] isAck;
    static int monitorRange;
    static String introducer;
    static int port;
    private final String hostName;
    private final Timestamp timestamp;

    private Main() throws IOException {
        Properties properties = new Properties();
        properties.load(this.getClass().getResourceAsStream(propertiesPath));
        monitorRange = Integer.parseInt(properties.getProperty("monitor_range"));
        isAck = new boolean[monitorRange];
        introducer = properties.getProperty("introducer");
        port = Integer.parseInt(properties.getProperty("port"));
        InetAddress address = InetAddress.getLocalHost();
        this.hostName = address.getHostName();
        Instant time = Instant.now();
        this.timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond()).build();
    }


    public static void main(String[] args) throws IOException {
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
                    main.display();
                    break;
                case "list_self":
                    main.print();
                    break;
                default:
                    System.out.println("wrong command, please re-input");
            }
        }
        main.join();
    }

    private void display() {
        System.out.println("-----------------------------------");
        System.out.println("-         membership list         -");
        System.out.println("-----------------------------------");
        for (Process process:membershipList) {
            System.out.println(process);
        }
        System.out.println("-----------------------------------");
    }

    private void print() {
        System.out.println("Self ID: " + hostName + "@" + timestamp.getSeconds());
    }

    private void leave() {
        //TODO call sender to inform others
        System.out.println("leaved the group!");
    }

    private void join() {
        //TODO call introducer to get list

        //TODO call sender to inform others
        System.out.println("joined into the group!");
    }
}
