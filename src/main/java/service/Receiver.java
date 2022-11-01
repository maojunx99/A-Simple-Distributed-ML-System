package service;

import core.*;
import core.Process;
import utils.LeaderFunction;
import utils.LogGenerator;
import utils.MemberListUpdater;
import utils.NeighborFilter;

import java.io.*;
import java.net.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * multi-threads receive messages from other processes
 */
public class Receiver extends Thread {
    //private final DatagramSocket datagramSocket;
    private static final int corePoolSize = 10;
    private final ServerSocket receiverSocket;
    private HashMap<String, List> storageList;
    private HashMap<String, List> globalStorageList;
    ThreadPoolExecutor threadPoolExecutor;

    public Receiver() throws SocketException {
        try {
            this.receiverSocket = new ServerSocket(Main.port);
            //this.datagramSocket = new DatagramSocket(Main.port, InetAddress.getByName(Main.hostName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        storageList = Main.storageList;
        if(Main.isLeader){
            globalStorageList = Main.globalStorageList;
        }
        int maximumPoolSize = Integer.MAX_VALUE / 2;
        threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 0L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    }
    @Override
    public void run(){
//        byte[] data = new byte[1024];
//        DatagramPacket packet = new DatagramPacket(data, data.length);
        Socket socket;
        try {
            while(true){
                socket = receiverSocket.accept();
                threadPoolExecutor.execute(new Executor(socket));
//                byte[] temp = new byte[packet.getLength()];
//                System.arraycopy(data,packet.getOffset(),temp,0,packet.getLength());
//                Message message = Message.parseFrom(temp);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static class Executor extends Thread {
        Message message;
        Socket socket;
        public Executor(Socket socket) throws IOException {
            this.socket = socket;
            InputStream inputStream = socket.getInputStream();
            DataInputStream in = new DataInputStream(inputStream);
            int len = in.available();
            byte[] temp = new byte[len];
            in.read(temp);
            this.message = Message.parseFrom(temp);
        }

        @Override
        public void run() {
            // if this process has left the group, then ignore all packages
            for (Process process: Main.membershipList) {
                if(process.getAddress().equals(Main.hostName)&&process.getStatus()==ProcessStatus.LEAVED){
                    return;
                }else{
                    break;
                }
            }
            if(this.message.getCommand() != Command.PING && this.message.getCommand() != Command.ACK){
                System.out.println("[MESSAGE] get " + this.message.getCommand() + " command from "
                        + this.message.getHostName() + "@" + this.message.getTimestamp());
            }
            String fileName = null;
            if(message.hasFile()){
                fileName = message.getFile().getFileName();
            }
            switch (this.message.getCommand()) {
                case LEAVE:
                    MemberListUpdater.update(message);
                    Sender.send(Message.newBuilder()
                                    .setHostName(Main.hostName)
                                    .setPort(Main.port)
                                    .setTimestamp(Main.timestamp)
                                    .addAllMembership(Main.membershipList)
                                    .setCommand(Command.UPDATE)
                                    .build(),
                            true
                    );
                    break;
                case WELCOME:
                    // update membership list
                    Main.membershipList = new ArrayList<>();
                    for (Process process: message.getMembershipList()) {
                        Main.membershipList.add(process);
                        if(!process.getAddress().equals(Main.hostName)){
                            try {
                                LogGenerator.logging(LogGenerator.LogType.JOIN, process.getAddress(), process.getTimestamp(), ProcessStatus.ALIVE);
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
                                    .setPort(Main.port)
                                    .setCommand(Command.ACK)
                                    .build()
                    );
//                    for(int i = 0; i < Main.membershipList.size(); i++){
//                        Process process = Main.membershipList.get(i);
//                        if(process.getAddress().equals(message.getHostName())){
//                            if(process.getStatus() == ProcessStatus.CRASHED){
//                                Main.membershipList.set(i, process.toBuilder().setStatus(ProcessStatus.ALIVE).setTimestamp(message.getTimestamp()).build());
//                                Sender.send(Message.newBuilder().
//                                        setHostName(Main.hostName)
//                                        .setPort(Main.port)
//                                        .setCommand(Command.UPDATE)
//                                        .setTimestamp(Main.timestamp)
//                                        .addAllMembership(Main.membershipList)
//                                        .build(),
//                                        true
//                                );
//                            }
//                            break;
//                        }
//                    }
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
                            .addAllMembership(Main.membershipList).setCommand(Command.UPDATE).build(), true);
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
                case UPLOAD:
                    if(fileName == null){
                        System.out.println("[ERROR] Nothing to upload!");
                        break;
                    }
                    String version = message.getFile().getVersion();
                    int index = fileName.lastIndexOf(".");
                    String filepath = fileName.substring(0,index) + "@" + version + "." + fileName.substring(index+1);
                    File file = new File(filepath);
                    try {
                        if(!file.exists()) {
                            file.createNewFile();
                        }
                        FileOutputStream fileOutputStream = new FileOutputStream(file);
                        fileOutputStream.write(message.getFile().getContent().toByteArray());
                    }catch (IOException e){
                        e.printStackTrace();
                    }
                    Main.storageList.put(fileName, Integer.parseInt(version));
                    Sender.send(
                            message.getHostName(),
                            (int)message.getPort(),
                            Message.newBuilder()
                                    .setCommand(Command.WRITE_ACK)
                                    .setHostName(Main.hostName)
                                    .setTimestamp(Main.timestamp)
                                    .setPort(Main.port)
                                    .build()
                    );
                    break;
                case UPLOAD_REQUEST:
                    if(fileName == null){
                        System.out.println("[ERROR] Nothing to upload!");
                        break;
                    }
                    List<Process> dataNodeList = LeaderFunction.getDataNodesToStoreFile();
                    if(!Main.totalStorage.containsKey(fileName)){
                        Main.totalStorage.put(fileName, new ArrayList<String>());
                    }
                    for(Process process : dataNodeList){
                        Main.totalStorage.get(fileName).add(process.getAddress());
                    }
                    //TODO:FIND FILE -> RETURN
                    Sender.send(
                            message.getHostName(),
                            (int)message.getPort(),
                            Message.newBuilder()
                                    .setCommand(Command.REPLY)
                                    .setHostName(Main.hostName)
                                    .setTimestamp(Main.timestamp)
                                    .setPort(Main.port)
                                    .addAllMembership(dataNodeList)
                                    .build()
                    );
                    break;
//                case DOWNLOAD:
//                    //TODO
//                    Sender.send(
//                            message.getHostName(),
//                            (int)message.getPort(),
//                            Message.newBuilder()
//                                    .setCommand(Command.WRITE_ACK)
//                                    .setHostName(Main.hostName)
//                                    .setTimestamp(Main.timestamp)
//                                    .setPort(Main.port)
//                                    .setFile(FileOuterClass.File.newBuilder()
//                                            .setFileName().build())
//                                    .build()
//                    );
//                    break;
//                case DOWNLOAD_REQUEST:
//                    addressList = new ArrayList<>();
//                    //TODO
//                    Sender.send(
//                            message.getHostName(),
//                            (int)message.getPort(),
//                            Message.newBuilder()
//                                    .setCommand(Command.REPLY)
//                                    .setHostName(Main.hostName)
//                                    .setTimestamp(Main.timestamp)
//                                    .setPort(Main.port)
//                                    .addAllMembership(addressList)
//                                    .build()
//                    );
//                    //TODO
//                    break;
                case REPLY:
                    for(Process process : message.getMembershipList()){
                        Main.nodeList.add(process);
                    }
                    break;
//                case ELECTED:
//                    //TODO
//                    break;
                default:
                    break;
            }
        }
    }
}
