package service;

import core.Command;
import core.Message;
import core.Process;
import core.ProcessStatus;
import utils.LeaderFunction;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * multi-threads receive messages from other processes
 */
public class SDFSReceiver extends Thread {
    //private final DatagramSocket datagramSocket;
    private static final int port = Main.port_sdfs;
    private static final int corePoolSize = 10;
    private final ServerSocket receiverSocket;
    ThreadPoolExecutor threadPoolExecutor;

    public SDFSReceiver() {
        try {
            this.receiverSocket = new ServerSocket(port);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        int maximumPoolSize = Integer.MAX_VALUE / 2;
        threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 0L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    }

    @Override
    public void run(){
        Socket socket;
        try {
            while(true){
                socket = receiverSocket.accept();
                threadPoolExecutor.execute(new Executor(socket));
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
                case UPLOAD:
                    if(fileName == null){
                        System.out.println("[ERROR] Nothing to upload!");
                        break;
                    }
                    String version = message.getFile().getVersion();
                    int index = fileName.lastIndexOf(".");
                    String filepath = Main.sdfsDirectory + fileName.substring(0,index) + "@" + version + "." + fileName.substring(index+1);
                    File file = new File(filepath);
                    try {
                        if(!file.exists()) {
                            if(file.createNewFile()){
                                FileOutputStream fileOutputStream = new FileOutputStream(file);
                                fileOutputStream.write(message.getFile().getContent().toByteArray());
                            }else{
                                System.out.println("[ERROR] Failed to create file: " + filepath);
                            }
                        }

                    }catch (IOException e){
                        e.printStackTrace();
                    }
                    Main.storageList.put(fileName, Integer.parseInt(version));
                    Sender.sendSDFS(
                            message.getHostName(),
                            (int)message.getPort(),
                            Message.newBuilder()
                                    .setCommand(Command.WRITE_ACK)
                                    .setHostName(Main.hostName)
                                    .setTimestamp(Main.timestamp)
                                    .setPort(port)
                                    .build()
                    );
                    break;
                case UPLOAD_REQUEST:
                    if(!Main.isLeader){
                        return;
                    }
                    if(fileName == null){
                        System.out.println("[ERROR] Nothing to upload!");
                        break;
                    }
                    List<String> dataNodeList = LeaderFunction.getDataNodesToStoreFile(fileName);
                    if(!Main.totalStorage.containsKey(fileName)){
                        Main.totalStorage.put(fileName, new ArrayList<>());
                    }
                    List<Process> dataNodeMemberList = new ArrayList<>();
                    for(String dataNode : dataNodeList){
                        Main.totalStorage.get(fileName).add(dataNode);
                        dataNodeMemberList.add(Process.newBuilder().setAddress(dataNode).build());
                    }
                    Sender.sendSDFS(
                            message.getHostName(),
                            (int)message.getPort(),
                            Message.newBuilder()
                                    .setCommand(Command.REPLY)
                                    .setHostName(Main.hostName)
                                    .setTimestamp(Main.timestamp)
                                    .setPort(port)
                                    .addAllMembership(dataNodeMemberList)
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
                    Main.nodeList = message.getMembershipList();
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
