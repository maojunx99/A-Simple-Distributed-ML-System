package service;

import com.google.protobuf.ByteString;
import core.Process;
import core.*;
import utils.LeaderFunction;
import utils.MyReader;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
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
    public void run() {
        Socket socket;
        try {
            while (true) {
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
            this.message = Message.parseFrom(MyReader.read(inputStream));
            inputStream.close();
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
            String fileName = null;
            if (message.hasFile()) {
                fileName = message.getFile().getFileName();
            }
            System.out.println("[MESSAGE] receive message: \n" + message);
            switch (this.message.getCommand()) {
                case UPLOAD:
                    if (fileName == null) {
                        System.out.println("[ERROR] Nothing to upload!");
                        break;
                    }
                    int version = Main.storageList.getOrDefault(fileName, 0) + 1;
                    Main.storageList.put(fileName, version);
                    int index = fileName.lastIndexOf(".");
                    String filepath = Main.sdfsDirectory + fileName.substring(0, index) + "@" + version + "." + fileName.substring(index + 1);
                    File file = new File(filepath);
                    try {
                        if (!file.exists()) {
                            if (file.createNewFile()) {
                                FileOutputStream fileOutputStream = new FileOutputStream(file);
                                fileOutputStream.write(message.getFile().getContent().toByteArray());
                            } else {
                                System.out.println("[ERROR] Failed to create file: " + filepath);
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    Sender.sendSDFS(
                            message.getHostName(),
                            (int) message.getPort(),
                            Message.newBuilder()
                                    .setCommand(Command.WRITE_ACK)
                                    .setHostName(Main.hostName)
                                    .setTimestamp(Main.timestamp)
                                    .setPort(port)
                                    .build()
                    );
                    break;
                case UPLOAD_REQUEST:
                    if (!Main.isLeader) {
                        return;
                    }
                    if (fileName == null) {
                        System.out.println("[ERROR] Nothing to upload!");
                        break;
                    }
                    List<String> dataNodeList = LeaderFunction.getDataNodesToStoreFile(fileName);
                    List<Process> dataNodeMemberList = new ArrayList<>();
                    for (String dataNode : dataNodeList) {
                        dataNodeMemberList.add(Process.newBuilder().setAddress(dataNode).build());
                    }
                    Sender.sendSDFS(
                            message.getHostName(),
                            (int) message.getPort(),
                            Message.newBuilder()
                                    .setCommand(Command.REPLY)
                                    .setHostName(Main.hostName)
                                    .setTimestamp(Main.timestamp)
                                    .setPort(port)
                                    .addAllMembership(dataNodeMemberList)
                                    .build()
                    );
                    break;
                case DOWNLOAD:
                    if (fileName == null) {
                        System.out.println("[ERROR] Nothing to download!");
                        break;
                    }
                    if (!Main.storageList.containsKey(fileName)) {
                        System.out.println("[ERROR] Can not find download file!");
                        break;
                    }
                    int latestVersion = Main.storageList.get(fileName);
                    String downloadPath = Main.sdfsDirectory + fileName;
                    int dotIndex = downloadPath.lastIndexOf(".");
                    downloadPath = downloadPath.substring(0, dotIndex) + "@" + latestVersion + downloadPath.substring(dotIndex);
                    fileName = downloadPath.substring(Main.sdfsDirectory.length());
                    File downloadFile = new File(downloadPath);
                    byte[] fileData = null;
                    try {
                        if (!downloadFile.exists()) {
                            System.out.println("[ERROR] Failed to find file: " + downloadPath);
                        } else {
                            FileInputStream fileInputStream = new FileInputStream(downloadFile);
                            fileData = MyReader.read(fileInputStream);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    Message message2 = Message.newBuilder()
                            .setCommand(Command.READ_ACK)
                            .setHostName(Main.hostName)
                            .setTimestamp(Main.timestamp)
                            .setPort(Main.port_sdfs)
                            .setFile(FileOuterClass.File.newBuilder()
                                    .setFileName(fileName).setContent(ByteString.copyFrom(fileData)).build())
                            .build();
                    Sender.sendSDFS(
                            message.getHostName(),
                            (int) message.getPort(),
                            message2
                    );
                    break;
                case DOWNLOAD_REQUEST:
                    if (!Main.isLeader) {
                        return;
                    }
                    if (!Main.totalStorage.containsKey(fileName)) {
                        System.out.println("[INFO] Target file does not exit in SDFS: " + fileName);
                    }
                    List<Process> tempList = new ArrayList<>();
                    for (String i : Main.totalStorage.get(fileName)) {
                        tempList.add(Process.newBuilder().setAddress(i).build());
                    }
                    Sender.sendSDFS(
                            message.getHostName(),
                            (int) message.getPort(),
                            Message.newBuilder()
                                    .setCommand(Command.REPLY)
                                    .setHostName(Main.hostName)
                                    .setTimestamp(Main.timestamp)
                                    .setPort(Main.port_sdfs)
                                    .addAllMembership(tempList)
                                    .build()
                    );
                    break;
                case READ_ACK:
                    String savePath = Main.localDirectory + message.getFile().getFileName();
                    File readFile = new File(savePath);
                    try {
                        if (!readFile.exists()) {
                            if (readFile.createNewFile()) {
                                FileOutputStream fileOutputStream = new FileOutputStream(readFile);
                                fileOutputStream.write(message.getFile().getContent().toByteArray());
                            } else {
                                System.out.println("[INFO] File already exists: " + readFile);
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    Main.READ_ACK++;
                    break;
                case WRITE_ACK:
                    Main.WRITE_ACK++;
                    break;
                case DELETE:
                    String deleteName = message.getMeta();
                    int temp = deleteName.lastIndexOf(".");
                    int newestVersion = Main.storageList.get(deleteName);
                    deleteName = deleteName.substring(0, temp) + "@" + newestVersion + deleteName.substring(temp);
                    for(int i = 1; i <= newestVersion; i++){
                        try {
                            boolean isDelete = Files.deleteIfExists(Paths.get(Main.sdfsDirectory, deleteName));
                            if(!isDelete){
                                System.out.println("[WARN] " + deleteName + "does not exist!");
                            }
                            System.out.println("[INFO] Successfully delete" + deleteName);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    break;
                case DELETE_REQUEST:
                    if (!Main.isLeader) {
                        return;
                    }
                    List<String> deleteList = Main.totalStorage.get(fileName);
                    for (String i : deleteList) {
                        Sender.sendSDFS(
                                i,
                                (int) message.getPort(),
                                Message.newBuilder()
                                        .setCommand(Command.DELETE)
                                        .setHostName(Main.hostName)
                                        .setTimestamp(Main.timestamp)
                                        .setPort(Main.port_sdfs)
                                        .setMeta(fileName)
                                        .build()
                        );
                    }
                case REPLY:
                    Main.nodeList = message.getMembershipList();
                    break;
                case ELECTED:
                    Main.leader = message.getMeta();
                    System.out.println("[INFO] Leader is " + Main.leader + " !");
                    break;
                default:
                    break;
            }
        }
    }
}
