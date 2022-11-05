package service;

import com.google.protobuf.ByteString;
import core.*;
import core.Process;
import utils.LeaderFunction;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
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
            byte[] bytes = new byte[0];
            byte[] buff = new byte[1024];
            int k = -1;
            while((k = inputStream.read(buff, 0, buff.length)) > -1) {
                byte[] temp;
                if(buff[0]==0){
                    temp = new byte[bytes.length + k - 1];
                    System.arraycopy(bytes, 0, temp, 0, bytes.length);
                    System.arraycopy(buff, 2, temp, bytes.length, k - 2);  // copy current lot
                }else{
                    temp = new byte[bytes.length + k];
                    System.arraycopy(bytes, 0, temp, 0, bytes.length);
                    System.arraycopy(buff, 0, temp, bytes.length, k);  // copy current lot
                }
                bytes = temp; // call the temp buffer as your result buff
            }
            System.out.println(new String(bytes, StandardCharsets.UTF_8));
            System.out.println(Arrays.toString(bytes));
            this.message = Message.parseFrom(bytes);
            inputStream.close();
        }

        @Override
        public void run() {
            // if this process has left the group, then ignore all packages
            for (Process process : Main.membershipList) {
                if (process.getAddress().equals(Main.hostName) && process.getStatus() == ProcessStatus.LEAVED) {
                    System.out.println("[ERROR] left the group");
                    return;
                } else {
                    break;
                }
            }
            String fileName = null;
            if (message.hasFile()) {
                fileName = message.getFile().getFileName();
            }
            System.out.println(message.toString());
            switch (this.message.getCommand()) {
                case UPLOAD:
                    if (fileName == null) {
                        System.out.println("[ERROR] Nothing to upload!");
                        break;
                    }
                    String version = message.getFile().getVersion();
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
                    Main.storageList.put(fileName, Integer.parseInt(version));
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
                        System.out.println("[INFO] I'm not leader, abort UPLOAD_REQUEST!");
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
                    String versionDownload = message.getFile().getVersion();
                    String fileNameDownload = message.getFile().getFileName();
                    if (!Main.storageList.containsKey(fileNameDownload)) {
                        System.out.println("[ERROR] Can not find download file!");
                        break;
                    } else {
                        if (Main.storageList.get(message.getFile().getFileName()) > Integer.parseInt(versionDownload)) {
                            System.out.println("[ERROR] Can not find target version of " + message.getFile().getFileName() + " !");
                            break;
                        }
                    }
                    String downloadPath = Main.localDirectory + fileNameDownload;
                    File downloadFile = new File(downloadPath);
                    byte[] fileData = null;
                    try {
                        if (!downloadFile.exists()) {
                            System.out.println("[ERROR] Failed to find file: " + downloadPath);
                        } else {
                            FileInputStream fileInputStream = new FileInputStream(downloadFile);
                            int len = fileInputStream.available();
                            fileData = new byte[len];
                            int readAck = fileInputStream.read(fileData);
                            if (readAck != len) {
                                System.out.println("[ERROR] Errors in reading file " + downloadPath + " !");
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    Sender.send(
                            message.getHostName(),
                            (int) message.getPort(),
                            Message.newBuilder()
                                    .setCommand(Command.WRITE_ACK)
                                    .setHostName(Main.hostName)
                                    .setTimestamp(Main.timestamp)
                                    .setPort(Main.port_sdfs)
                                    .setFile(FileOuterClass.File.newBuilder()
                                            .setFileName(fileNameDownload).setContent(ByteString.copyFrom(fileData)).build())
                                    .build()
                    );
                    break;
                case DOWNLOAD_REQUEST:
                    if (!Main.isLeader) {
                        return;
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
                case REPLY:
                    Main.nodeList = message.getMembershipList();
                    break;
                default:
                    System.out.println("[ERROR] NAA");
                    break;
            }
        }
    }
}
