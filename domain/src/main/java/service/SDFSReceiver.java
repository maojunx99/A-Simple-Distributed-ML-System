package service;

import com.google.protobuf.ByteString;
import core.Process;
import core.*;
import org.tensorflow.op.core.All;
import service.Processor.QueryProcessor;
import service.Processor.QueryReplyProcessor;
import utils.*;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * multi-threads receive messages from other processes
 */
public class SDFSReceiver extends Thread {
    private static final int port = Main.port_sdfs;
    private static final int corePoolSize = 10;
    private final ServerSocket receiverSocket;
    private static boolean[] isInference;
//    static List<>
    ThreadPoolExecutor threadPoolExecutor;

    public SDFSReceiver() {
        try {
            this.receiverSocket = new ServerSocket(port);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        int maximumPoolSize = Integer.MAX_VALUE / 2;
        isInference = new boolean[Models.models.size()];
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
            try {
                LogGenerator.loggingInfo(LogGenerator.LogType.RECEIVING,
                        "Got " + message.getCommand() + " " + message.getFile().getFileName() + " from " + message.getHostName() + message.getMeta());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            switch (this.message.getCommand()) {
                case UPLOAD:
                    if (fileName == null) {
                        try {
                            LogGenerator.loggingInfo(LogGenerator.LogType.ERROR, "Nothing to upload!");
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
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
                                message.getFile().getContent().writeTo(fileOutputStream);
//                                fileOutputStream.write(message.getFile().getContent().toByteArray());
                            } else {
                                LogGenerator.loggingInfo(LogGenerator.LogType.ERROR, "Failed to create file: " + filepath);
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
                        try {
                            LogGenerator.loggingInfo(LogGenerator.LogType.ERROR, "Nothing to upload!");
                            return;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    List<String> dataNodeList;
                    try {
                        dataNodeList = LeaderFunction.getDataNodesToStoreFile(fileName);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
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
                        try {
                            LogGenerator.loggingInfo(LogGenerator.LogType.ERROR, "Nothing to download!");
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    }
                    if (!Main.storageList.containsKey(fileName)) {
                        try {
                            LogGenerator.loggingInfo(LogGenerator.LogType.ERROR, "Can not find download file!");
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    }
                    int latestVersion = Main.storageList.get(fileName);
                    String downloadPath = Main.sdfsDirectory + fileName;
                    int dotIndex = downloadPath.lastIndexOf(".");
                    int lastNumVersion = Integer.parseInt(message.getFile().getVersion());
                    for (int i = 0; i < Math.min(lastNumVersion, latestVersion); i++) {
                        String currentDownloadPath = downloadPath.substring(0, dotIndex) + "@" + (latestVersion - i) + downloadPath.substring(dotIndex);
                        fileName = currentDownloadPath.substring(Main.sdfsDirectory.length());
                        File downloadFile = new File(currentDownloadPath);
                        byte[] fileData = null;
                        try {
                            if (!downloadFile.exists()) {
                                LogGenerator.loggingInfo(LogGenerator.LogType.ERROR, "Failed to find file: " + currentDownloadPath);
                            } else {
                                FileInputStream fileInputStream = new FileInputStream(downloadFile);
                                fileData = MyReader.read(fileInputStream);
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        assert fileData != null;
                        Message message2 = Message.newBuilder()
                                .setCommand(Command.READ_ACK)
                                .setHostName(Main.hostName)
                                .setTimestamp(Main.timestamp)
                                .setPort(Main.port_sdfs)
                                .setFile(FileOuterClass.File.newBuilder()
                                        .setFileName(fileName).setContent(ByteString.copyFrom(fileData))
                                        .setVersion(String.valueOf(latestVersion - i))
                                        .build())
                                .setMeta(message.getMeta())
                                .build();
                        Sender.sendSDFS(
                                message.getHostName(),
                                (int) message.getPort(),
                                message2
                        );
                    }
                    break;
                case DOWNLOAD_REQUEST:
                    if (!Main.isLeader) {
                        return;
                    }
                    if (!Main.totalStorage.containsKey(fileName)) {
                        try {
                            LogGenerator.loggingInfo(LogGenerator.LogType.INFO, "Target file does not exit in SDFS: " + fileName);
                            Sender.sendSDFS(
                                    message.getHostName(),
                                    (int) message.getPort(),
                                    Message.newBuilder()
                                            .setCommand(Command.REPLY)
                                            .setHostName(Main.hostName)
                                            .setTimestamp(Main.timestamp)
                                            .setPort(Main.port_sdfs)
                                            .setMeta("No such file" + fileName)
                                            .build()
                            );
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
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
                    String savePath;
                    String meta = message.getMeta();
                    if (meta.equals("replica")) {
                        String temp = message.getFile().getFileName();
                        int i = temp.lastIndexOf("@");
                        temp = temp.substring(0, i) + temp.substring(i + 2);
                        savePath = Main.sdfsDirectory + temp;
                        Main.storageList.put(message.getFile().getFileName(), Integer.parseInt(message.getFile().getVersion()));
                    } else {
                        savePath = Main.localDirectory + message.getFile().getFileName();
                    }
                    File readFile = new File(savePath);
                    try {
                        if (!readFile.exists()) {
                            if (readFile.createNewFile()) {
                                FileOutputStream fileOutputStream = new FileOutputStream(readFile);
                                message.getFile().getContent().writeTo(fileOutputStream);
                            } else {
                                LogGenerator.loggingInfo(LogGenerator.LogType.INFO, "File already exists: " + readFile);
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
                    int newestVersion;
                    if (!Main.storageList.containsKey(deleteName)) {
                        try {
                            LogGenerator.loggingInfo(LogGenerator.LogType.ERROR, "No file: " + deleteName);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    } else {
                        newestVersion = Main.storageList.get(deleteName);
                    }
                    for (int i = 1; i <= newestVersion; i++) {
                        String fileTobeDeleted = deleteName.substring(0, temp) + "@" + i + deleteName.substring(temp);
                        try {
                            boolean isDelete = Files.deleteIfExists(Paths.get(Main.sdfsDirectory, fileTobeDeleted));
                            if (!isDelete) {
                                LogGenerator.loggingInfo(LogGenerator.LogType.WARNING, fileTobeDeleted + "does not exist!");
                            }
                            LogGenerator.loggingInfo(LogGenerator.LogType.INFO, "Successfully delete " + fileTobeDeleted);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    Main.storageList.remove(deleteName);
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
                    Main.totalStorage.remove(fileName);
                case REPLY:
                    Main.nodeList = message.getMembershipList();
                    break;
                case ELECTED:
                    Main.leader = message.getMeta();
                    Main.isLeader = false;
                    try {
                        LogGenerator.loggingInfo(LogGenerator.LogType.INFO, "Leader is " + Main.leader + " !");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                case QUERY_REQUEST:
                    if(!Main.isLeader){
                        return;
                    }
                    String option = message.getMeta();
                    if(!Models.models.containsKey(option)){
                        System.out.println("[WARNING] not such model: " + option);
                    }
                    if(isInference[Models.models.get(option)]){
                        System.out.println("[WARNING] Model " + option + " is in inference!");
                        break;
                    }
                    try {
                        LogGenerator.loggingInfo(LogGenerator.LogType.INFO, "Query request (type = " +
                                (Objects.equals(option, "RESNET50") ? "Resnet)" : "Inception)") + " from " + message.getHostName());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    new Thread(new MyQuery(option, Allocator.batchSizeMap.get(option))).start();
                    Sender.sendSDFS(Main.backupCoordinator, Main.port_sdfs, Message.newBuilder()
                            .setMeta(Allocator.batchSizeMap.get("RESNET50") + " " + Allocator.batchSizeMap.get("INCEPTION_V3"))
                            .setCommand(Command.SYNC_INFO)
                            .build());
                    isInference[Models.models.get(option)] = true;
                    break;
                case QUERY:
                    option = message.getMeta().split(" ")[0];
                    new Thread(new QueryProcessor(message)).start();
                    try {
                        LogGenerator.loggingInfo(LogGenerator.LogType.INFO, "Query (type = " +
                                (option.equals("RESNET50") ? "Resnet)" : "Inception)") + " from " + message.getHostName());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                case QUERY_REPLY:
                    new Thread(new QueryReplyProcessor(message)).start();
                    break;
                case SYNC_INFO:
                    if(!Main.isLeader){
                        if(Main.hostName.equals(Main.backupCoordinator)){
                            String[] sizes = message.getMeta().split(" ");
                            if(sizes[0].equals("totalStorage")){
                                if(!Main.totalStorage.containsKey(sizes[1])){
                                    Main.totalStorage.put(sizes[1], new ArrayList<>());
                                }
                                for(int i = 2; i < sizes.length; i ++){
                                    Main.totalStorage.get(sizes[1]).add(sizes[i]);
                                }
                                break;
                            }
                            if(sizes.length == 2){
                                int size1 = Integer.parseInt(sizes[0]);
                                int size2 = Integer.parseInt(sizes[1]);
                                Allocator.batchSizeMap.put("RESNET50", size1);
                                Allocator.batchSizeMap.put("INCEPTION_V3", size2);
                                break;
                            }else{
                                String[] allocation = message.getMeta().split(";");
                                for(String i :allocation){
                                    String[] tmp = i.trim().split(" ");
                                    List<String> val = new ArrayList<>();
                                    for(int j = 1; j < tmp.length; j ++){
                                        val.add(tmp[j]);
                                    }
                                    Main.availableWorker();
                                    Allocator.allocationMap.put(tmp[0], val);
                                    Allocator.nextVMPointer.put(tmp[0], 0);
                                }
                            }
                            break;
                        }
                        return;
                    }
                case RETRIEVE:
                    String item = message.getMeta();
                    switch(item){
                        case "allocation":
                            StringBuilder allocation = new StringBuilder();
                            for (String key: Allocator.allocationMap.keySet()) {
                                allocation.append(key).append(": ");
                                for (String vm: Allocator.allocationMap.get(key)) {
                                    allocation.append(vm).append(", ");
                                }
                                allocation.delete(allocation.length() - 2, allocation.length());
                                allocation.append("\n");
                            }
                            Sender.sendSDFS(message.getHostName(), Main.port_sdfs, Message.newBuilder().setCommand(Command.RETRIEVE).setMeta(String.valueOf(allocation)).setHostName(Main.hostName).setPort(Main.port_sdfs).build());
                            break;
                        case "batch_size":
                            StringBuilder batchSize = new StringBuilder();
                            for (String key: Allocator.batchSizeMap.keySet()) {
                                batchSize.append(key).append(": ").append(Allocator.batchSizeMap.get(key)).append("\n");
                            }
                            Sender.sendSDFS(message.getHostName(), Main.port_sdfs, Message.newBuilder().setCommand(Command.RETRIEVE).setMeta(String.valueOf(batchSize)).setHostName(Main.hostName).setPort(Main.port_sdfs).build());
                            break;
                        case "query-rate":
                            StringBuilder queryRate = new StringBuilder();
                            for (String model : Allocator.modelList) {
                                queryRate.append(model)
                                        .append(": {\n")
                                        .append("       query-rate: ").append(Allocator.queryRate(model)).append("\n")
                                        .append("       query-count: ").append(Allocator.countTillNow(model)).append("\n")
                                        .append("}\n");
                            }
                            Sender.sendSDFS(message.getHostName(), Main.port_sdfs, Message.newBuilder().setCommand(Command.RETRIEVE).setMeta(String.valueOf(queryRate)).setHostName(Main.hostName).setPort(Main.port_sdfs).build());
                            break;
                        case "statistic":
                            StringBuilder statistic = new StringBuilder();
                            double[][] result = Allocator.processTime();
                            statistic.append("STATISTIC:\n");
                            for (int i = 0; i < Allocator.modelList.size(); i++) {
                                statistic.append(Allocator.modelList.get(i))
                                        .append(": {\n")
                                        .append("       average time consumption: ").append(result[i][0]).append("ms\n")
                                        .append("       25% percentile: ").append(result[i][1]).append("ms\n")
                                        .append("       50% percentile: ").append(result[i][2]).append("ms\n")
                                        .append("       75% percentile: ").append(result[i][3]).append("ms\n")
                                        .append("       deviation: ").append(result[i][4]).append("\n")
                                        .append("}\n");
                            }
                            Sender.sendSDFS(message.getHostName(), Main.port_sdfs, Message.newBuilder().setCommand(Command.RETRIEVE).setMeta(String.valueOf(statistic)).setHostName(Main.hostName).setPort(Main.port_sdfs).build());
                            break;
                        default:
                            System.out.println(item);
                    }
                    break;
                default:
                    break;
            }
        }
    }
}
