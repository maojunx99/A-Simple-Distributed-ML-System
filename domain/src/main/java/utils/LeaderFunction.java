package utils;

import core.*;
import core.Process;
import service.Main;
import service.Sender;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class LeaderFunction {
    public static List<String> getDataNodesToStoreFile(String sdfsFileName) throws IOException {
        LogGenerator.loggingInfo(LogGenerator.LogType.INFO, "Start find node list");
        List<String> list = new ArrayList<>();
        if (Main.totalStorage.containsKey(sdfsFileName)) {
            return Main.totalStorage.get(sdfsFileName);
        }
        int index = 0;
        int aliveCnt = 0;
        boolean[] isSelected = new boolean[Main.membershipList.size()];
        Random r = new Random();
        for (int i = 0; i < Main.membershipList.size(); i++) {
            Process process = Main.membershipList.get(i);
            if (process.getStatus() == ProcessStatus.ALIVE) {
                aliveCnt++;
                if (r.nextBoolean()) {

                    System.out.println("[INFO] Add " + process.getAddress());
                    list.add(process.getAddress());
                    isSelected[i] = true;
                }
            }
        }
        if (aliveCnt < Main.copies) {
            LogGenerator.loggingInfo(LogGenerator.LogType.WARNING, "No enough nodes in the group!");
            return list;
        }
        while (list.size() < Main.copies) {
            System.out.println(list.size());
            if (Main.membershipList.get(index).getStatus() == ProcessStatus.ALIVE && !isSelected[index] && r.nextBoolean()) {
                list.add(Main.membershipList.get(index).getAddress());
                isSelected[index] = true;
            }
            index = (index + 1) % Main.membershipList.size();
        }
        Main.totalStorage.put(sdfsFileName, list);
        return list;
    }

    public static void reReplica(String address) throws IOException {
        for (String file : Main.totalStorage.keySet()) {
            List<String> nodeList = Main.totalStorage.get(file);
            if (nodeList.contains(address)) {
                nodeList.remove(address);
                String backupAddress = LeaderFunction.getDataNodeToStoreFile(file, nodeList);
                for (int i = 0; i < Main.R; i++) {
                    Sender.sendSDFS(nodeList.get(i), Main.port_sdfs,
                            Message.newBuilder()
                                    .setHostName(backupAddress)
                                    .setPort(Main.port_sdfs)
                                    .setCommand(Command.DOWNLOAD)
                                    .setMeta("replica")
                                    .setFile(FileOuterClass.File.newBuilder().setFileName(file).setVersion("5").build())
                                    .build()
                    );
                }
                nodeList.add(backupAddress);
                Main.totalStorage.put(file, nodeList);
            }
        }
    }

    private static String getDataNodeToStoreFile(String file, List<String> nodeList) throws IOException {
        int cnt = 0;
        for (Process process : Main.membershipList) {
            if (process.getStatus() == ProcessStatus.ALIVE) {
                if (!nodeList.contains(process.getAddress())) {
                    return process.getAddress();
                }
                cnt++;
            }
        }
        if (cnt < Main.copies) {
            LogGenerator.loggingInfo(LogGenerator.LogType.WARNING, "No enough data nodes to re-replica file " + file);
        }
        return "";
    }
}
