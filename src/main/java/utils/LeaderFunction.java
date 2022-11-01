package utils;

import core.Process;
import core.ProcessStatus;
import service.Main;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class LeaderFunction {
    public static List<String> getDataNodesToStoreFile(String sdfsFileName) {
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
                    list.add(process.getAddress());
                    isSelected[i] = true;
                }
            }
        }
        if (aliveCnt < Main.copies) {
            System.out.println("[WARNING] No enough nodes in the group!");
            return list;
        }
        while (list.size() < Main.copies) {
            if (Main.membershipList.get(index).getStatus() == ProcessStatus.ALIVE && !isSelected[index] && r.nextBoolean()) {
                list.add(Main.membershipList.get(index).getAddress());
                isSelected[index] = true;
            }
            index = (index + 1) % Main.membershipList.size();
        }
        return list;
    }
}
