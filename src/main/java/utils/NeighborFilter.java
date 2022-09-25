package utils;

import core.Process;
import core.ProcessStatus;
import service.Main;

import java.util.ArrayList;
import java.util.List;

public class NeighborFilter {
    public static List<Process> getNeighbors() {
        List<Process> neighbors = new ArrayList<>();
        int cnt = 0;
        int self = -1;
        List<Process> temp = new ArrayList<>();
        for (Process process : Main.membershipList) {
            if (process.getAddress().equals(Main.hostName)) {
                self = cnt;
                if(process.getStatus()!=ProcessStatus.ALIVE){
                    temp.add(process);
                    cnt++;
                }
            }
            if (process.getStatus() == ProcessStatus.ALIVE) {
                temp.add(process);
                cnt++;
            }
        }
        if (cnt >= 5) {
            for (int i = 1; i <= Main.monitorRange; i++) {
                int index = self - i;
                index += index < 0 ? temp.size() : 0;
                neighbors.add(temp.get(index));
            }
            for (int i = 1; i <= Main.monitorRange; i++) {
                int index = self + i;
                index -= index >= temp.size() ? temp.size() : 0;
                neighbors.add(temp.get(index));
            }
        } else {
            switch (cnt) {
                case 4:
                case 3:
                    for (int i = 1; i <= Main.monitorRange; i++) {
                        int index = self - i;
                        index += index < 0 ? temp.size() : 0;
                        neighbors.add(temp.get(index));
                    }
                    break;
                case 2:
                    if (self == 0) {
                        neighbors.add(temp.get(1));
                    } else {
                        neighbors.add(temp.get(0));
                    }
                    break;
                default:
                    return neighbors;
            }
            if (cnt == 4) {
                int index = self + 1;
                index -= index >= temp.size() ? temp.size() : 0;
                neighbors.add(temp.get(index));
            }
        }
        return neighbors;
    }
}
