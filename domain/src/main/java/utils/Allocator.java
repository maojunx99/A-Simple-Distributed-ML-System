package utils;

import core.*;
import core.Process;
import service.Main;
import service.Models;
import service.Sender;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Allocator {
    public static ConcurrentHashMap<String, Integer> nextVMPointer = new ConcurrentHashMap<>();

    public static ConcurrentHashMap<String, List<String>> allocationMap = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, ConcurrentHashMap<String, Query>> jobMonitor = new ConcurrentHashMap<>();

    public static ConcurrentHashMap<String, List<Query>> vmQueryMatcher = new ConcurrentHashMap<>();

    public static HashMap<String, Integer> batchSizeMap = new HashMap<>();
    private final static int DEFAULT_BATCH_SIZE = 10;

    public static final List<String> modelList = new ArrayList<>();


    static {
        for (String model : Models.models.keySet()) {
            jobMonitor.put(model, new ConcurrentHashMap<>());
            batchSizeMap.put(model, DEFAULT_BATCH_SIZE);
        }
    }

    static public void allocateResourceTo(String model) {
        // calculate average recourse
        int total = batchSizeMap.get(model);
        for (String job : allocationMap.keySet()) {
            total += batchSizeMap.get(job);
        }
        List<String> releasedVM = new ArrayList<>();
        allocationMap.put(model, new ArrayList<>());
        modelList.add(model);
        System.out.println("[INFO] There are " + Main.availableWorker() + " available vms in the network");
        if (total == batchSizeMap.get(model)) {
            for (Process process : Main.membershipList) {
                if (process.getStatus().equals(ProcessStatus.ALIVE)) {
                    if (!process.getAddress().equals(Main.leader) && !process.getAddress().equals(Main.backupCoordinator)) {
                        allocationMap.get(model).add(process.getAddress());
                    }
                }
            }
        } else {
            for (String job : modelList) {
                int newSection = batchSizeMap.get(job) * Main.availableWorker() / total;
                if (newSection < allocationMap.get(job).size()) {
                    System.out.println("[INFO] Removing some source from " + job +
                            " [new section: " + newSection + ", original: " + allocationMap.get(job).size() + "]");
                    while (allocationMap.get(job).size() > newSection) {
                        releasedVM.add(allocationMap.get(job).get(0));
                        allocationMap.remove(allocationMap.get(job).remove(0));
                    }
                } else if (newSection > allocationMap.get(job).size()) {
                    System.out.println("[INFO] Allocating some source to " + job +
                            " [new section: " + newSection + ", original: " + allocationMap.get(job).size() + "]");
                    while (allocationMap.get(job).size() < newSection) {
                        allocationMap.get(job).add(releasedVM.get(0));
                        releasedVM.remove(0);
                    }
                }
            }
            while (releasedVM.size() > 0) {
                for (String job : modelList) {
                    allocationMap.get(job).add(releasedVM.get(0));
                    releasedVM.remove(0);
                    if (releasedVM.size() == 0) break;
                }
            }
        }
        if (!Main.hostName.equals(Main.backupCoordinator)) {
            StringBuilder stringBuilder = new StringBuilder();
            for (Map.Entry<String, List<String>> entry : allocationMap.entrySet()) {
                stringBuilder.append(entry.getKey()).append(" ");
                for (String str : entry.getValue()) {
                    stringBuilder.append(str).append(" ");
                }
                stringBuilder.append(";");
            }
            Sender.sendSDFS(Main.backupCoordinator, Main.port_sdfs,
                    Message.newBuilder()
                            .setCommand(Command.SYNC_INFO)
                            .setMeta(stringBuilder.toString())
                            .build());
        }
        nextVMPointer.put(model, 0);
    }

    static public Process getVm2RunTaskOfJob(String job) {
        if (!allocationMap.containsKey(job)) {
            allocateResourceTo(job);
        }
        if (nextVMPointer.get(job) >= allocationMap.get(job).size()) {
            nextVMPointer.put(job, 0);
        }
        Process vm = Process.newBuilder().setAddress(allocationMap.get(job).get(nextVMPointer.get(job))).setPort(Main.port_sdfs).build();
        nextVMPointer.put(job, (nextVMPointer.get(job) + 1) % allocationMap.get(job).size());
        return vm;
    }

    public static void recordAllocationOfQuery(Query query) {
        jobMonitor.get(query.job).put(query.getID(), query);
        vmQueryMatcher.get(query.executeAddress).add(query);
    }

    public static int countTillNow(String model) {
        if (!jobMonitor.containsKey(model)) {
            try {
                LogGenerator.loggingInfo(LogGenerator.LogType.ERROR, "no model: " + model);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return 0;
        }
        return jobMonitor.get(model).size();
    }

    public static int queryRate(String model) {
        List<Query> queryList;
        if (model.equals("RESNET50")) {
            queryList = Main.queryList.get(0);
        } else if (model.equals("INCEPTION_V3")) {
            queryList = Main.queryList.get(1);
        } else {
            try {
                LogGenerator.loggingInfo(LogGenerator.LogType.ERROR, "no model: " + model);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return 0;
        }
        int cnt = 0;
        long current = System.currentTimeMillis();
        for (int i = queryList.size() - 1; i >= 0; i--) {
            if (queryList.get(i).end != 0L) {
                if (current - queryList.get(i).end <= 10000) {
                    cnt++;
                } else {
                    return cnt;
                }
            }
        }
        return cnt;
    }


    static public double[][] processTime() {
        System.out.println("process time calculation");
        double[][] result = new double[modelList.size()][5];
        // 0 -> average
        // 1 -> percentile of 25
        // 2 -> percentile of 50
        // 3 -> percentile of 75
        // 4 -> deviation
        for (int i = 0; i < modelList.size(); i++) {
            List<Long> finishedList = new ArrayList<>();
            long sum = 0L;
            for (Query query : Main.queryList.get(i)) {
                if (query.end != 0L) {
                    finishedList.add(query.getTimeConsumption());
                    sum += query.getTimeConsumption();
                }
            }
            Collections.sort(finishedList);
            result[i][0] = (double) sum / (double) finishedList.size();
            result[i][1] = finishedList.get(finishedList.size() / 4);
            result[i][2] = finishedList.get(finishedList.size() / 2);
            result[i][3] = finishedList.get(3 * finishedList.size() / 4);
            long deviationSquare = 0;
            for (Long time : finishedList) {
                deviationSquare += (time - result[i][0]) * (time - result[i][0]);
            }
            result[i][4] = Math.sqrt((double) deviationSquare / (double) finishedList.size());
        }
        return result;
    }
}
