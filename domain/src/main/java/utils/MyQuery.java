package utils;

import com.google.protobuf.ByteString;
import core.*;
import core.Process;
import service.Main;
import service.Sender;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class MyQuery extends Thread {
    private final int batch;
    private final String option;
    public boolean flag;

    public MyQuery(String option, int batch) {
        this.batch = batch;
        this.option = option;
        this.flag = true;
    }

    @Override
    public void run() {
        //assume batch <= 2 * dataset.size
        String path = Main.dataDirectory;        //要遍历的路径
        File file = new File(path);        //获取其file对象
        File[] fs = file.listFiles();
        List<String> filename = new ArrayList<>();
        assert fs != null;
        for (File file1 : fs) {
            filename.add(path + file1.getName());
        }
        int start = 0, end = 0;
        int id = (option.equals("RESNET50") ? Main.queryId_Resnet : Main.queryId_Inception);
        List<Query> lst = Main.queryList.get(option.equals("RESNET50") ? 0 : 1);
        while (flag) {
            System.out.println("Send query");
            end = start + batch - 1;
            StringBuilder stringBuilder = new StringBuilder();
            if (end <= filename.size() - 1) {
                for (int i = start; i <= end; i++) {
                    stringBuilder.append(filename.get(i));
                    if (i < end) {
                        stringBuilder.append(" ");
                    }
                }
            } else {
                for (int i = start; i <= filename.size() - 1; i++) {
                    stringBuilder.append(filename.get(i)).append(" ");
                }
                end = end - filename.size() - 1;
                for (int i = 0; i <= end; i++) {
                    stringBuilder.append(filename.get(i));
                    if (i < end) {
                        stringBuilder.append(" ");
                    }
                }
            }
            Process vm = Allocator.getVm2RunTaskOfJob(option);
            Query query = new Query(option, id, vm.getAddress());
            lst.add(query);
            Allocator.recordAllocationOfQuery(query);
            start = end + 1;
            Sender.sendSDFS(
                    vm.getAddress(),
                    (int) vm.getPort(),
                    Message.newBuilder()
                            .setMeta(option + " " + id)
                            .setCommand(Command.QUERY)
                            .setHostName(Main.hostName)
                            .setPort(Main.port_membership)
                            .setFile(FileOuterClass.File.newBuilder()
                                    .setContent((ByteString.copyFrom(stringBuilder.toString().getBytes())))
                                    .build()).build()
            );
            synchronized (MyQuery.class) {
                if (option.equals("RESNET50")) {
                    Main.queryId_Resnet++;
                } else {
                    Main.queryId_Inception++;
                }
            }
            id++;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
