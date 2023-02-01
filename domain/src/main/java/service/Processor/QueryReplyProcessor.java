package service.Processor;

import core.Message;
import service.Main;
import utils.LogGenerator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class QueryReplyProcessor extends Thread{
    public Message message;

    public QueryReplyProcessor(Message message){
        this.message = message;
    }

    @Override
    public void run() {
        String[] tmp = message.getMeta().split(" ");
        String option = tmp[0];
        int id = Integer.parseInt(tmp[1]);
        Main.queryList.get(option.equals("RESNET50")? 0 : 1).get(id).setEndTime(System.currentTimeMillis());
        String filename = "Query_result_" + option;
        String filepath = Main.sdfsDirectory + filename;
        File file = new File(filepath);
        try {
            if (!file.exists()) {
                boolean isCreate = file.createNewFile();
                try {
                    LogGenerator.loggingInfo(LogGenerator.LogType.INFO, "Create file: " + filename +
                            (isCreate ? " success" : " failure") );
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            FileOutputStream fileOutputStream = new FileOutputStream(file, true);
            fileOutputStream.write(message.getFile().getContent().toByteArray());
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
