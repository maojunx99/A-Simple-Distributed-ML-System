package service.Processor;

import com.google.protobuf.ByteString;
import core.Command;
import core.FileOuterClass;
import core.Message;
import service.Main;
import service.Sender;
import utils.LogGenerator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class QueryProcessor extends Thread{
    private final Message message;
    public QueryProcessor(Message message){
        this.message = message;
    }
    @Override
    public void run() {
        String[] tmp = message.getMeta().split(" ");
        String option = tmp[0];
        int id = Integer.parseInt(tmp[1]);
        String queryContent = message.getFile().getContent().toStringUtf8();
        String[] query = queryContent.split(" ");
        List<String> queryResult = Main.models.Inference(query, option);
//        for(String i :queryResult){
//            System.out.println("query result: " + i);
//        }
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Query id: ").append(id).append("\n");
        for(String str : queryResult){
            stringBuilder.append(str).append("\n");
        }
        stringBuilder.append("\n");
        String filename = "Query_result_" + option;
        String filepath = Main.localDirectory + filename;
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
            for (int i = 0; i < query.length; i++) {
                String str = query[i].substring(query[i].lastIndexOf('/') + 1) + " " + queryResult.get(i) + "\n";
                fileOutputStream.write(str.getBytes(StandardCharsets.UTF_8));
            }
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Sender.sendSDFS(
                Main.leader,
                Main.port_sdfs,
                Message.newBuilder()
                        .setCommand(Command.QUERY_REPLY)
                        .setHostName(Main.hostName)
                        .setPort(Main.port_sdfs)
                        .setMeta(message.getMeta())
                        .setFile(FileOuterClass.File.newBuilder()
                                .setContent(ByteString.copyFrom(stringBuilder.toString().getBytes()))
                                .build())
                        .build()
        );
    }
}
