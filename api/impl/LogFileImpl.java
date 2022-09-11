package api.impl;

import api.LogFile;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class LogFileImpl implements LogFile {
    private static String filepath = "test.log";
    private static Random random = new Random();
    private static String content = null;
    public static int num = 100;
    private static String[] Requests = {
            "\"GET /list HTTP/1.0\"",
            "\"POST /wp-admin HTTP/1.0\"",
            "\"DELETE /list HTTP/1.0\"",
            "\"PUT /list HTTP/1.0\""
    };
    private static String[] Websites = {
            "\"http://davis.com/list/privacy.php\"",
            "\"http://www.boone.org/terms.php\"",
            "\"http://www.robinson-rodriguez.com/index\"",
            "\"http://www.stone.com/about/\"",
            "\"http://johnson.net/\""
    };
    private static String[] SysBrowser = {
            "\"Mozilla/5.0 (X11; Linux x86_64; rv:1.9.7.20) Gecko/2017-08-21 08:51:13 Firefox/12.0\"",
            "\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_5; rv:1.9.2.20) Gecko/2011-12-16 00:51:08 Firefox/3.6.6\"",
            "\"Mozilla/5.0 (compatible; MSIE 5.0; Windows NT 5.0; Trident/3.0)\""
    };

    public static String GenerateIp(){
        StringBuilder stringBuilder = new StringBuilder();
        for(int i = 0; i < 4; i ++){
            stringBuilder.append(random.nextInt(255));
            if(i<3){
                stringBuilder.append(".");
            }
        }
        return stringBuilder.toString();
    }

    public static String GenerateDate(){
        SimpleDateFormat dateFormat = new SimpleDateFormat();
        dateFormat.applyPattern(" - - [dd/MM/yyyy:HH:mm:ss-0500] ");
        Date date = new Date();
        return dateFormat.format(date);
    }

    public static String GenerateRequest(){
        return Requests[random.nextInt(Requests.length)] + " ";
    }

    public  static String GenerateWeb(){
        return Websites[random.nextInt(Websites.length)] + " ";
    }

    public static String GenerateSysBrowser(){
        return SysBrowser[random.nextInt(SysBrowser.length)];
    }

    public void generate(){
        File file = new File(filepath);
        try {
            if(!file.exists()) {
                file.createNewFile();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        StringBuilder log =new StringBuilder();
        for(int i = 0; i < num; i++){
            log.append(GenerateIp()).append(GenerateDate()).append(GenerateRequest()).append(GenerateWeb()).append(GenerateSysBrowser()).append("\n");
        }
        content = log.toString();
        System.out.println(content);
        try{
            FileWriter fileWriter = new FileWriter(filepath);
            fileWriter.write(content);
            fileWriter.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public String getContent(){
        if(content == null){
            this.generate();
        }
        return content;
    }
}
