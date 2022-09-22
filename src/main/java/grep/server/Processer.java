package grep.server;

import grep.api.LogFile;
import grep.api.impl.GrepImpl;
import grep.api.impl.LogFileImpl;

import java.io.*;
import java.net.Socket;

public class Processer extends Thread {
    private int threshold = 30000;
    private Socket socket = null;
    private boolean flag = true;

    public Processer(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            InputStream inputStream = socket.getInputStream();
            DataInputStream in = new DataInputStream(inputStream);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            String str = null;
            while (true) {
                StringBuilder stringBuilder = new StringBuilder();
                str = in.readUTF();
                str = str.trim();
                if(str.equals("test")){
                    LogFile logFile = new LogFileImpl();
                    String content = logFile.getContent();
                    System.out.println(content);
                    out.writeUTF(content);
                }else{
                    stringBuilder.append(str);
                    String[] args = {"sh", "-c",stringBuilder.toString()};
                    for (String i : args) {
                        System.out.println("args: " + i);
                    }
                    String resGrep = new GrepImpl().grep(args);
                    int len = resGrep.length();
                    System.out.println("length of grep result: " + len);
                    if(len == 0){
                        out.writeUTF("");
                        continue;
                    }
                    for(int i = 0; i < len; i+=threshold){
                        out.writeUTF(resGrep.substring(i, Math.min(i+threshold,len)));
                    }
                }
                out.flush();
                System.out.println("write over"+"\n");
            }
        } catch (IOException e) {
        }
    }
}