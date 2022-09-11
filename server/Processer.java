package server;

import api.Grep;
import api.LogFile;
import api.impl.GrepImpl;
import api.impl.LogFileImpl;

import java.io.*;
import java.net.Socket;

public class Processer extends Thread {
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
                    System.out.println("resGrep: ");
                    System.out.println(resGrep);
                    out.writeUTF(resGrep);
                }
                out.flush();
                System.out.println("write over");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}