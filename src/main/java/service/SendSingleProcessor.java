package service;

import core.Message;

import java.io.IOException;
import java.net.*;

public class SendSingleProcessor extends Thread{
    public String hostName = null;
    public int port = 0;
    public Message message = null;
    SendSingleProcessor(String hostName, int port, Message message){
        this.hostName = hostName;
        this.port = port;
        this.message = message;
    }
    @Override
    public void run(){
        Socket socket = null;
        byte[] arr = message.toByteArray();
        try {
            socket = new Socket(hostName, port);
            socket.getOutputStream().write(arr);
            socket.close();
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }
}
