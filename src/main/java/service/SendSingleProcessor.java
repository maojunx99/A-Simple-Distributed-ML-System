package service;

import core.Message;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

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
        DatagramSocket datagramSocket = null;
        try {
            datagramSocket = new DatagramSocket();
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
        byte[] arr = message.toByteArray();
        try {
            DatagramPacket packet = null;
            packet = new DatagramPacket(arr, 0, arr.length,
                    InetAddress.getByName(hostName), port);
            datagramSocket.send(packet);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
        datagramSocket.close();
    }
}
