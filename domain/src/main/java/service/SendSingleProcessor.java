package service;

import core.Message;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

public class SendSingleProcessor extends Thread{
    public String hostName;
    public int port;
    public Message message;
    SendSingleProcessor(String hostName, int port, Message message){
        this.hostName = hostName;
        this.port = port;
        this.message = message;
    }
    @Override
    public void run(){
        DatagramSocket datagramSocket;
        try {
            datagramSocket = new DatagramSocket();
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
        byte[] arr = message.toByteArray();
        try {
            DatagramPacket packet;
            packet = new DatagramPacket(arr, 0, arr.length,
                    InetAddress.getByName(hostName), port);
            datagramSocket.send(packet);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
        datagramSocket.close();
    }
}
