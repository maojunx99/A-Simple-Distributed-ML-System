package service;

import core.Message;

import java.io.IOException;
import java.net.*;

public class SenderProcesser extends Thread{
    private Message message;

    public SenderProcesser(Message message) {
        this.message = message;
    }
    @Override
    public void run() {
        //
        DatagramSocket datagramSocket = null;
        try {
            datagramSocket = new DatagramSocket();
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
        String address = message.getHostName();
        int port = message.getPort();
        byte[] arr = message.toByteArray();
        try {
            DatagramPacket packet = null;
            packet = new DatagramPacket(arr, 0, arr.length,
                    InetAddress.getByName(address), (int) port);
            datagramSocket.send(packet);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
        datagramSocket.close();
    }
}
