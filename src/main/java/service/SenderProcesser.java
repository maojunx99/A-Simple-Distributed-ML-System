package service;

import core.Message;
import core.Process;
import utils.NeighborFilter;

import java.io.IOException;
import java.net.*;
import java.util.List;

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
        byte[] arr = message.toByteArray();
        List<Process> processList = NeighborFilter.getNeighbors();
        try {
            for(Process process : processList){
                String address = process.getAddress();
                long port = process.getPort();
                DatagramPacket packet = new DatagramPacket(arr, 0, arr.length,
                        InetAddress.getByName(process.getAddress()), (int) port);
                datagramSocket.send(packet);
            }
        }catch (IOException e){
            throw new RuntimeException(e);
        }
        datagramSocket.close();
    }
}
