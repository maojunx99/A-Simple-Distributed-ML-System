package service;

import core.Message;
import core.Process;
import core.ProcessStatus;
import core.Command;
import utils.NeighborFilter;

import java.io.IOException;
import java.net.*;
import java.util.List;
import java.util.ArrayList;

public class SenderProcessor extends Thread{
    private final Message message;
    private final boolean onlyNeighbors;

    public SenderProcessor(Message message, boolean onlyNeighbors) {
        this.message = message;
        this.onlyNeighbors = onlyNeighbors;
    }
    @Override
    public void run() {
        //
        DatagramSocket datagramSocket;
        try {
            datagramSocket = new DatagramSocket();
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
        byte[] arr = message.toByteArray();
        List<Process> processList = onlyNeighbors ? NeighborFilter.getNeighbors() : Main.membershipList;
        try {
            if(onlyNeighbors){
                for(Process process : processList){
                    long port = process.getPort();
                    DatagramPacket packet = new DatagramPacket(arr, 0, arr.length,
                            InetAddress.getByName(process.getAddress()), (int) port);
                    datagramSocket.send(packet);
                }
            }else{
                for(Process process : processList){
                    if(!process.getAddress().equals(Main.hostName)){
                        long port = process.getPort();
                        DatagramPacket packet = new DatagramPacket(arr, 0, arr.length,
                                InetAddress.getByName(process.getAddress()), (int) port);
                        datagramSocket.send(packet);
                    }
                }
            }
        }catch (IOException e){
            throw new RuntimeException(e);
        }
        if(message.getCommand() == Command.LEAVE){
            Main.membershipList = new ArrayList<>();
            Main.membershipList.add(Process.newBuilder().setAddress(Main.hostName).setPort(Main.port_membership).setTimestamp(Main.timestamp).setStatus(ProcessStatus.LEAVED).build());
        }
        datagramSocket.close();
    }
}
