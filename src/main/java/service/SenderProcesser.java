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

public class SenderProcesser extends Thread{
    private Message message;
    private boolean onlyNeighbors;

    public  SenderProcesser(Message message, boolean onlyNeighbors) {
        this.message = message;
        this.onlyNeighbors = onlyNeighbors;
    }
    @Override
    public void run() {
        //
        Socket socket = null;
        byte[] arr = message.toByteArray();
        List<Process> processList = onlyNeighbors ? NeighborFilter.getNeighbors() : Main.membershipList;
        try {
            if(onlyNeighbors){
                for(Process process : processList){
                    socket = new Socket(process.getAddress(),(int)process.getPort());
                    socket.getOutputStream().write(arr);
                    socket.close();
                }
            }else{
                for(Process process : processList){
                    if(!process.getAddress().equals(Main.hostName)){
                        socket = new Socket(process.getAddress(),(int)process.getPort());
                        socket.getOutputStream().write(arr);
                        socket.close();
                    }
                }
            }
        }catch (IOException e){
            throw new RuntimeException(e);
        }
        if(message.getCommand() == Command.LEAVE){
            Main.membershipList = new ArrayList<>();
            Main.membershipList.add(Process.newBuilder().setAddress(Main.hostName).setPort(Main.port).setTimestamp(Main.timestamp).setStatus(ProcessStatus.LEAVED).build()); 
        }
    }
}
