package service;

import core.Command;
import core.Message;
import core.Process;
import core.ProcessStatus;
import utils.NeighborFilter;

import java.io.IOException;
import java.net.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * To monitor neighbors, periodically check whether anyone of them crash
 */
public class Monitor extends Thread{
    private final List<DatagramPacket> datagramPacketList = new ArrayList<>();
    private final DatagramSocket datagramSocket;
    boolean [] isAck;
    public Monitor() throws SocketException {
        this.isAck = Main.isAck;
        this.datagramSocket = new DatagramSocket();
    }
    @Override
    public void run(){
        // initialize Sockets of neighbors
        while(true){
            synchronized (Monitor.class){
                for(int j = 0; j < 4; j ++){
                    isAck[j] = false;
                }
                // ping 4 neighbors every 1 s
                List<Process> neighbors = NeighborFilter.getNeighbors();
                Main.timestamp = Instant.now().getEpochSecond() + "";
                for(Process process : neighbors){
                    Message message = Message.newBuilder().setHostName(Main.hostName)
                                        .setPort(Main.port)
                                        .setCommand(Command.PING)
                                        .setTimestamp(Main.timestamp)
                                        .addAllMembership(Main.membershipList).build();
                    byte[] data = message.toByteArray();
                    String address = process.getAddress();
                    long port = process.getPort();
                    DatagramPacket packet;
                    try {
                        packet = new DatagramPacket(data, 0, data.length,
                                InetAddress.getByName(address), (int) port);
                    } catch (UnknownHostException e) {
                        throw new RuntimeException(e);
                    }
                    datagramPacketList.add(packet);
                }
                for(DatagramPacket datagramPacket : datagramPacketList){
                    try {
                        datagramSocket.send(datagramPacket);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                try {
                    //wait for 1s
                    Thread.sleep(Main.timeBeforeCrash);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                //check whether receive "ACK" from each neighbors
                boolean hasCrash = false;
                for(int k = 0; k < neighbors.size(); k ++){
                    if(!isAck[k]){
                        hasCrash = true;
                        Process target = neighbors.get(k);
                        int length = Main.membershipList.size();
                        for(int i = 0; i < length; i++){
                            if(Main.membershipList.get(i).getAddress().equals(target.getAddress())){
                                Main.membershipList.set(i, Process.newBuilder()
                                        .setStatus(ProcessStatus.CRASHED)
                                        .setTimestamp(String.valueOf(Instant.now().getEpochSecond()))
                                        .setAddress(target.getAddress())
                                        .setPort(target.getPort()).build());
                            }
                        }
                    }
                }
                if(hasCrash){
                    //send update message to 4 neighbors
                    Message message = Message.newBuilder().setCommand(Command.UPDATE).setHostName(Main.hostName)
                            .setPort(Main.port).setTimestamp(Main.timestamp).addAllMembership(Main.membershipList).build();
                    Sender.send(message);
                }
            }
        }
    }

}
