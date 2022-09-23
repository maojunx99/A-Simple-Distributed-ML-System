package service;

import core.Message;
import core.Process;
import utils.MemberListUpdater;

import java.io.IOException;
import java.net.*;
import java.util.List;

/**
 * To monitor neighbors, periodically check whether anyone of them crash
 */
public class Monitor extends Thread{
    private List<Process> membershipList = null;
    private List<DatagramPacket> datagramPacketList = null;
    private DatagramSocket datagramSocket = null;
    boolean [] isAck = null;
    public Monitor(List<Process> membershipList) throws SocketException {
        this.membershipList = Main.membershipList;
        this.isAck = Receiver.isAck;
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
                // ping 4 neighors every 1 s
                for(Process process : neighbors){
                    String address = process.getAddress();
                    long port = process.getPort();
                    DatagramPacket packet = null;
                    try {
                        packet = new DatagramPacket(ping, 0, ping.length,
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
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                //check whether receive "ACK" from each neighbors
                for(int k = 0; k < 4; k ++){
                    if(!isAck[k]){
                        membershipList.remove(k);
                    }
                }
                //send update message to 4 neighbors
                Message newMessage =
                MemberListUpdater.update();
            }
        }
    }

}
