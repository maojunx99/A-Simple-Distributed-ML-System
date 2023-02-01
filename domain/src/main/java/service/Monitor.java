package service;

import core.Command;
import core.Message;
import core.Process;
import core.ProcessStatus;
import utils.*;

import java.io.IOException;
import java.net.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * To monitor neighbors, periodically check whether anyone of them crash
 */
public class Monitor extends Thread {
    private final DatagramSocket datagramSocket;
    boolean[] isAck;

    public Monitor() throws SocketException {
        this.isAck = Main.isAck;
        this.datagramSocket = new DatagramSocket();
    }

    @Override
    public void run() {
        // initialize Sockets of neighbors
        while (true) {
            synchronized (Monitor.class) {
                for (int j = 0; j < 4; j++) {
                    isAck[j] = false;
                }
                // ping 4 neighbors every 1 s
                List<Process> neighbors = NeighborFilter.getNeighbors();
                Main.timestamp = Instant.now().getEpochSecond() + "";
                List<DatagramPacket> datagramPacketList = new ArrayList<>();
                for (Process process : neighbors) {
                    Message message = Message.newBuilder().setHostName(Main.hostName)
                            .setPort(Main.port_membership)
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
                boolean hasCrash = heartbeat(datagramPacketList);
                if(hasCrash){
                    int cnt = 3;
                    while(hasCrash && cnt > 0){
                        hasCrash = heartbeat(datagramPacketList);
                        cnt--;
                    }
                }else{
                    continue;
                }
                for (int k = 0; k < neighbors.size(); k++) {
                    if (!isAck[k]) {
                        Process neighbor = neighbors.get(k);
                        if(Main.hostName.equals(Main.backupCoordinator) && neighbor.getAddress().equals(Main.leader) && !Main.isLeader){
                            Sender.send(Message.newBuilder()
                                    .setMeta(Main.hostName)
                                    .setCommand(Command.ELECTED)
                                    .build(),false);
                            Main.isLeader =true;
                            Main.isCoordinator = true;
                            new Thread(new MyQuery("RESNET50", Allocator.batchSizeMap.get("RESNET50"))).start();
                            new Thread(new MyQuery("INCEPTION_V3", Allocator.batchSizeMap.get("INCEPTION_V3"))).start();
                        }
                        boolean _continue = false;
                        for (Process process : Main.membershipList) {
                            if (process.getAddress().equals(neighbor.getAddress())) {
                                if (process.getStatus() != ProcessStatus.ALIVE) {
                                    _continue = true;
                                }
                                break;
                            }
                        }
                        if (_continue) continue;
                        Process target = neighbors.get(k);
                        int length = Main.membershipList.size();
                        for (int i = 0; i < length; i++) {
                            if (Main.membershipList.get(i).getAddress().equals(target.getAddress())) {
                                Main.membershipList.set(i, Process.newBuilder()
                                        .setStatus(ProcessStatus.CRASHED)
                                        .setTimestamp(String.valueOf(Instant.now().getEpochSecond()))
                                        .setAddress(target.getAddress())
                                        .setPort(target.getPort()).build());
                                // if this is the leader, then re-replica files on this machine
                                if (Main.isLeader) {
                                    try {
                                        LeaderFunction.reReplica(Main.membershipList.get(i).getAddress());
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                            }
                        }
                        System.out.println("[CRASH] " + Main.hostName + "@" + Main.timestamp + " detected a crash on "
                                + target.getAddress() + "@" + target.getTimestamp());
                        try {
                            LogGenerator.loggingCrashInfo(LogGenerator.LogType.CRASH, Main.hostName, Main.timestamp,
                                    target.getAddress(), target.getTimestamp());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                if (hasCrash) {
                    //send update message to 4 neighbors
                    Message message = Message.newBuilder().setCommand(Command.UPDATE).setHostName(Main.hostName)
                            .setPort(Main.port_membership).setTimestamp(Main.timestamp).addAllMembership(Main.membershipList).build();
                    Sender.send(message, true);
                }
            }
        }
    }

    private boolean heartbeat(List<DatagramPacket> datagramPacketList){
        for (DatagramPacket datagramPacket : datagramPacketList) {
            try {
                datagramSocket.send(datagramPacket);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        //check whether receive "ACK" from each neighbors
        boolean hasCrash = false;
        int cnt = 0;
        while (cnt < Main.timeBeforeCrash) {
            try {
                //wait for 1s
                Thread.sleep(1000);
                boolean allFine = true;
                for (int i = 0; i < isAck.length; i++) {
                    if(!isAck[i]){
                        allFine = false;
                        hasCrash = true;
                        break;
                    }
                }
                if(allFine){
                    hasCrash = false;
                    break;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            cnt += 1000;
        }
        return hasCrash;
    }
}
