package utils;

import com.google.protobuf.Timestamp;
import core.Command;
import core.Message;
import core.Process;
import core.ProcessStatus;
import service.Main;
import service.Sender;

import java.io.StringReader;
import java.time.Instant;
import java.util.List;

/**
 * To update membership list
 * used by monitor and receiver
 */
public class MemberListUpdater {
    static List<Process> membershipList = Main.membershipList;
    public static void update(Message message){
        Command commmand = message.getCommand();
        if(commmand == Command.PING){
            return;
        }
        if(commmand == Command.ACK){
            // TODO: 2022/9/23
        }else if(commmand == Command.JOIN){
            Process process = Process.newBuilder()
                                    .setAddress(message.getHostName())
                                    .setTimestamp(message.getTimestamp())
                                    .setPort(ProcessStatus.ALIVE_VALUE).build();
            insert(process, membershipList);
            System.out.println(message.getHostName() + " joins the membershipList");
        } else if (commmand == Command.LEAVE) {
            if(!remove(message, membershipList)){
                System.out.println(message.getHostName() + "not exists");
            }
        } else if (commmand == Command.UPDATE){
            updateMemberList(message, Main.membershipList);
        }
    }
    synchronized public static void insert(Process process, List<Process> processList){
        int index = 0;
        for(Process p : processList){
            if(p.getAddress().compareTo(process.getAddress()) > 0){
                processList.add(index, process);
                return;
            }
            index ++;
        }
        processList.add(process);
    }

    synchronized public static boolean remove(Message message, List<Process> processList){
        String hostname = message.getHostName();
        String timestamp = message.getTimestamp();
        for(int i = 0; i < processList.size(); i++){
            Process process = processList.get(i);
            if(process.getAddress().equals(hostname)){
                processList.set(i, process.toBuilder().setStatus(ProcessStatus.LEAVED)
                        .setTimestamp(timestamp).build());
                return true;
            }
        }
        return false;
    }

    synchronized public static void removeById(int id, List<Process> processList){
        //if current process do
        if(id < 0 || id > processList.size() - 1){
            return;
        }
        processList.set(id, processList.get(id).toBuilder().setStatus(ProcessStatus.CRASHED)
                .setTimestamp(String.valueOf(Instant.now().getEpochSecond())).build());
    }

    synchronized public static void updateMemberList(Message message, List<Process> curMembershipList){
        List<Process> newMembershipList = message.getMembershipListList();
        //curMembershipList : membershipList on current node
        //newMembershipList : membershipList on input message
        int curLength = curMembershipList.size();
        int newLength = newMembershipList.size();
        int curIndex = 0, newIndex = 0;
        while(curIndex < curLength && newIndex < newLength) {
            Process curProcess = curMembershipList.get(curIndex);
            Process newProcess = newMembershipList.get(newIndex);
            String curAddress = curProcess.getAddress();
            String newAddress = newProcess.getAddress();
            if(curAddress.compareToIgnoreCase(newAddress) > 0){
                curMembershipList.add(curIndex, newProcess);
                System.out.println(newProcess.getAddress() + " is added into "
                                + Main.hostName + "'s membershipList");
                newIndex ++;
            }else if(curAddress.compareToIgnoreCase(newAddress) < 0){
                curIndex ++;
            }else {
                // if hostname equal, compare timestamp, update if needed
                String curTimeStamp = curProcess.getTimestamp();
                String newTimeStamp = newProcess.getTimestamp();
                if(curTimeStamp.compareToIgnoreCase(newTimeStamp) < 0){
                    curMembershipList.set(curIndex, curProcess.toBuilder()
                                     .setStatus(newProcess.getStatus())
                                     .setTimestamp(newTimeStamp).build());
                    System.out.println(curProcess.getAddress() + "'s timestamp is updated in "
                    + Main.hostName + "'s membershipList");
                }
                newIndex ++;
                curIndex ++;
            }
        }
        //add remaining processes in newMembershipList into curMembershipList
        while(newIndex < newLength){
            curMembershipList.add(newMembershipList.get(newIndex));
            System.out.println(newMembershipList.get(newIndex).getAddress() + " is added into "
                    + Main.hostName + "'s membershipList");
            newIndex ++;
        }
    }
}
