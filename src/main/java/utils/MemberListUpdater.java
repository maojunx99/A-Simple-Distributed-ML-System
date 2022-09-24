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
    public static boolean update(Message message){
        Command commmand = message.getCommand();
        if(commmand == Command.ACK){
            // TODO: 2022/9/23
            for(int i = 0;i < Main.membershipList.size();i++){
                // check whether process has already existed
                Process p = Main.membershipList.get(i);
                if(p.getAddress().equals(message.getHostName())){
                    Main.membershipList.set(i, p.toBuilder().setTimestamp(message.getTimestamp()).build());
                    break;
                }
            }
            return true;
        }else if(commmand == Command.JOIN){
            Process process = Process.newBuilder()
                                    .setAddress(message.getHostName())
                                    .setTimestamp(message.getTimestamp())
                                    .setStatus(ProcessStatus.ALIVE)
                                    .setPort(message.getPort()).build();
            insert(process, Main.membershipList);
            System.out.println(message.getHostName() + " joins the membershipList");
            return true;
        } else if (commmand == Command.LEAVE) {
            boolean isModified = remove(message, Main.membershipList);
            if(!isModified){
                System.out.println(message.getHostName() + "not exists");
            }
            return isModified;
        } else if (commmand == Command.UPDATE){
            return updateMemberList(message, Main.membershipList);
        }
        return false;
    }
    synchronized public static void insert(Process process, List<Process> processList){
        int index = 0;
        for(Process p : processList){
            // check whether process has already existed
            if(p.getAddress().equals(process.getAddress())){
                processList.set(index, process.toBuilder().setTimestamp(process.getTimestamp()).setStatus(process.getStatus()).build());
                return;
            }
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

//    synchronized public static void removeById(String hostname, List<Process> processList){
//        //if current process do
//        for(Process process : processList){
//            if
//        }
//        processList.set(id, processList.get(id).toBuilder().setStatus(ProcessStatus.CRASHED)
//                .setTimestamp(String.valueOf(Instant.now().getEpochSecond())).build());
//    }

    synchronized public static boolean updateMemberList(Message message, List<Process> curMembershipList){
        List<Process> newMembershipList = message.getMembershipList();
        //curMembershipList : membershipList on current node
        //newMembershipList : membershipList on input message
        int newLength = newMembershipList.size();
        int curIndex = 0, newIndex = 0;
        boolean isModified = false;
        while(curIndex < curMembershipList.size() && newIndex < newLength) {
            Process curProcess = curMembershipList.get(curIndex);
            Process newProcess = newMembershipList.get(newIndex);
            String curAddress = curProcess.getAddress();
            String newAddress = newProcess.getAddress();
            if(curAddress.compareToIgnoreCase(newAddress) > 0){
                curMembershipList.add(curIndex, newProcess);
                isModified = true;
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
                    isModified = true;
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
            isModified = true;
        }
        return isModified;
    }
}
