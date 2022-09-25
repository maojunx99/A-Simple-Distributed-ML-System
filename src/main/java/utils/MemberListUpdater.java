package utils;

import core.Command;
import core.Message;
import core.Process;
import core.ProcessStatus;
import service.Main;

import java.io.IOException;
import java.util.List;

/**
 * To update membership list
 * used by monitor and receiver
 */
public class MemberListUpdater {
    public static boolean update(Message message){
        Command command = message.getCommand();
        if(command == Command.ACK){
            for(int i = 0;i < Main.membershipList.size();i++){
                // check whether process has already existed
                Process p = Main.membershipList.get(i);
                if(p.getAddress().equals(message.getHostName())){
                    try {
                        LogGenerator.timestampLogging(p.getAddress(), p.getTimestamp(), message.getTimestamp());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    Main.membershipList.set(i, p.toBuilder().setTimestamp(message.getTimestamp()).build());
                    break;
                }
            }
            return true;
        }else if(command == Command.JOIN){
            Process process = Process.newBuilder()
                                    .setAddress(message.getHostName())
                                    .setTimestamp(message.getTimestamp())
                                    .setStatus(ProcessStatus.ALIVE)
                                    .setPort(message.getPort()).build();
            insert(process, Main.membershipList);
            System.out.println("[INFO] " + message.getHostName() + "@" + message.getTimestamp() + " joins the membershipList");
            try {
                LogGenerator.logging(LogGenerator.LogType.JOIN, message.getHostName(), message.getTimestamp(), ProcessStatus.ALIVE);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return false;
        } else if (command == Command.LEAVE) {
            boolean isModified = remove(message, Main.membershipList);
            if(!isModified){
                System.out.println(message.getHostName() + "not exists");
            }
            return isModified;
        } else if (command == Command.UPDATE){
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
                try {
                    LogGenerator.logging(LogGenerator.LogType.LEAVE, hostname, timestamp, ProcessStatus.LEAVED);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
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
                System.out.println("[INFO] " + newProcess.getAddress() + "@" + newProcess.getTimestamp() + " is added into "
                                + Main.hostName + "'s membershipList");
                try {
                    LogGenerator.logging(LogGenerator.LogType.JOIN, newProcess.getAddress(), newProcess.getTimestamp(), ProcessStatus.ALIVE);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                newIndex ++;
            }else if(curAddress.compareToIgnoreCase(newAddress) < 0){
                curIndex ++;
            }else {
                // if hostname equal, compare timestamp, update if needed
                String curTimeStamp = curProcess.getTimestamp();
                String newTimeStamp = newProcess.getTimestamp();
                if(curTimeStamp.compareToIgnoreCase(newTimeStamp) < 0){
                    Process.Builder temp = curProcess.toBuilder().setTimestamp(newTimeStamp);
                    if(newProcess.getStatus()!=curProcess.getStatus()){
                        temp.setStatus(newProcess.getStatus());
                        if(newProcess.getStatus() == ProcessStatus.CRASHED){
                            try {
                                LogGenerator.logging(LogGenerator.LogType.CRASH,
                                        message.getHostName(), message.getTimestamp(),
                                        temp.getAddress(), temp.getTimestamp()
                                );
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        try {
                            LogGenerator.logging(LogGenerator.LogType.UPDATE, temp.getAddress(),
                                    temp.getTimestamp(), temp.getStatus());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        isModified = true;
                    }
                    curMembershipList.set(curIndex, temp.build());
                    System.out.println("[INFO] " + curProcess.getAddress() + "'s timestamp is updated in "
                    + Main.hostName + "'s membershipList");
                    try {
                        LogGenerator.timestampLogging(curProcess.getAddress(), curTimeStamp, newTimeStamp);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                newIndex ++;
                curIndex ++;
            }
        }
        //add remaining processes in newMembershipList into curMembershipList
        while(newIndex < newLength){
            Process process = newMembershipList.get(newIndex);
            curMembershipList.add(process);
            System.out.println("[INFO] " + process.getAddress() + "@" + process.getTimestamp() + " is added into "
                    + Main.hostName + "'s membershipList");
            try {
                LogGenerator.logging(LogGenerator.LogType.UPDATE, process.getAddress(), process.getTimestamp(), ProcessStatus.ALIVE);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            newIndex ++;
            isModified = true;
        }
        return isModified;
    }
}
