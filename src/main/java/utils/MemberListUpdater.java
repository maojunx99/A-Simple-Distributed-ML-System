package utils;

import com.google.protobuf.Timestamp;
import core.Command;
import core.Message;
import core.Process;
import core.ProcessStatus;
import service.Main;
import service.Sender;

import java.util.List;

/**
 * To update membership list
 * used by monitor and receiver
 */
public class MemberListUpdater {
    static List<Process> membershipList = Main.membershipList;
    public static void update(Message message){
        Command commmand = message.getCommand();
        if(commmand == Command.PING || commmand == Command.ACK){
            return;
        }
        //
        Message newMessage = Message.newBuilder()
        //
        if(commmand == Command.JOIN){
            Process process = Process.newBuilder().setAddress(message.getHostName()).setPort(message.get).setStatus(ProcessStatus.ALIVE)
                    .setTimestamp(Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano())).build();
            insert(process, membershipList);

        } else if (commmand == Command.LEAVE) {
            if(!remove(message.getHostName(), membershipList)){
                System.out.println(message.getHostName() + "not exists");
            }
        } else if (commmand == Command.UPDATE){
//            membershipList = message.getMembershipListList();
        }
        newMessage = generateMessage(message);
        Sender.send(newMessage);
    }
    synchronized public static void insert(Process process, List<Process> processList){
        int len = processList.size();
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

    synchronized public static boolean remove(String hostname, List<Process> processList){
        for(Process process : processList){
            if(process.getAddress().equals(hostname)){
                processList.remove(process);
                return true;
            }
        }
        return false;
    }

    synchronized public static void removeById(int id, List<Process> processList){
        processList.remove(id);
    }

    synchronized public static generateMessage(Message message){
        //command:"update" timestamp:new pro hostname:new
        Message newMessage = Message.newBuilder().setCommandValue(4). ;
        return newMessage;
    }
}
