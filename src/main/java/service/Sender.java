package service;

import core.Message;

/**
 * Send messages to other processes
 * join - join the network
 * leave - leave the network
 * ack - response to ping
 */
public class Sender {

    static public void send(Message message){
        //TODO
        System.out.println("MESSAGE\n" + message + "has been sent to neighbors");
    }
    static public void send(String hostName, int port, Message message){

    }
}
