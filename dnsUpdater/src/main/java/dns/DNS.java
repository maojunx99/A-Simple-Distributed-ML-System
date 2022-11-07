package dns;

import core.Message;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class DNS {
    private static final String hostName;
    private static String introducer = "fa22-cs425-3801.cs.illinois.edu";

    static {
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private static final int dns_prt = 8088;


    public static void main(String[] args) throws IOException {
        DatagramSocket datagramSocket = new DatagramSocket(dns_prt, InetAddress.getByName(hostName));
        byte[] data = new byte[1024];
        DatagramPacket datagramPacket = new DatagramPacket(data, data.length);

        while (true) {
            System.out.println("[INFO] Waiting for updating introducer");
            datagramSocket.receive(datagramPacket);
            byte[] info = new byte[datagramPacket.getLength()];
            System.arraycopy(data, datagramPacket.getOffset(), info, 0, datagramPacket.getLength());
            Message message = Message.parseFrom(info);
            introducer = message.getMeta();
            System.out.println("[INFO] Introducer is updated as " + introducer);
        }
    }

    public static String getIntroducer() {
        return introducer;
    }
}
