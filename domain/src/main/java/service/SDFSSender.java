package service;

import com.google.protobuf.ByteString;
import core.Command;
import core.FileOuterClass;
import core.Message;
import utils.LogGenerator;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;

public class SDFSSender extends Thread {
    private final String hostName;
    private final int port;
    private Message message = null;
    private String localFileName = null;
    private String sdfsFileName = null;

    public SDFSSender(String hostName, int port, Message message) {
        this.hostName = hostName;
        this.port = port;
        this.message = message;
    }

    public SDFSSender(String hostName, int port, String localFileName, String sdfsFileName) {
        this.hostName = hostName;
        this.port = port;
        this.localFileName = localFileName;
        this.sdfsFileName = sdfsFileName;
    }

    @Override
    public void run() {
        Socket socket;
        try {
            socket = new Socket(this.hostName, this.port);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        OutputStream toServer;
        try {
            toServer = socket.getOutputStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        DataOutputStream outputStream = new DataOutputStream(toServer);
        if (this.message != null) {
            try {
                LogGenerator.loggingInfo(LogGenerator.LogType.SEND, "\n" + message);
                byte[] temp = this.message.toByteArray();
                outputStream.write(temp);
                try {
                    socket.shutdownOutput();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        // read local file into message and flush to TCP socket
        byte[] contents;
        try {
            contents = Files.readAllBytes(Paths.get(Main.localDirectory + localFileName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Message message1 = Message.newBuilder()
                .setHostName(Main.hostName)
                .setPort(Main.port_sdfs)
                .setCommand(Command.UPLOAD)
                .setFile(FileOuterClass.File.newBuilder()
                        .setFileName(sdfsFileName)
                        .setContent(ByteString.copyFrom(contents))
                        .build()
                )
                .build();
        try {
            LogGenerator.loggingInfo(LogGenerator.LogType.SEND, "\n" + message1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            outputStream.write(message1.toByteArray());
            try {
                socket.shutdownOutput();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // refer to https://srikarthiks.files.wordpress.com/2019/07/file-transfer-using-tcp.pdf
    }
}
