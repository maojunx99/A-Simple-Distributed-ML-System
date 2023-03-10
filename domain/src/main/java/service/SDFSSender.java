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
                LogGenerator.loggingInfo(LogGenerator.LogType.SEND,  message.getHostName() +
                        " sends " + message.getCommand() + " " + message.getFile().getFileName() + " to " + hostName);
                this.message.writeTo(outputStream);
                try {
                    socket.shutdownOutput();
                    socket.close();
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
            try {
                LogGenerator.loggingInfo(LogGenerator.LogType.ERROR, "wrong file name, please try again");
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            try {
                socket.shutdownOutput();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            return;
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
            LogGenerator.loggingInfo(LogGenerator.LogType.SEND,  message1.getHostName() +
                    " sends " + message1.getFile().getFileName() + " to " + hostName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            message1.writeTo(outputStream);
            try {
                socket.shutdownOutput();
                socket.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
