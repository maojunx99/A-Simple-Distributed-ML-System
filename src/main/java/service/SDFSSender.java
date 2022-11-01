package service;

import com.google.protobuf.ByteString;
import core.Command;
import core.Message;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;

import core.FileOuterClass;

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
                outputStream.writeUTF(String.valueOf(this.message));
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
        try {
            outputStream.writeUTF(String.valueOf(
                    Message.newBuilder()
                            .setHostName(Main.hostName)
                            .setPort(Main.port_sdfs)
                            .setCommand(Command.UPLOAD)
                            .setFile(FileOuterClass.File.newBuilder()
                                    .setFileName(sdfsFileName)
                                    .setContent(ByteString.copyFrom(contents))
                                    .build()
                            )
                            .build()
            ));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            socket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // refer to https://srikarthiks.files.wordpress.com/2019/07/file-transfer-using-tcp.pdf
    }
}
