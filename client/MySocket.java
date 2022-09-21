package client;

import java.io.*;
import java.net.Socket;

public class MySocket extends Thread {
    private String server_address;
    private String command;
    private String query;
    private Socket socket;
    private String responseMsg;
    private ServerState serverState;
    private Result result;
    private static final int SLEEPING_INTERVAL = 5;
    public static final int MAX_SLEEPING_CYCLE = 10000;


    public MySocket(String server_address, int port, String command, String query, ServerState serverState, Result result) {
        this.server_address = server_address;
        this.command = command;
        this.query = query;
        this.serverState = serverState;
        this.result = result;
        try {
            this.socket = new Socket(server_address, port);
        } catch (IOException e) {
            this.serverState = ServerState.FAILED;
        }
    }

    @Override
    public void run() {
        if (socket == null) return;
        responseMsg = "";
        if (serverState == ServerState.FAILED) {
            // TODO: add some codes here to check whether the server is recovered
            return;
        }
        OutputStream toServer;
        try {
            toServer = socket.getOutputStream();
        } catch (IOException e) {
            this.serverState = ServerState.REFUSE_MESSAGE;
            return;
        }
        DataOutputStream outputStream = new DataOutputStream(toServer);
        try {
            outputStream.writeUTF(command + " " + query);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        InputStream fromServer;
        try {
            fromServer = socket.getInputStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        DataInputStream inputStream = new DataInputStream(fromServer);
        try {
            // in case server is blocked
            int cnt = 0;
            while (inputStream.available() == 0 && cnt < MAX_SLEEPING_CYCLE) {
                Thread.sleep(SLEEPING_INTERVAL);
                cnt++;
            }
            if (cnt >= MAX_SLEEPING_CYCLE) {
                return;
            }
            while (inputStream.available() > 0) {
                responseMsg = inputStream.readUTF();
                System.out.println(server_address+": "+responseMsg);
                result.add(responseMsg);
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public String getResponseMsg() {
        return responseMsg;
    }

    public String getServerState() {
        return serverState.toString();
    }

    public void setCommandAndQuery(String command, String query) {
        this.command = command;
        this.query = query;
    }

    public void setResult(Result result) {
        this.result = result;
    }

    public void closeSocket() throws IOException {
        this.socket.close();
    }
}
