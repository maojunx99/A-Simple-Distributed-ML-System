package grep.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Server extends Thread{
    private static ServerSocket serverSocket =null;
    private static int port = 8866;
    private static ThreadPoolExecutor threadPoolExecutor = null;
    private static final int corePoolSize = 10;
    private static int maximumPoolSize = Integer.MAX_VALUE/2;

    public Server() throws IOException {
        serverSocket = new ServerSocket(port);
        threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 0L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    }

    @Override
    public void run() {
        while(true){
            Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            threadPoolExecutor.execute(new Processer(socket));
        }
    }
}
