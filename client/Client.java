package client;

import java.io.*;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Client {
    private static final String PROPERTIES_ADDRESS = "/home/yiteng3/cs425-mp/client/serverAddress.properties";
    private static final String SERVER_AMOUNT = "amount";
    private static final String SERVER_STATE = "state";
    private static final String SERVER_ADDRESS = "server";
    private static final String PORT = "port";
    public static final String GREP_C = "grep -c";


    public static void main(String[] args) {
        Scanner s = new Scanner(System.in);
        String line;
        String command;
        String query;
        System.out.println("Please input a grep command:");
        while (!(line = s.nextLine()).equals("EOF")) {
            // parse line
            String[] words = line.split(" ");
            StringBuilder builder = new StringBuilder(words[0]);
            if (words[1].equals("-c")) {
                builder.append(" -c");
            }
            command = builder.toString();
            query = line.replace(command + " ", "");
            long start = System.currentTimeMillis();
            try {
                System.out.println(callServers(command, query));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
            long end = System.currentTimeMillis();
            System.out.println("Total executing time: " + (end - start) + "ms");
            System.out.println("Please input a grep command:");
        }
    }

    /**
     * @param command command
     * @param query   query body
     * @return string - the response message from servers
     * @throws IOException - no such properties file
     */
    public static String callServers(String command, String query) throws IOException, InterruptedException {
        System.out.println("start executing command: " + command + " " + query);
        // create thread pool
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 15, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        // initialize result container
        Result result = new Result(command.equals(GREP_C) ? ResultType.Integer : ResultType.String);

        // get all server amount from properties file
        Properties properties = new Properties();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(PROPERTIES_ADDRESS));
        properties.load(bufferedReader);
        int amount = Integer.parseInt(properties.getProperty(SERVER_AMOUNT));

        // iterate all server and fetch log from alive server, failed servers will be ignored
        for (int i = 0; i < amount; i++) {
            String state = properties.getProperty(SERVER_STATE + i);
            String server_address = properties.getProperty(SERVER_ADDRESS + i);
            int port = Integer.parseInt(properties.getProperty(PORT + i));
            MySocket mySocket = new MySocket(server_address, port, command, query, ServerState.valueOf(state), result);
            executor.execute(mySocket);
        }
        // wait until thread pool is empty
        while (executor.getActiveCount() != 0) {
        }
        executor.shutdown();
        return result.getResult();
    }
}
