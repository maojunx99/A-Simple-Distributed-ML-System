package test;
import client.*;

import java.io.*;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class GrepTests {
    public static final String ACTIVE_SERVER_ADDRESS_PROPERTIES = "/home/yiteng3/cs425-mp/test/activeServerAddress.properties";
    public static final String ABNORMAL_SERVER_ADDRESS_PROPERTIES = "/home/yiteng3/cs425-mp/test/partiallyFailedServerAddress.properties";
    public static final String LOG_ADDRESS = "logging.log";
    private static final String SERVER_AMOUNT = "amount";
    public static final String SERVER = "server";
    public static final String PORT = "port";
    public static final String TEST_COMMAND = "test";
    public static final String VM_HOME_ADDRESS = "/home/maojunx2/";
    public static final String VM_LOG_ADDRESS = "mp1/test.log";
    public static final String GREP = "grep";
    public static final String GREP_C = "grep -c";

    public static void main(String[] args) throws IOException, InterruptedException {
        Scanner s = new Scanner(System.in);
        String test;
        System.out.println("Please input a test name! \n" +
                "You can choose from(\n" +
                "[normal test]        ->   all sites are active\n" +
                "[normal test -c]     ->   all sites are active and grep with -c\n" +
                "[abnormal test]      ->   some of sites are failed\n" +
                "[abnormal test -c]   ->   some of sites are failed and grep with -c\n" +
                "[all tests]          ->   run all tests\n" +
                "[EOF]                ->   end this process):");
        while (!Objects.equals(test = s.nextLine(), "EOF")) {
            switch (test) {
                case "normal test":
                    normalGrepTest(GREP);
                    break;
                case "normal test -c":
                    normalGrepTest(GREP_C);
                    break;
                case "abnormal test":
                    abnormalGrepTest(GREP);
                    break;
                case "abnormal test -c":
                    abnormalGrepTest(GREP_C);
                    break;
                case "all tests":
                    normalGrepTest(GREP);
                    normalGrepTest(GREP_C);
                    abnormalGrepTest(GREP);
                    abnormalGrepTest(GREP_C);
                    break;
                default:
                    System.out.println("please check the test name and re-input, choose from(normal test, abnormal test, all tests):");
            }
            System.out.println("\nPlease input a test name, choose from(normal test, abnormal test, all tests, EOF):");
        }
    }

    public static void normalGrepTest(String command) throws IOException, InterruptedException {
        System.out.println("\n[normal " + command + " test]:");
        boolean rlt = grepTest(ACTIVE_SERVER_ADDRESS_PROPERTIES, command);
        if(rlt){
            System.out.println("normal " + command + " test passed!");
        }else{
            System.out.println("normal " + command + " test Failed!");
        }
    }

    public static void abnormalGrepTest(String command) throws IOException, InterruptedException {
        System.out.println("\n[abnormal " + command + " test]:");

        boolean rlt = grepTest(ABNORMAL_SERVER_ADDRESS_PROPERTIES, command);
        if(rlt){
            System.out.println("abnormal " + command + " test passed!");
        }else{
            System.out.println("abnormal " + command + " test Failed!");
        }

    }

    public static boolean grepTest(String propertiesPath, String command) throws IOException, InterruptedException {
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 15, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        // initialize result
        Result result = new Result(ResultType.String);
        // define grep queries
        String[] queries = new String[]{
                "Intel Mac OS X 10_7_5",
                "google.com"
        };
        // get active servers' addresses and ports from properties file
        Properties properties = new Properties();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(propertiesPath));
        properties.load(bufferedReader);
        int amount = Integer.parseInt(properties.getProperty(SERVER_AMOUNT));
        MySocket[] sockets = new MySocket[amount];

        // create socket and connect to server
        for (int i = 0; i < amount; i++) {
            String serverAddress = properties.getProperty(SERVER + i);
            int port = Integer.parseInt(properties.getProperty(PORT + i));
            // send test message to inform server generate log files
            sockets[i] = new MySocket(serverAddress, port, TEST_COMMAND, "", ServerState.ACTIVE, result);
            executor.execute(sockets[i]);
        }
        while (executor.getActiveCount() != 0) {
        }
        // fetch random log and save
        File logging = new File(LOG_ADDRESS);
        FileWriter fileWriter = new FileWriter(logging);
        fileWriter.write(result.getResult());
        fileWriter.close();

        for (String query : queries) {
            // invoke method that needs be tested and record result
            String resultString = Client.callServers(command, query + " ~/" + VM_LOG_ADDRESS);
            if(resultString.matches("\\d+")) {
                System.out.println("result value: " + resultString);
            }
            // put them in order
            String[] resultArray = resultString.split("\n");
            for (int i = 0; i < resultArray.length; i++) {
                resultArray[i] = resultArray[i].replace(VM_HOME_ADDRESS + VM_LOG_ADDRESS + ":", "");
            }
            Arrays.sort(resultArray);
            // calculate expect value
            StringBuilder expectValue = new StringBuilder();
            Process localGrep = Runtime.getRuntime().exec(command + " " + query + " " + LOG_ADDRESS);
            BufferedReader localGrepRlt = new BufferedReader(new InputStreamReader(localGrep.getInputStream()));
            String line;
            while ((line = localGrepRlt.readLine()) != null) {
                expectValue.append(line.replace(LOG_ADDRESS + ":", "")).append("\n");
            }
            if(expectValue.toString().matches("\\d+\\n")){
                System.out.print("expected value:" + expectValue);
            }
            String[] expectedArray = expectValue.toString().split("\n");
            Arrays.sort(expectedArray);
            // compare
            if (resultArray.length != expectedArray.length) {
                return false;
            }
            for (int i = 0; i < resultArray.length; i++) {
                if (!resultArray[i].equals(expectedArray[i])) {
                    return false;
                }
            }
        }
        executor.shutdown();
        return true;
    }
}
