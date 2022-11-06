package grep.test;
import grep.client.*;

import java.io.*;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class GrepTests {
    public static final String ACTIVE_SERVER_ADDRESS_PROPERTIES = "/home/yiteng3/cs425-mp/grep.test/activeServerAddress.properties";
    public static final String ABNORMAL_SERVER_ADDRESS_PROPERTIES = "/home/yiteng3/cs425-mp/grep.test/partiallyFailedServerAddress.properties";
    public static final String LOG_ADDRESS = "logging.log";
    private static final String SERVER_AMOUNT = "amount";
    public static final String SERVER = "server";
    public static final String PORT = "port";
    public static final String TEST_COMMAND = "test";
    public static final String VM_HOME_ADDRESS = "/home/maojunx2/";
    public static final String VM_LOG_ADDRESS = "mp1/grep.test.log";
    public static final String GREP = "grep";
    public static final String GREP_C = "grep -c";

    public static void main(String[] args) throws IOException, InterruptedException {
        Scanner s = new Scanner(System.in);
        String test;
        System.out.println("Please input a grep.test name! \n" +
                "You can choose from(\n" +
                "[normal grep.test]        ->   all sites are active\n" +
                "[normal grep.test -c]     ->   all sites are active and grep with -c\n" +
                "[abnormal grep.test]      ->   some of sites are failed\n" +
                "[abnormal grep.test -c]   ->   some of sites are failed and grep with -c\n" +
                "[all tests]          ->   run all tests\n" +
                "[EOF]                ->   end this process):");
        while (!Objects.equals(test = s.nextLine(), "EOF")) {
            switch (test) {
                case "normal grep.test":
                    normalGrepTest(GREP);
                    break;
                case "normal grep.test -c":
                    normalGrepTest(GREP_C);
                    break;
                case "abnormal grep.test":
                    abnormalGrepTest(GREP);
                    break;
                case "abnormal grep.test -c":
                    abnormalGrepTest(GREP_C);
                    break;
                case "all tests":
                    normalGrepTest(GREP);
                    normalGrepTest(GREP_C);
                    abnormalGrepTest(GREP);
                    abnormalGrepTest(GREP_C);
                    break;
                default:
                    System.out.println("please check the grep.test name and re-input, choose from(normal grep.test, abnormal grep.test, all tests):");
            }
            System.out.println("\nPlease input a grep.test name, choose from(normal grep.test, abnormal grep.test, all tests, EOF):");
        }
    }

    public static void normalGrepTest(String command) throws IOException, InterruptedException {
        System.out.println("\n[normal " + command + " grep.test]:");
        boolean rlt = grepTest(ACTIVE_SERVER_ADDRESS_PROPERTIES, command);
        if(rlt){
            System.out.println("normal " + command + " grep.test passed!");
        }else{
            System.out.println("normal " + command + " grep.test Failed!");
        }
    }

    public static void abnormalGrepTest(String command) throws IOException, InterruptedException {
        System.out.println("\n[abnormal " + command + " grep.test]:");

        boolean rlt = grepTest(ABNORMAL_SERVER_ADDRESS_PROPERTIES, command);
        if(rlt){
            System.out.println("abnormal " + command + " grep.test passed!");
        }else{
            System.out.println("abnormal " + command + " grep.test Failed!");
        }

    }

    public static boolean grepTest(String propertiesPath, String command) throws IOException, InterruptedException {
        // initialize thread pool and return value
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 15, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        boolean rlt = true;
        // initialize result
        Result result = new Result(ResultType.String);

        // define grep queries - grep.test cases
        String[] queries = new String[]{
                // rare or even not exist grep.test cases
                "187.185.87.78",
                "156.98.57.6",
                "29.157.145.157",
                // somewhat frequent grep.test cases
                "Intel Mac OS X 10_7_5",
                "\"GET\\|PUT\"",
                "POS*",
                // frequent grep.test cases
                ".",
                "/",
                "2022"
        };
        // get active servers' addresses and ports from properties file
        Properties properties = new Properties();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(propertiesPath));
        properties.load(bufferedReader);
        int amount = Integer.parseInt(properties.getProperty(SERVER_AMOUNT));
        MySocket[] sockets = new MySocket[amount];

        // create socket and connect to grep.server
        for (int i = 0; i < amount; i++) {
            String serverAddress = properties.getProperty(SERVER + i);
            int port = Integer.parseInt(properties.getProperty(PORT + i));
            // send grep.test message to inform grep.server generate log files
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
                // remove unusefal location infomation from each line
                resultArray[i] = resultArray[i].replace(VM_HOME_ADDRESS + VM_LOG_ADDRESS + ":", "");
            }
            Arrays.sort(resultArray);

            // calculate expect value by executing grep command locally
            StringBuilder expectValue = new StringBuilder();
            if(query.charAt(0) == '\"' && query.charAt(query.length()-1) == '\"'){
                query = query.substring(1, query.length()-1);
            }
            Process localGrep = Runtime.getRuntime().exec(command + " " + query + " " + LOG_ADDRESS);
            BufferedReader localGrepRlt = new BufferedReader(new InputStreamReader(localGrep.getInputStream()));
            String line;
            while ((line = localGrepRlt.readLine()) != null) {
                expectValue.append(line.replace(LOG_ADDRESS + ":", "")).append("\n");
            }
            if(expectValue.toString().matches("\\d+\\n")){
                System.out.print("expected value:" + expectValue);
            }
            if(command.equals(GREP_C) && expectValue.toString().equals("")){
                expectValue.append("0\n");
                System.out.println("expected value: 0");
            }
            String[] expectedArray = expectValue.toString().split("\n");
            Arrays.sort(expectedArray);

            // compare real value with expect value
            if (resultArray.length != expectedArray.length) {
                rlt = false;
                System.out.println(rlt?"PASS":"FAILED");
                System.out.println("-------------------------------------");
                continue;
            }

            // compare iteratly only when lengthes of 2 arrays are the same
            boolean current = true;
            for (int i = 0; i < resultArray.length; i++) {
                if (!resultArray[i].equals(expectedArray[i])) {
                    current = false;
                }
            }
            rlt&=current;
            System.out.println(current?"PASS":"FAILED");
            System.out.println("-------------------------------------");
        }
        executor.shutdown();
        return rlt;
    }
}
