package service;

import core.Process;
import core.ProcessStatus;
import dns.DNS;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class IntroducerChecker extends Thread {

    private final List<String> totalNetwork;

    public IntroducerChecker() throws IOException {
        Properties properties = new Properties();
        String networkPropertiesPath = "../network.properties";
        properties.load(this.getClass().getResourceAsStream(networkPropertiesPath));
        totalNetwork = Collections.singletonList(properties.getProperty("vm_address"));
    }

    @Override
    public void run() {
        while (true) {
            if (Main.isLeader) {
                for (Process process : Main.membershipList) {
                    System.out.println("[INFO] Checking " + process.getAddress());
                    if (process.getAddress().equals(Main.introducer)) {
                        System.out.println("[INFO] Introducer is " + process.getAddress());
                        if (process.getStatus() != ProcessStatus.ALIVE) {
                            System.out.println("[ERROR] Introducer is down!");
                            System.out.println("[INFO] Start changing the introducer...");
                            this.updateIntroducer();
                            System.out.println("[INFO] New Introducer in main is " + Main.introducer);
                            System.out.println("[INFO] Actual Introducer in dns is " + DNS.getIntroducer());
                        }
                        break;
                    }
                }
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void updateIntroducer() {
        boolean exist = false;
        for (Process process : Main.membershipList) {
            if (!process.getAddress().equals(Main.hostName)) {
                if (process.getStatus() == ProcessStatus.ALIVE) {
                    exist = true;
                    Sender.broadcastNewIntroducer(process.getAddress());
                    break;
                }
            }
        }
        if (!exist) {
            System.out.println("[INFO] No other process can become introducer, leader become the introducer.");
            Sender.broadcastNewIntroducer(Main.hostName);
        }
    }
}
