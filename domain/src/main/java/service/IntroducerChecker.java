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
                    if (process.getAddress().equals(Main.introducer)) {
                        if (process.getStatus() != ProcessStatus.ALIVE) {
                            this.updateIntroducer();
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
            Sender.broadcastNewIntroducer(Main.hostName);
        }
    }
}
