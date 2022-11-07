package service;

import core.Process;
import core.ProcessStatus;

public class IntroducerChecker extends Thread {

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
