package core;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

public class Query {
    public int number;
    public String job;
    public String executeAddress;
    Long start;
    public Long end = 0L;

    public Query(String job, int number, String executeAddress){
        this.job = job;
        this.number = number;
        this.executeAddress = executeAddress;
        this.start = System.currentTimeMillis();
    }

    public long getTimeConsumption(){
        return end - start;
    }

    public void switch2Address(String address){
        this.executeAddress = address;
    }

    public String getID(){
        return job + "-" + number;
    }

    public void setEndTime(Long end){
        this.end = end;
    }
}
