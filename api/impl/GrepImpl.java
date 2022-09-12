package api.impl;

import api.Grep;

import java.io.*;

public class GrepImpl implements Grep {
    private static Process process = null;
    StringBuilder stringBuilder = new StringBuilder();
    @Override
    public String grep(String[] args) {
        try{
            process = Runtime.getRuntime().exec(args);
            BufferedInputStream in = new BufferedInputStream(process.getInputStream());
            BufferedReader rd = new BufferedReader(new InputStreamReader(in));
            String str = null;
            while((str = rd.readLine()) != null){
                stringBuilder.append(str);
                stringBuilder.append("\n");
            }
            process.waitFor();
        }catch (Exception e){
            e.printStackTrace();
        }
        return stringBuilder.toString();
    }
}
