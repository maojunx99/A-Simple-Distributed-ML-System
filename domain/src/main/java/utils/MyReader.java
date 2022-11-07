package utils;

import java.io.IOException;
import java.io.InputStream;

public class MyReader {
    public static byte[] read(InputStream inputStream) throws IOException {
//        byte[] bytes = new byte[0];
//        byte[] buff = new byte[1024];
//        int k = -1;
//        while ((k = inputStream.read(buff, 0, buff.length)) > -1) {
//            byte[] temp = new byte[bytes.length + k];
//            System.arraycopy(bytes, 0, temp, 0, bytes.length);
//            System.arraycopy(buff, 0, temp, bytes.length, k);  // copy current lot
//            bytes = temp; // call the temp buffer as your result buff
//        }
//        return bytes;
        return inputStream.readAllBytes();
    }
}
