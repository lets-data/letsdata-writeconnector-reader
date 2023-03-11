package com.letsdata.reader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class GZipUtil {
    public static byte[] decompressByteArr(byte[] dataArr) {
        ByteArrayInputStream byteArrayInputStream = null;
        GZIPInputStream gzipInputStream = null;
        byte[] dataArrUncompressed = new byte[1024];
        ByteArrayOutputStream byteArrayOutputStream = null;


        try {
            byteArrayOutputStream = new ByteArrayOutputStream();
            byteArrayInputStream = new ByteArrayInputStream(dataArr);
            gzipInputStream = new GZIPInputStream(byteArrayInputStream);
            int len;
            while ((len = gzipInputStream.read(dataArrUncompressed)) > 0) {
                byteArrayOutputStream.write(dataArrUncompressed, 0, len);
            }

            byteArrayOutputStream.flush();

            return byteArrayOutputStream.toByteArray();
        } catch (IOException ex) {
            throw new RuntimeException("IOException in decompressing the data record");
        } finally {
            if (byteArrayOutputStream != null) {
                try {
                    byteArrayOutputStream.close();
                } catch (Exception ex) {
                    System.out.println("Exception in byteArrayOutputStream.close "+ex);
                }
            }

            if (byteArrayInputStream != null) {
                try {
                    byteArrayInputStream.close();
                } catch (Exception ex) {
                    System.out.println("Exception in byteArrayInputStream.close "+ex);
                }
            }

            if (gzipInputStream != null) {
                try {
                    gzipInputStream.close();
                } catch (Exception ex) {
                    System.out.println("Exception in closing gzipInputStream.close" + ex);
                }
            }
        }
    }
}
