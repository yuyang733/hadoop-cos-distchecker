package com.qcloud.cos.hadoop.distchecker.checksum.utils;

import java.io.InputStream;
import java.util.zip.CheckedInputStream;

public final class IOUtils {
    public static Long getCRCValue(InputStream inputStream) {
        if (inputStream instanceof CheckedInputStream) {
            return ((CheckedInputStream) inputStream).getChecksum().getValue();
        }
        return null;
    }

    public static byte[] long2byte(long res) {
        byte[] buffer = new byte[8];
        for (int i = 0; i < 8; i++) {
            int offset = 64 - (i + 1) * 8;
            buffer[i] = (byte) ((res >> offset) & 0xff);
        }
        return buffer;
    }

    public static boolean checkChecksum(Long clientChecksum, Long serverChecksum) {
        if (clientChecksum != null && serverChecksum != null && !clientChecksum.equals(serverChecksum)) {
            return false;
        }

        return true;
    }
}
