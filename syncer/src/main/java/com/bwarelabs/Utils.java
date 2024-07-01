package com.bwarelabs;

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utils {
    public static String calculateSHA256Checksum(Result result) throws NoSuchAlgorithmException, IOException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        CellScanner scanner = result.cellScanner();

        while (scanner.advance()) {
            byte[] value = scanner.current().getValueArray();
            int valueOffset = scanner.current().getValueOffset();
            int valueLength = scanner.current().getValueLength();
            digest.update(value, valueOffset, valueLength);
        }

        byte[] hashBytes = digest.digest();
        StringBuilder hexString = new StringBuilder();
        for (byte hashByte: hashBytes) {
            String hex = Integer.toHexString(0xff & hashByte);
            if (hex.length() == 1)
                hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }
}
