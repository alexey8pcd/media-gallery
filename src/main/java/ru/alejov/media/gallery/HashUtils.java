package ru.alejov.media.gallery;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashUtils {

    private static final MessageDigest MD5;

    static {
        try {
            MD5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getMd5Hash(Path path) {
        return null;
//        try {
//            byte[] digest = MD5.digest(Files.readAllBytes(path));
//            return Utils.toHexString(digest);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }
}
