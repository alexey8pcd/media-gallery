package ru.alejov.media.gallery;

import org.postgresql.core.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashUtils {

    private static final ThreadLocal<MessageDigest> MD5 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    });

    public static String getMd5Hash(Path path) {
        try {
            byte[] digest = MD5.get().digest(Files.readAllBytes(path));
            return Utils.toHexString(digest);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
