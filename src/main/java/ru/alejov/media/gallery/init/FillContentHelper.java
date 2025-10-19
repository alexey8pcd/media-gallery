package ru.alejov.media.gallery.init;

import ru.alejov.media.gallery.DateUtils;
import ru.alejov.media.gallery.HashUtils;
import ru.alejov.media.gallery.Media;
import ru.alejov.media.gallery.MetadataUtils;
import ru.alejov.media.gallery.PgHelper;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Stream;

public class FillContentHelper {


    public static void main(String[] args) throws IOException, SQLException {
        String rootDirectory = args[0];
        String jdbcPropertiesFile = args[1];
        Properties supportedExtensions = new Properties();
        try (InputStream resourceAsStream = FillContentHelper.class.getClassLoader().getResourceAsStream("supported_extensions.properties")) {
            supportedExtensions.load(resourceAsStream);
        }
        Set<String> unsupportedExtensions = new LinkedHashSet<>();
        List<Media> mediaList = new ArrayList<>();
        String systemName = InetAddress.getLocalHost().getHostName();
        try (Stream<Path> stream = Files.walk(Path.of(rootDirectory))) {
            stream.forEach((Path path) -> {
                if (!Files.isDirectory(path)) {
                    String fileName = path.getFileName().toString();
                    String extension = getExtension(fileName);
                    String type = supportedExtensions.getProperty(extension);
                    if (type == null) {
                        unsupportedExtensions.add(extension);
                    } else {
                        Media media = getMedia(path, fileName, type, systemName);
                        mediaList.add(media);
                    }
                }
            });
        }
        PrintStream printStream = new PrintStream(System.out, true, StandardCharsets.UTF_8);
        mediaList.forEach(printStream::println);

        PgHelper.saveToDatabase(jdbcPropertiesFile, mediaList);
    }

    private static Media getMedia(Path path, String fileName, String type, String systemName) {
        Media media;
        try {
            Map<String, String> metadata = MetadataUtils.getMetadata(path, type);

            Timestamp createDate = DateUtils.getCreateDate(metadata, fileName);
            if (createDate == null) {
                BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
                FileTime creationTime = attributes.creationTime();
                createDate = Timestamp.from(creationTime.toInstant());
            }
            String md5Hash = HashUtils.getMd5Hash(path);
            media = new Media(fileName, createDate, systemName + File.separator + path.toString(), md5Hash, path.toFile().length(), type, metadata);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return media;
    }


    public static String getExtension(String fileName) {
        if (!fileName.contains(".")) {
            return "";
        }
        String[] parts = fileName.split("\\.");
        return parts[parts.length - 1].toLowerCase(Locale.ENGLISH);
    }

}
