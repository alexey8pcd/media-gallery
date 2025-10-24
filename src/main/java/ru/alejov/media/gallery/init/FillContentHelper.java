package ru.alejov.media.gallery.init;

import ru.alejov.media.gallery.DateUtils;
import ru.alejov.media.gallery.HashUtils;
import ru.alejov.media.gallery.Media;
import ru.alejov.media.gallery.MetaTag;
import ru.alejov.media.gallery.MetadataUtils;
import ru.alejov.media.gallery.PgHelper;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

public class FillContentHelper {


    private static final String PRIMARY_FILL = "--primary-fill";
    private static final String ROOT_DIR = "rootDir";
    private static final String PG_SETTINGS_PATH = "pgSettingsPath";
    private static final String PARALLEL = "parallel";
    private static final String CALCULATE_MD5 = "calculateMD5";

    private static final PrintStream outStream;
    private static final PrintStream errorStream;

    static {
        try {
            outStream = new PrintStream(System.out, true, StandardCharsets.UTF_8.name());
            errorStream = new PrintStream(System.err, true, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    //--primary-fill rootDir="rootDirectory" [pgSettingsPath="path to jdbc.properties"] [parallel=true] [calculateMD5=true]
    public static void main(String[] args) throws IOException, SQLException {
        Map<String, String> params = new HashMap<>();
        for (String arg : args) {
            String[] strings = arg.split("=");
            params.put(strings[0], strings.length > 1 ? strings[1] : "");
        }
        if (params.containsKey(PRIMARY_FILL)) {
            String rootDir = params.get(ROOT_DIR);
            String pgSettingsPath = params.get(PG_SETTINGS_PATH);
            boolean parallel = Boolean.parseBoolean(params.getOrDefault(PARALLEL, "false"));
            boolean calculateMd5 = Boolean.parseBoolean(params.getOrDefault(CALCULATE_MD5, "false"));
            primaryFill(rootDir, pgSettingsPath, parallel, calculateMd5);
        } else {
            outStream.printf("Only '%s' is supported now", PRIMARY_FILL);
        }
    }

    private static void primaryFill(String rootDirectory, String jdbcPropertiesFile, boolean parallel, boolean calculateMd5) throws IOException, SQLException {
        outStream.println("Start primaryFill(parallel=" + parallel + ")");
        Properties supportedExtensions = new Properties();
        try (InputStream resourceAsStream = FillContentHelper.class.getClassLoader().getResourceAsStream("supported_extensions.properties")) {
            supportedExtensions.load(resourceAsStream);
        }
        Set<String> unsupportedExtensions = new LinkedHashSet<>();
        List<Media> mediaList = new ArrayList<>();
        String systemName = InetAddress.getLocalHost().getHostName();
        try (Stream<Path> stream = Files.walk(Paths.get(rootDirectory))) {
            if (parallel) {
                stream.parallel().filter(path -> !Files.isDirectory(path))
                      .forEach(path -> {
                          processMedia(path, supportedExtensions, unsupportedExtensions, systemName, mediaList);
                      });
            } else {
                stream.filter(path -> !Files.isDirectory(path))
                      .forEach(path -> {
                          processMedia(path, supportedExtensions, unsupportedExtensions, systemName, mediaList);
                      });
            }

        }
        if (!unsupportedExtensions.isEmpty()) {
            outStream.println("Unsupported extensions: " + unsupportedExtensions);
        }
        outStream.printf("Find %d files\n", mediaList.size());
        if (calculateMd5) {
            outStream.printf("Calculate MD5 for %d files\n", mediaList.size());
            Instant begin = Instant.now();
            if (parallel) {
                mediaList.parallelStream()
                         .forEach(Media::calculateMd5);
            }
            Duration duration = Duration.between(begin, Instant.now());
            outStream.println("MD5 calculated at " + duration.toString().replace("PT", ""));
        }
        //mediaList.forEach(outStream::println);
        if (jdbcPropertiesFile != null) {
            PgHelper.saveToDatabase(jdbcPropertiesFile, mediaList);
        }
        outStream.println("Finish primaryFill");
    }

    private static void processMedia(Path path, Properties supportedExtensions, Set<String> unsupportedExtensions, String systemName, List<Media> mediaList) {
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

    private static Media getMedia(Path path, String fileName, String type, String systemName) {
        Media media;
        try {
            Map<MetaTag, String> metadata = MetadataUtils.getMetadata(path, type);

            Timestamp createDate = DateUtils.getCreateDate(metadata, fileName);
            if (createDate == null) {
                BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
                FileTime creationTime = attributes.creationTime();
                createDate = Timestamp.from(creationTime.toInstant());
            }
            String md5Hash = null;
            media = new Media(fileName, createDate, Collections.singletonMap(systemName, path.toString()), md5Hash, path.toFile().length(), type, metadata, path);
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
