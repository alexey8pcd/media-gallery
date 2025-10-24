package ru.alejov.media.gallery.init;

import ru.alejov.media.gallery.DateUtils;
import ru.alejov.media.gallery.Media;
import ru.alejov.media.gallery.MetaTag;
import ru.alejov.media.gallery.MetadataUtils;
import ru.alejov.media.gallery.PgHelper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FillContentHelper {


    private static final String PRIMARY_FILL = "--primary-fill";
    private static final String INCREMENTAL_FILL = "--incremental-fill";

    private static final String ROOT_DIR = "rootDir";
    private static final String PG_SETTINGS_PATH = "pgSettingsPath";
    private static final String PARALLEL = "parallel";
    private static final String CALCULATE_MD5 = "calculateMD5";

    private static final PrintStream outStream;
    private static final PrintStream errorStream;
    private static final Predicate<Path> IS_FILE = (Path path) -> !Files.isDirectory(path);

    static {
        try {
            outStream = new PrintStream(System.out, true, StandardCharsets.UTF_8.name());
            errorStream = new PrintStream(System.err, true, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    //--primary-fill rootDir="rootDirectory" [pgSettingsPath="path to jdbc.properties"] [parallel=true] [calculateMD5=true]
    public static void main(String[] args) {
        try {
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
            } else if (params.containsKey(INCREMENTAL_FILL)) {
                String rootDir = params.get(ROOT_DIR);
                String pgSettingsPath = params.get(PG_SETTINGS_PATH);
                boolean parallel = Boolean.parseBoolean(params.getOrDefault(PARALLEL, "false"));
                boolean calculateMd5 = Boolean.parseBoolean(params.getOrDefault(CALCULATE_MD5, "false"));
                incrementalFill(rootDir, pgSettingsPath, parallel, calculateMd5);
            } else {
                outStream.printf("Unknown command. Only %s is supported now", Arrays.asList(PRIMARY_FILL, INCREMENTAL_FILL));
            }
        } catch (Exception e) {
            errorStream.println("Error detected: " + e);
            e.printStackTrace(errorStream);
        }
    }

    private static void incrementalFill(String rootDirectory, String jdbcPropertiesFile, boolean parallel, boolean calculateMd5) throws IOException, SQLException {
        outStream.println("Start incrementalFill(parallel=" + parallel + ") at " + LocalDateTime.now());
        Properties supportedExtensions = getSupportedExtensions();
        Set<String> unsupportedExtensions = new LinkedHashSet<>();
        String hostName = getHostName();
        List<Media> mediaList = collectMedia(rootDirectory, parallel, supportedExtensions, unsupportedExtensions, hostName);
        if (!unsupportedExtensions.isEmpty()) {
            outStream.println("Unsupported extensions: " + unsupportedExtensions);
        }
        if (calculateMd5) {
            calculateMd5(parallel, mediaList);
        }
        outStream.println("Finish incrementalFill(parallel=" + parallel + ") at " + LocalDateTime.now());
        if (jdbcPropertiesFile != null) {
            outStream.println("Start mergeToDatabase at " + LocalDateTime.now());
            int updatedRows = PgHelper.mergeToDatabase(jdbcPropertiesFile, mediaList);
            outStream.println("Finish mergeToDatabase at " + LocalDateTime.now() + ", added rows: " + updatedRows);
        }
    }

    private static void primaryFill(String rootDirectory, String jdbcPropertiesFile, boolean parallel, boolean calculateMd5) throws IOException, SQLException {
        outStream.println("Start primaryFill(parallel=" + parallel + ") at " + LocalDateTime.now());
        Properties supportedExtensions = getSupportedExtensions();
        Set<String> unsupportedExtensions = new LinkedHashSet<>();
        String hostName = getHostName();
        List<Media> mediaList = collectMedia(rootDirectory, parallel, supportedExtensions, unsupportedExtensions, hostName);
        if (!unsupportedExtensions.isEmpty()) {
            outStream.println("Unsupported extensions: " + unsupportedExtensions);
        }
        if (calculateMd5) {
            calculateMd5(parallel, mediaList);
        }
        if (jdbcPropertiesFile != null) {
            outStream.println("Start fillEmptyDatabase at " + LocalDateTime.now());
            boolean filled = PgHelper.fillEmptyDatabase(jdbcPropertiesFile, mediaList);
            if (filled) {
                outStream.println("Finish fillEmptyDatabase at " + LocalDateTime.now());
            } else {
                outStream.println("Database not empty, use command: " + INCREMENTAL_FILL);
            }
        }
        outStream.println("Finish primaryFill at " + LocalDateTime.now());
    }

    private static String getHostName() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }

    private static List<Media> collectMedia(String rootDirectory,
                                            boolean parallel,
                                            Properties supportedExtensions,
                                            Set<String> unsupportedExtensions,
                                            String systemName) throws IOException {
        List<Media> mediaList;
        try (Stream<Path> stream = Files.walk(Paths.get(rootDirectory))) {
            if (parallel) {
                mediaList = stream.parallel()
                                  .filter(IS_FILE)
                                  .map((Path path) -> processMedia(path, supportedExtensions, unsupportedExtensions, systemName))
                                  .filter(Objects::nonNull)
                                  .sorted(Comparator.comparing(Media::name))
                                  .collect(Collectors.toList());
            } else {
                mediaList = stream.filter(IS_FILE)
                                  .map((Path path) -> processMedia(path, supportedExtensions, unsupportedExtensions, systemName))
                                  .filter(Objects::nonNull)
                                  .limit(1)
                                  .sorted(Comparator.comparing(Media::name))
                                  .collect(Collectors.toList());
            }

        }
        outStream.printf("Find %d files\n", mediaList.size());
        return mediaList;
    }

    private static Properties getSupportedExtensions() throws IOException {
        Properties supportedExtensions = new Properties();
        try (InputStream resourceAsStream = FillContentHelper.class.getClassLoader().getResourceAsStream("supported_extensions.properties")) {
            supportedExtensions.load(resourceAsStream);
        }
        return supportedExtensions;
    }

    private static void calculateMd5(boolean parallel, List<Media> mediaList) {
        outStream.printf("Calculate MD5 for %d files\n", mediaList.size());
        Instant begin = Instant.now();
        if (parallel) {
            mediaList.parallelStream()
                     .forEach(Media::calculateMd5);
        }
        Duration duration = Duration.between(begin, Instant.now());
        outStream.println("MD5 calculated at " + duration.toString().replace("PT", ""));
    }

    @Nullable
    private static Media processMedia(Path path, Properties supportedExtensions, Set<String> unsupportedExtensions, String systemName) {
        String fileName = path.getFileName().toString();
        String extension = getExtension(fileName);
        String type = supportedExtensions.getProperty(extension);
        if (type == null) {
            unsupportedExtensions.add(extension);
            return null;
        } else {
            return getMedia(path, fileName, type, systemName);
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
