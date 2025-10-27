package ru.alejov.media.gallery.init;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FillContentHelper {

    private static final Logger log;

    private static final String PRIMARY_FILL = "--primary-fill";
    private static final String INCREMENTAL_FILL = "--incremental-fill";

    private static final String ROOT_DIR = "rootDir";
    private static final String PG_SETTINGS_PATH = "pgSettingsPath";
    private static final String PARALLEL = "parallel";
    private static final String CALCULATE_MD5 = "calculateMD5";

    private static final Predicate<Path> IS_FILE = (Path path) -> !Files.isDirectory(path);

    static {
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss.SSS");
        log = LoggerFactory.getLogger(FillContentHelper.class);
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
                log.warn("Unknown command. Only {} is supported now", Arrays.asList(PRIMARY_FILL, INCREMENTAL_FILL));
            }
        } catch (Exception e) {
            log.error(e.toString(), e);
        }
    }

    private static void incrementalFill(String rootDirectory, String jdbcPropertiesFile, boolean parallel, boolean calculateMd5) throws IOException, SQLException {
        log.info("Start incrementalFill(parallel={})", parallel);
        Properties supportedExtensions = getSupportedExtensions();
        Set<String> unsupportedExtensions = new LinkedHashSet<>();
        String hostName = getHostName();
        List<Media> mediaList = collectMedia(rootDirectory, parallel, supportedExtensions, unsupportedExtensions, hostName);
        if (!unsupportedExtensions.isEmpty()) {
            log.warn("Unsupported extensions: {}", unsupportedExtensions);
        }
        if (calculateMd5) {
            calculateMd5(parallel, mediaList);
        }
        log.info("Finish incrementalFill(parallel={})", parallel);
        if (jdbcPropertiesFile != null) {
            new PgHelper(log).mergeToDatabase(jdbcPropertiesFile, mediaList);
        }
    }

    private static void primaryFill(String rootDirectory, String jdbcPropertiesFile, boolean parallel, boolean calculateMd5) throws IOException, SQLException {
        log.info("Start primaryFill(parallel={})", parallel);
        Properties supportedExtensions = getSupportedExtensions();
        Set<String> unsupportedExtensions = new LinkedHashSet<>();
        String hostName = getHostName();
        List<Media> mediaList = collectMedia(rootDirectory, parallel, supportedExtensions, unsupportedExtensions, hostName);
        if (!unsupportedExtensions.isEmpty()) {
            log.warn("Unsupported extensions: {}", unsupportedExtensions);
        }
        if (calculateMd5) {
            calculateMd5(parallel, mediaList);
        }
        if (jdbcPropertiesFile != null) {
            boolean filled = new PgHelper(log).fillEmptyDatabase(jdbcPropertiesFile, mediaList);
        }
        log.info("Finish primaryFill");
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
                                  .sorted()
                                  .collect(Collectors.toList());
            } else {
                mediaList = stream.filter(IS_FILE)
                                  .map((Path path) -> processMedia(path, supportedExtensions, unsupportedExtensions, systemName))
                                  .filter(Objects::nonNull)
                                  .sorted()
                                  .collect(Collectors.toList());
            }

        }
        log.info("Find {} files", mediaList.size());
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
        log.info("Calculate MD5 for {} files", mediaList.size());
        Instant begin = Instant.now();
        if (parallel) {
            mediaList.parallelStream()
                     .forEach(Media::calculateMd5);
        }
        Duration duration = Duration.between(begin, Instant.now());
        log.info("MD5 calculated at {}", duration.toString().replace("PT", ""));
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
            Path absolutePath = path.toAbsolutePath();
            Map<String, String> paths = Collections.singletonMap(systemName, absolutePath.toString());
            media = new Media(fileName, createDate, paths, null, path.toFile().length(), type, metadata, absolutePath);
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
