package ru.alejov.media.gallery.init;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.alejov.media.gallery.DateUtils;
import ru.alejov.media.gallery.JsonIOHelper;
import ru.alejov.media.gallery.Media;
import ru.alejov.media.gallery.MetaTag;
import ru.alejov.media.gallery.MetadataUtils;
import ru.alejov.media.gallery.PgHelper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class FillContentHelper {

    public static final Logger log;

    private static final String PRIMARY_FILL = "--primary-fill";
    private static final String INCREMENTAL_FILL = "--incremental-fill";
    private static final String HELP = "--help";

    private static final String ROOT_DIR = "root-dir";
    private static final String SOURCE_FILE = "source-file";
    private static final String PG_SETTINGS_PATH = "pg-settings-path";
    private static final String PARALLEL = "parallel";
    private static final String CALCULATE_MD5 = "calculate-hash";

    private static final Predicate<Path> IS_FILE = (Path path) -> !Files.isDirectory(path);

    static {
        try {
            System.setOut(new PrintStream(System.out, true, StandardCharsets.UTF_8.name()));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss.SSS");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        log = LoggerFactory.getLogger(FillContentHelper.class);
    }

    //--primary-fill root-dir="rootDirectory" [pg-settings-path="path to jdbc.properties"] [parallel=true] [calculate-hash=true]
    public static void main(String[] args) {
        try {
            Map<String, String> params = new HashMap<>();
            for (String arg : args) {
                String[] strings = arg.split("=");
                params.put(strings[0], strings.length > 1 ? strings[1] : "");
            }
            if (params.containsKey(PRIMARY_FILL)) {
                String rootDir = params.get(ROOT_DIR);
                if (rootDir != null) {
                    String pgSettingsPath = params.get(PG_SETTINGS_PATH);
                    boolean parallel = Boolean.parseBoolean(params.getOrDefault(PARALLEL, "false"));
                    boolean calculateMd5 = Boolean.parseBoolean(params.getOrDefault(CALCULATE_MD5, "false"));
                    primaryFill(rootDir, pgSettingsPath, parallel, calculateMd5);
                } else {
                    System.out.println("Missing parameter: " + ROOT_DIR);
                }
            } else if (params.containsKey(INCREMENTAL_FILL)) {
                String pgSettingsPath = params.get(PG_SETTINGS_PATH);
                if (pgSettingsPath != null) {
                    String rootDir = params.get(ROOT_DIR);
                    String sourceFile = params.get(SOURCE_FILE);
                    boolean parallel = Boolean.parseBoolean(params.getOrDefault(PARALLEL, "false"));
                    boolean calculateMd5 = Boolean.parseBoolean(params.getOrDefault(CALCULATE_MD5, "false"));
                    if (rootDir != null) {
                        incrementalFillFromDir(rootDir, pgSettingsPath, parallel, calculateMd5);
                    } else if (sourceFile != null) {
                        incrementalFillFromFile(new File(sourceFile), pgSettingsPath, parallel);
                    } else {
                        System.out.println("Missing any parameters: " + Arrays.asList(ROOT_DIR, SOURCE_FILE));
                    }
                } else {
                    System.out.println("Missing parameter: " + PG_SETTINGS_PATH);
                }
            } else if (params.containsKey(HELP)) {
                System.out.println("Example: [--primary-fill | --incremental-fill] root-dir=\"rootDirectory\" [pg-settings-path=\"path to jdbc.properties\"] [parallel=true] [calculate-hash=true]");
            } else {
                System.out.println("Unknown command. Only " + Arrays.asList(PRIMARY_FILL, INCREMENTAL_FILL, HELP) + " is supported now");
            }
        } catch (Exception e) {
            log.error(e.toString(), e);
        }
    }

    private static void incrementalFillFromFile(File sourceFile,
                                                @Nonnull String jdbcPropertiesFile,
                                                boolean parallel) throws IOException, SQLException {
        log.info("Start incrementalFillFromFile(parallel={})", parallel);
        String hostName = getHostName();
        List<Media> mediaList = collectMediaFromFile(sourceFile);
        log.info("Finish incrementalFillFromFile(parallel={})", parallel);
        new PgHelper(log).mergeToDatabase(jdbcPropertiesFile, mediaList, hostName);
    }

    private static void incrementalFillFromDir(String rootDirectory,
                                               @Nonnull String jdbcPropertiesFile,
                                               boolean parallel,
                                               boolean calculateMd5) throws IOException, SQLException {
        log.info("Start incrementalFillFromDir(parallel={})", parallel);
        Properties supportedExtensions = getSupportedExtensions();
        Set<String> unsupportedExtensions = new LinkedHashSet<>();
        String hostName = getHostName();
        List<Media> mediaList = collectMediaFromDir(rootDirectory, parallel, supportedExtensions, unsupportedExtensions, hostName);
        if (!unsupportedExtensions.isEmpty()) {
            log.warn("Unsupported extensions: {}", unsupportedExtensions);
        }
        if (calculateMd5) {
            calculateMd5(parallel, mediaList);
        }
        log.info("Finish incrementalFillFromDir(parallel={})", parallel);
        new PgHelper(log).mergeToDatabase(jdbcPropertiesFile, mediaList, hostName);
    }

    private static void primaryFill(String rootDirectory, String jdbcPropertiesFile, boolean parallel, boolean calculateMd5) throws IOException, SQLException {
        log.info("Start primaryFill(parallel={})", parallel);
        Properties supportedExtensions = getSupportedExtensions();
        Set<String> unsupportedExtensions = new LinkedHashSet<>();
        String hostName = getHostName();
        List<Media> mediaList = collectMediaFromDir(rootDirectory, parallel, supportedExtensions, unsupportedExtensions, hostName);
        if (!unsupportedExtensions.isEmpty()) {
            log.warn("Unsupported extensions: {}", unsupportedExtensions);
        }
        if (calculateMd5) {
            calculateMd5(parallel, mediaList);
        }
        if (jdbcPropertiesFile != null) {
            new PgHelper(log).fillEmptyDatabase(jdbcPropertiesFile, mediaList);
        } else {
            new JsonIOHelper().toJsonFile(mediaList);
        }
        log.info("Finish primaryFill");
    }

    private static String getHostName() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }

    private static List<Media> collectMediaFromFile(File sourceFile) throws IOException {
        List<Media> mediaList;
        try (ZipInputStream zipInputStream = new ZipInputStream(Files.newInputStream(sourceFile.toPath()))) {
            ZipEntry nextEntry = zipInputStream.getNextEntry();
            mediaList = JsonIOHelper.parseMedia(zipInputStream);
        }
        int size = mediaList.size();
        log.info("Find {} files", size);
        return mediaList;
    }

    private static List<Media> collectMediaFromDir(String rootDirectory,
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
        int size = mediaList.size();
        log.info("Find {} files", size);
        extractMetadata(size, mediaList, parallel);
        return mediaList;
    }

    private static void extractMetadata(int size, List<Media> mediaList, boolean parallel) {
        log.info("Start extracting metadata(parallel={})", parallel);
        AtomicInteger progress = new AtomicInteger();
        AtomicInteger toThreshold = new AtomicInteger();
        final int threshold = getThreshold(size);
        if (parallel) {
            mediaList.parallelStream().forEach(media -> {
                extractMetadataInner(size, media, progress, toThreshold, threshold);
            });
        } else {
            for (Media media : mediaList) {
                extractMetadataInner(size, media, progress, toThreshold, threshold);
            }
        }
        log.info("Finish extracting metadata");
    }

    private static int getThreshold(int size) {
        final int threshold;
        if (size < 1000) {
            threshold = size / 25;
        } else if (size < 10000) {
            threshold = size / 125;
        } else {
            threshold = size / 500;
        }
        return threshold;
    }

    private static void extractMetadataInner(int size, Media media, AtomicInteger progress, AtomicInteger toThreshold, int threshold) {
        Map<String, String> metadata = MetadataUtils.getMetadata(media.getLocalPath(), media.getType());
        if (!metadata.isEmpty()) {
            media.setMetadata(metadata);
            Timestamp createDate = DateUtils.getCreateDate(metadata, media.getName());
            if (createDate != null) {
                media.setCreateDate(createDate);
            }
        }
        progress.incrementAndGet();
        if (toThreshold.incrementAndGet() > threshold) {
            toThreshold.set(0);
            log.info("Progress {}/{}", progress, size);
        }
    }

    private static Properties getSupportedExtensions() throws IOException {
        Properties supportedExtensions = new Properties();
        try (InputStream resourceAsStream = FillContentHelper.class.getClassLoader().getResourceAsStream("supported_extensions.properties")) {
            supportedExtensions.load(resourceAsStream);
        }
        return supportedExtensions;
    }

    private static void calculateMd5(boolean parallel, List<Media> mediaList) {
        int size = mediaList.size();
        log.info("Calculate MD5 for {} files", size);
        Instant begin = Instant.now();
        AtomicInteger progress = new AtomicInteger();
        AtomicInteger toThreshold = new AtomicInteger();
        final int threshold = getThreshold(size);
        if (parallel) {
            mediaList.parallelStream()
                     .forEach(media -> media.calculateMd5(threshold, toThreshold, progress, size));
        } else {
            for (Media media : mediaList) {
                media.calculateMd5(threshold, toThreshold, progress, size);
            }
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

    @Nullable
    private static Media getMedia(Path path, String fileName, String type, String systemName) {
        try {
            Timestamp createDate = DateUtils.getCreateDate(Collections.emptyMap(), fileName);
            BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
            if (createDate == null) {
                FileTime creationTime = attributes.creationTime();
                createDate = Timestamp.from(creationTime.toInstant());
            }
            Timestamp lastModify = Timestamp.from(attributes.lastModifiedTime().toInstant());
            Path absolutePath = path.toAbsolutePath();
            Map<String, String> paths = Collections.singletonMap(systemName, absolutePath.toString());
            return new Media(fileName, createDate, lastModify, paths, null, path.toFile().length(), type, Collections.emptyMap(), absolutePath);
        } catch (IOException e) {
            log.error("Error on file: {}: {}", path, e.toString());
            return null;
        }
    }


    public static String getExtension(String fileName) {
        if (!fileName.contains(".")) {
            return "";
        }
        String[] parts = fileName.split("\\.");
        return parts[parts.length - 1].toLowerCase(Locale.ENGLISH);
    }

}
