package ru.alejov.media.gallery;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class JsonIOHelper {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public JsonIOHelper() {
    }

    @SuppressWarnings("unchecked")
    public static List<Media> parseMedia(InputStream inputStream) throws IOException {

        Map<String, Object>[] files = OBJECT_MAPPER.readValue(inputStream, Map[].class);
        List<Media> mediaList = new ArrayList<>(files.length);
        for (Map<String, Object> map : files) {
            String name = (String) map.get("name");
            Timestamp createdAt = new Timestamp((Long) map.get("createdAt"));
            Timestamp lastModify = new Timestamp((Long) map.get("lastModify"));
            Map<String, String> paths = (Map<String, String>) map.get("paths");
            Number size = (Number) map.get("size");
            String md5Hash = (String) map.get("md5Hash");
            String type = (String) map.get("type");
            Map<String, String> metadata = (Map<String, String>) map.get("metadata");
            Media media = new Media(name, createdAt, lastModify, paths, md5Hash, size.longValue(), type, metadata, null);
            mediaList.add(media);
        }
        mediaList.sort(Media::compareTo);
        return mediaList;
    }

    public void toJsonFile(List<Media> mediaList) throws IOException {
        File resultFile = new File("media.zip");
        try (ZipOutputStream zipOutputStream = new ZipOutputStream(Files.newOutputStream(resultFile.toPath()), StandardCharsets.UTF_8)) {
            zipOutputStream.putNextEntry(new ZipEntry("media.json"));
            zipOutputStream.setLevel(5);
            OBJECT_MAPPER.writeValue(zipOutputStream, mediaList);
        }
    }
}
