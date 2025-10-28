package ru.alejov.media.gallery;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class JsonWriteHelper {
    private final Logger log;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public JsonWriteHelper(Logger log) {
        this.log = log;
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
