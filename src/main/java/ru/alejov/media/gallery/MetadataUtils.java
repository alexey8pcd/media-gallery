package ru.alejov.media.gallery;

import org.apache.commons.imaging.Imaging;
import org.apache.commons.imaging.common.ImageMetadata;

import javax.annotation.Nullable;
import java.awt.image.BufferedImage;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class MetadataUtils {
    public static Map<String, String> getMetadata(Path path, String type) {
        Map<String, String> metadata = Collections.emptyMap();
        if (type.equals("i")) {
            try {
                ImageMetadata imageMetadata = Imaging.getMetadata(path.toFile());
                if (imageMetadata != null) {
                    List<? extends ImageMetadata.ImageMetadataItem> items = imageMetadata.getItems();
                    List<Tag> tagList = items.stream()
                                             .map(Objects::toString)
                                             .map(Tag::parse)
                                             .filter(Objects::nonNull)
                                             .toList();
                    metadata = new TreeMap<>();
                    boolean hasSize = false;
                    for (Tag tag : tagList) {
                        metadata.put(tag.key, tag.value);
                        if (tag.key.contains("ImageWidth")) {
                            hasSize = true;
                        }
                    }
                    if (!hasSize) {
                        BufferedImage bufferedImage = Imaging.getBufferedImage(path.toFile());
                        int width = bufferedImage.getWidth();
                        int height = bufferedImage.getHeight();
                        metadata.put("ImageWidth", String.valueOf(width));
                        metadata.put("ImageLength", String.valueOf(height));
                    } else {
                        metadata.put("ImageWidth", metadata.remove("ExifImageWidth"));
                        metadata.put("ImageLength", metadata.remove("ExifImageLength"));
                    }
                }
            } catch (Exception e) {
                System.err.println("Image " + path + " metadata error: " + e);
            }
        }
        return metadata;
    }

    public static class Tag {

        private final String key;
        private final String value;

        public Tag(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Nullable
        public static Tag parse(String s) {
            String[] strings = s.split(":");
            String key = strings[0];
            if (!legal(key.strip())) {
                return null;
            }
            String value;
            int length = strings.length;
            if (length == 1) {
                value = null;
            } else if (length == 2) {
                value = strings[1].strip();
            } else {
                StringBuilder stringBuilder = new StringBuilder(strings[1]);
                for (int i = 2; i < length; i++) {
                    stringBuilder.append(strings[i]);
                }
                value = stringBuilder.toString().strip();
            }
            if (value != null && (key.equals("Make") || key.equals("Model") || key.equals("Software"))) {
                value = value.replace(",", "").strip();
            }
            return new Tag(key, value);
        }

        @Override
        public String toString() {
            return key + ":" + value;
        }

        private static boolean legal(String key) {
            return key.equals("ExifImageWidth") ||
                   key.equals("ExifImageLength") ||
                   key.equals("DateTimeOriginal") ||
                   key.equals("Make") ||
                   key.equals("Model") ||
                   key.equals("GPSLatitude") ||
                   key.equals("GPSLongitude") ||
                   key.equals("Software") ||
                   key.equals("Orientation");
        }

    }
}
