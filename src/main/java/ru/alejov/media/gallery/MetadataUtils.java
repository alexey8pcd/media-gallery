package ru.alejov.media.gallery;

import org.apache.commons.imaging.Imaging;
import org.apache.commons.imaging.common.ImageMetadata;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.awt.image.BufferedImage;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class MetadataUtils {


    @Nonnull
    public static Map<MetaTag, String> getMetadata(Path path, String type) {
        Map<MetaTag, String> metadata = Collections.emptyMap();
        if (type.equals("i")) {
            try {
                ImageMetadata imageMetadata = Imaging.getMetadata(path.toFile());
                if (imageMetadata != null) {
                    List<? extends ImageMetadata.ImageMetadataItem> items = imageMetadata.getItems();
                    List<Tag> tagList = items.stream()
                                             .map(Objects::toString)
                                             .map(Tag::parse)
                                             .filter(Objects::nonNull)
                                             .collect(Collectors.toList());
                    metadata = new TreeMap<>();
                    boolean hasSize = false;
                    for (Tag tag : tagList) {
                        metadata.put(tag.key, tag.value);
                        if (tag.key == MetaTag.ImageWidth || tag.key == MetaTag.ExifImageWidth) {
                            hasSize = true;
                        }
                    }
                    if (!hasSize) {
                        BufferedImage bufferedImage = Imaging.getBufferedImage(path.toFile());
                        int width = bufferedImage.getWidth();
                        int height = bufferedImage.getHeight();
                        metadata.put(MetaTag.ImageWidth, String.valueOf(width));
                        metadata.put(MetaTag.ImageLength, String.valueOf(height));
                    } else {
                        metadata.put(MetaTag.ImageWidth, metadata.remove(MetaTag.ExifImageWidth));
                        metadata.put(MetaTag.ImageLength, metadata.remove(MetaTag.ExifImageLength));
                    }
                }
            } catch (Exception e) {
                System.err.println("Image " + path + " metadata error: " + e);
            }
        }
        return metadata;
    }

    public static class Tag {

        private final MetaTag key;
        private final String value;

        public Tag(MetaTag key, String value) {
            this.key = key;
            this.value = value;
        }

        @Nullable
        public static Tag parse(String s) {
            String[] strings = s.split(":");
            MetaTag metaTag = MetaTag.of(strings[0].trim());
            if (metaTag == null) {
                return null;
            }
            String value;
            int length = strings.length;
            if (length == 1) {
                value = null;
            } else if (length == 2) {
                value = strings[1].trim();
            } else {
                StringBuilder stringBuilder = new StringBuilder(strings[1]);
                for (int i = 2; i < length; i++) {
                    stringBuilder.append(strings[i]);
                }
                value = stringBuilder.toString().trim();
            }
            if (value != null && (metaTag == MetaTag.Make || metaTag == MetaTag.Model || metaTag == MetaTag.Software)) {
                value = value.replace(",", "").trim();
            }
            return new Tag(metaTag, value);
        }

        @Override
        public String toString() {
            return key + ":" + value;
        }


    }
}
