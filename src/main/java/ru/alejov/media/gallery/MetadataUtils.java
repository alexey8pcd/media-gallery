package ru.alejov.media.gallery;

import org.apache.commons.imaging.Imaging;
import org.apache.commons.imaging.common.ImageMetadata;
import org.apache.commons.lang3.StringUtils;

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

import static ru.alejov.media.gallery.init.FillContentHelper.log;

public class MetadataUtils {


    @Nonnull
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
                                             .collect(Collectors.toList());
                    metadata = new TreeMap<>();
                    boolean hasSize = false;
                    for (Tag tag : tagList) {
                        metadata.put(tag.key.name(), tag.value);
                        if (tag.key == MetaTag.ImageWidth || tag.key == MetaTag.ExifImageWidth) {
                            hasSize = true;
                        }
                    }
                    if (!hasSize) {
                        BufferedImage bufferedImage = Imaging.getBufferedImage(path.toFile());
                        int width = bufferedImage.getWidth();
                        int height = bufferedImage.getHeight();
                        metadata.put(MetaTag.ImageWidth.name(), String.valueOf(width));
                        metadata.put(MetaTag.ImageLength.name(), String.valueOf(height));
                    } else {
                        metadata.put(MetaTag.ImageWidth.name(), metadata.remove(MetaTag.ExifImageWidth.name()));
                        metadata.put(MetaTag.ImageLength.name(), metadata.remove(MetaTag.ExifImageLength.name()));
                    }
                }
            } catch (Exception e) {
                log.warn("Image {} metadata error: {}", path, e.toString());
            }
        }
        return metadata;
    }

    public static void clearMetadataValues(@Nonnull Map<String, String> input) {
        input.replaceAll((String key, String value) -> clearMetadata(value));
    }

    private static String clearMetadata(String value) {
        return StringUtils.strip(removeStartEnd(value, "'"));
    }

    private static String removeStartEnd(String value, String symbol) {
        if (value == null) {
            return null;
        }
        if (value.startsWith(symbol)) {
            if (value.endsWith(symbol)) {
                return value.substring(1, value.length() - 1);
            } else {
                return value.substring(1);
            }
        } else {
            if (value.endsWith(symbol)) {
                return value.substring(0, value.length() - 2);
            } else {
                return value;
            }
        }
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
            String cleared = clearMetadata(value);
            return new Tag(metaTag, cleared);
        }

        @Override
        public String toString() {
            return key + ":" + value;
        }


    }

}
