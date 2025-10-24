package ru.alejov.media.gallery;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public enum MetaTag {
    ExifImageWidth,
    ImageWidth,
    ExifImageLength,
    ImageLength,
    DateTimeOriginal,
    Make,
    Model,
    GPSLatitude,
    GPSLatitudeRef,
    GPSLongitude,
    GPSLongitudeRef,
    Software,
    Orientation;

    public static final Set<String> VALUES = Arrays.stream(MetaTag.values())
                                                   .map(MetaTag::name)
                                                   .collect(Collectors.toSet());

    @Nullable
    public static MetaTag of(String s) {
        if (VALUES.contains(s)) {
            return MetaTag.valueOf(s);
        }
        return null;
    }
}
