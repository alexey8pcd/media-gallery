package ru.alejov.media.gallery;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateUtils {
    static final Pattern DATE_FULL_PATTERN = Pattern.compile(".*([0-9]{8}_[0-9]{6}).*");
    static final Pattern DATE_SHORT_PATTERN = Pattern.compile(".*([0-9]{8}).*");
    static final DateTimeFormatter DATE_FULL_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    //20211002 143434
    static final DateTimeFormatter DATE_FULL_FORMAT2 = DateTimeFormatter.ofPattern("yyyyMMdd HHmmss");
    static final DateTimeFormatter DATE_SHORT_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    @Nullable
    public static Timestamp getCreateDate(Map<MetaTag, String> tags, String fileName) {
        Matcher dateFullMatcher = DateUtils.DATE_FULL_PATTERN.matcher(fileName);
        if (dateFullMatcher.find()) {
            String dateFull = dateFullMatcher.group(1);
            try {
                LocalDateTime localDateTime = LocalDateTime.parse(dateFull, DateUtils.DATE_FULL_FORMAT);
                return Timestamp.valueOf(localDateTime);
            } catch (DateTimeParseException ignored) {
            }
        } else {
            String dateTimeOriginal = tags.get(MetaTag.DateTimeOriginal);
            Timestamp createDate = null;
            if (dateTimeOriginal != null) {
                createDate = parseDateTimeMetadata(dateTimeOriginal);
            }
            if (createDate == null) {
                Matcher dateShortMatcher = DateUtils.DATE_SHORT_PATTERN.matcher(fileName);
                if (dateShortMatcher.find()) {
                    String dateShort = dateShortMatcher.group(1);
                    try {
                        LocalDate localDate = LocalDate.parse(dateShort, DateUtils.DATE_SHORT_FORMAT);
                        createDate = Timestamp.valueOf(localDate.atStartOfDay());
                    } catch (DateTimeParseException ignored) {
                    }
                }
            }
            return createDate;
        }
        return null;
    }


    private static Timestamp parseDateTimeMetadata(@Nonnull String dateTimeOriginal) {
        try {
            String removed = StringUtils.remove(dateTimeOriginal, '\'');
            LocalDateTime localDateTime = LocalDateTime.parse(removed, DateUtils.DATE_FULL_FORMAT2);
            return Timestamp.valueOf(localDateTime);
        } catch (DateTimeParseException e) {
            e.printStackTrace();
        }
        return null;
    }
}
