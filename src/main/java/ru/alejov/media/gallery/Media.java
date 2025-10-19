package ru.alejov.media.gallery;

import java.sql.Timestamp;
import java.util.Map;

public record Media(String name, Timestamp createdAt, String path, String md5Hash, long size, String type,
                    Map<String, String> metadata) {

}
