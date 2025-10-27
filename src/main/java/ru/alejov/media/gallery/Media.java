package ru.alejov.media.gallery;

import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Objects;

public final class Media implements Comparable<Media> {
    private final String name;
    private final Timestamp createdAt;
    private final Map<String, String> paths;
    private final long size;
    private final String type;
    private final Map<MetaTag, String> metadata;
    private String md5Hash;
    private transient final Path localPath;
    private transient final String nameToSort;

    public Media(String name,
                 Timestamp createdAt,
                 Map<String, String> paths,
                 String md5Hash,
                 long size,
                 String type,
                 Map<MetaTag, String> metadata,
                 Path localPath) {
        this.name = name;
        this.createdAt = createdAt;
        this.paths = paths;
        this.md5Hash = md5Hash;
        this.size = size;
        this.type = type;
        this.metadata = metadata;
        this.localPath = localPath;
        this.nameToSort = name.replace("_", "").replace("-", "");
    }

    public String name() {
        return name;
    }

    public Timestamp createdAt() {
        return createdAt;
    }

    public Map<String, String> paths() {
        return paths;
    }

    public String md5Hash() {
        return md5Hash;
    }

    public long size() {
        return size;
    }

    public String type() {
        return type;
    }

    public Map<MetaTag, String> metadata() {
        return metadata;
    }

    public Path getLocalPath() {
        return localPath;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        Media that = (Media) obj;
        return Objects.equals(this.name, that.name) &&
               Objects.equals(this.md5Hash, that.md5Hash) &&
               this.size == that.size &&
               Objects.equals(this.type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, md5Hash, size, type);
    }

    @Override
    public String toString() {
        return "Media[" +
               "name=" + name + ", " +
               "createdAt=" + createdAt + ", " +
               "paths=" + paths + ", " +
               "md5Hash=" + md5Hash + ", " +
               "size=" + size + ", " +
               "type=" + type + ", " +
               "metadata=" + metadata + ']';
    }


    public void calculateMd5() {
        if (md5Hash == null) {
            md5Hash = HashUtils.getMd5Hash(localPath);
        }
    }

    @Override
    public int compareTo(Media o) {
        return nameToSort.compareTo(o.nameToSort);
    }
}
