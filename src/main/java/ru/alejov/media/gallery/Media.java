package ru.alejov.media.gallery;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Objects;

public final class Media implements Comparable<Media> {
    private final String name;
    private final Timestamp createdAt;
    private final Timestamp lastModify;
    private final Map<String, String> paths;
    private final long size;
    private final String type;
    private final Map<MetaTag, String> metadata;
    private String md5Hash;
    @JsonIgnore
    private transient final Path localPath;
    @JsonIgnore
    private transient final String nameToSort;

    public Media(String name,
                 Timestamp createdAt,
                 Timestamp lastModify,
                 Map<String, String> paths,
                 String md5Hash,
                 long size,
                 String type,
                 Map<MetaTag, String> metadata,
                 Path localPath) {
        this.name = name;
        this.createdAt = createdAt;
        this.lastModify = lastModify;
        this.paths = paths;
        this.md5Hash = md5Hash;
        this.size = size;
        this.type = type;
        this.metadata = metadata;
        this.localPath = localPath;
        this.nameToSort = name.replace("_", "").replace("-", "");
    }

    public String getName() {
        return name;
    }

    public Timestamp getCreatedAt() {
        return createdAt;
    }

    public Map<String, String> getPaths() {
        return paths;
    }

    public String getMd5Hash() {
        return md5Hash;
    }

    public long getSize() {
        return size;
    }

    public String getType() {
        return type;
    }

    public Map<MetaTag, String> getMetadata() {
        return metadata;
    }

    @JsonIgnore
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
               "lastModify=" + lastModify + ", " +
               "paths=" + paths + ", " +
               "md5Hash=" + md5Hash + ", " +
               "size=" + size + ", " +
               "type=" + type + ", " +
               "metadata=" + metadata + ']';
    }

    @Nonnull
    public Timestamp getLastModify() {
        return lastModify;
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
