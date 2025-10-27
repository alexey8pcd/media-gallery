package ru.alejov.media.gallery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("ConcatenationWithEmptyString")
public class PgHelper {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String INSERT_SQL = ""
                                             + "INSERT INTO media(name, create_date, metadata, paths, type, file_size, hash_md5)\n"
                                             + "VALUES (?, ?, ?::jsonb, ?::jsonb, ?, ?, ?)\n"
                                             + "RETURNING id";
    private static final String TEST_SELECT = ""
                                              + "SELECT name\n"
                                              + "  FROM media\n"
                                              + " LIMIT 1";
    private static final String SELECT_SQL = ""
                                             + "SELECT id,\n"
                                             + "       name,\n"
                                             + "       replace(replace(name,'-',''),'_','') AS name_to_sort,\n"
                                             + "       file_size,\n"
                                             + "       hash_md5,\n"
                                             + "       paths\n"
                                             + "  FROM media\n"
                                             + " ORDER BY name_to_sort";
    private static final String UPDATE_MD5_SQL = ""
                                                 + "UPDATE media\n"
                                                 + "   SET hash_md5 = ?\n"
                                                 + " WHERE id = ?";
    private static final String UPDATE_NAME_SQL = ""
                                                  + "UPDATE media\n"
                                                  + "   SET name = ?\n"
                                                  + " WHERE id = ?";
    private static final String UPDATE_PATHS_SQL = ""
                                                   + "UPDATE media\n"
                                                   + "   SET paths = ?::jsonb\n"
                                                   + " WHERE id = ?";
    private static final int LIMIT = 500;
    private final Logger log;

    public PgHelper(Logger log) {
        this.log = log;
    }


    public boolean fillEmptyDatabase(String jdbcPropertiesFilePath, List<Media> mediaList) throws IOException, SQLException {
        log.info("Start fillEmptyDatabase");
        Properties properties = new Properties();
        try (InputStream inputStream = Files.newInputStream(Paths.get(jdbcPropertiesFilePath))) {
            properties.load(inputStream);
        }
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(properties.getProperty("pg.url"));
        dataSource.setUser(properties.getProperty("pg.user"));
        dataSource.setPassword(properties.getProperty("pg.password"));
        boolean filled;
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(true);
            try (PreparedStatement insertStatement = connection.prepareStatement(INSERT_SQL);
                 PreparedStatement testStatement = connection.prepareStatement(TEST_SELECT)) {
                try (ResultSet resultSet = testStatement.executeQuery()) {
                    filled = !resultSet.next();
                }
                if (filled) {
                    int count = 0;
                    for (Media media : mediaList) {
                        fillInsertStatement(media, insertStatement);
                        insertStatement.addBatch();
                        ++count;
                        if (count > 100) {
                            insertStatement.executeBatch();
                        }
                    }
                    insertStatement.executeBatch();
                }
            }
        }

        if (filled) {
            log.info("Finish fillEmptyDatabase");
        } else {
            log.warn("Database not empty");
        }
        return filled;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public void mergeToDatabase(String jdbcPropertiesFilePath, List<Media> mediaList) throws IOException, SQLException {
        log.info("Start mergeToDatabase");
        if (mediaList.isEmpty()) {
            return;
        }
        Properties properties = new Properties();
        try (InputStream inputStream = Files.newInputStream(Paths.get(jdbcPropertiesFilePath))) {
            properties.load(inputStream);
        }
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(properties.getProperty("pg.url"));
        dataSource.setUser(properties.getProperty("pg.user"));
        dataSource.setPassword(properties.getProperty("pg.password"));
        AtomicInteger insertedCount = new AtomicInteger();
        AtomicInteger updatedCount = new AtomicInteger();
        int commitChunk = 10_000;
        int commitThreshold = commitChunk;
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement selectStatement = connection.prepareStatement(SELECT_SQL);
                 PreparedStatement insertStmt = connection.prepareStatement(INSERT_SQL);
                 PreparedStatement updateMd5Statement = connection.prepareStatement(UPDATE_MD5_SQL);
                 PreparedStatement updateNameStmt = connection.prepareStatement(UPDATE_NAME_SQL);
                 PreparedStatement updatePathsStmt = connection.prepareStatement(UPDATE_PATHS_SQL)) {
                selectStatement.setFetchSize(LIMIT);
                Iterator<Media> mediaIterator = mediaList.iterator();
                Media media = nextMedia(mediaIterator);
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    DbMedia dbMedia = nextFromDb(resultSet);
                    while (dbMedia != null && media != null) {
                        int compared = media.name().compareTo(dbMedia.name);
                        if (compared == 0) {
                            mergeSameFiles(media, dbMedia, updatePathsStmt, updateMd5Statement, updateNameStmt, insertStmt, insertedCount, updatedCount);
                            dbMedia = nextFromDb(resultSet);
                            media = nextMedia(mediaIterator);
                        } else if (compared > 0) {
                            //Файл в памяти больше, чем в базе - возможно файл удалили. Но он может быть на другом устройстве.
                            //Ничего не делаем, выбираем следующий из базы
                            dbMedia = nextFromDb(resultSet);
                        } else {
                            //Файл в базе больше, чем в памяти. Файл надо добавить.
                            fillInsertStatement(media, insertStmt);
                            insertStmt.execute();
                            log.info("File '{}' inserted", media.getLocalPath().toString());
                            insertedCount.incrementAndGet();
                            //Выбираем следующий из памяти
                            media = nextMedia(mediaIterator);
                        }
                        if (insertedCount.get() + updatedCount.get() > commitThreshold) {
                            connection.commit();
                            commitThreshold += commitChunk;
                        }
                    }
                    if (dbMedia == null && media != null) {
                        //нет больше записей в БД
                        fillInsertStatement(media, insertStmt);
                        insertStmt.execute();
                        insertedCount.incrementAndGet();
                        log.info("File '{}' inserted", media.getLocalPath().toString());

                        int count = insertRestMedia(mediaIterator, insertStmt);
                        if (count > 0) {
                            insertedCount.addAndGet(count);
                            log.info("Inserted '{}' new files", count);
                        }
                    } else if (dbMedia != null) {
                        //остальное есть в БД, пропускаем
                    } else {
                        //ничего не осталось
                    }
                }
                connection.commit();
            }
        }
        log.info("Finish mergeToDatabase. Inserted rows: " + insertedCount + ", updated rows: " + updatedCount);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private void mergeSameFiles(Media media,
                                DbMedia dbMedia,
                                PreparedStatement updatePathsStmt,
                                PreparedStatement updateMd5Statement,
                                PreparedStatement updateNameStmt,
                                PreparedStatement insertStmt,
                                AtomicInteger insertedCount,
                                AtomicInteger updatedCount) throws SQLException, JsonProcessingException {
        //Записи одинаковые. Проверяем fileSize и md5
        if (media.size() == dbMedia.fileSize) {
            if (Objects.equals(media.md5Hash(), dbMedia.md5Hash)) {
                //записи абсолютно одинаковые, допишем путь, если это другое устройство
                Map<String, String> paths = media.paths();
                String device = paths.keySet().iterator().next();
                if (!dbMedia.paths.containsKey(device)) {
                    dbMedia.paths.putAll(paths);
                    updatePathsStmt.setString(1, OBJECT_MAPPER.writeValueAsString(dbMedia.paths));
                    updatePathsStmt.setLong(2, dbMedia.id);
                    updatePathsStmt.executeUpdate();
                    log.info("File '{}' merged with other path: {}", dbMedia.name, toLogPath(paths));
                    updatedCount.incrementAndGet();
                }
            } else if (dbMedia.md5Hash == null) {
                //допишем в БД md5
                updateMd5Statement.setString(1, media.md5Hash());
                updateMd5Statement.setLong(2, dbMedia.id);
                updateMd5Statement.executeUpdate();
                log.info("File '{}' merged with MD5: {}", dbMedia.name, media.md5Hash());
                updatedCount.incrementAndGet();
            } else {
                //в базе есть хеш, в памяти нет, пропускаем
            }
        } else {
            //Разные файлы с одним названием. Считаем, что в базе устарел, запишем старый файл с другим именем
            //Новый запишем с актуальным именем
            String newName = "autorenamed_" + dbMedia.name;
            updateNameStmt.setString(1, newName);
            updateNameStmt.setLong(2, dbMedia.id);
            updateNameStmt.executeUpdate();
            updatedCount.incrementAndGet();
            log.info("File with ID: {}, name: '{}' in database in obsolete. Renamed to '{}'", dbMedia.id, dbMedia.name, newName);
            fillInsertStatement(media, insertStmt);
            insertStmt.execute();
            log.info("File '{}' inserted", media.name());
            insertedCount.incrementAndGet();
        }
    }

    @Nullable
    private static Media nextMedia(Iterator<Media> mediaIterator) {
        Media media;
        if (mediaIterator.hasNext()) {
            media = mediaIterator.next();
        } else {
            media = null;
        }
        return media;
    }

    @Nullable
    private static DbMedia nextFromDb(ResultSet resultSet) throws SQLException, JsonProcessingException {
        if (resultSet.next()) {
            return DbMedia.from(resultSet);
        } else {
            return null;
        }
    }

    private int insertRestMedia(Iterator<Media> mediaIterator, PreparedStatement insertStmt) throws SQLException, JsonProcessingException {
        int total = 0;
        int batch = 0;
        while (mediaIterator.hasNext()) {
            Media media = mediaIterator.next();
            fillInsertStatement(media, insertStmt);
            log.info("File '{}' inserted", media.name());
            insertStmt.addBatch();
            ++total;
            ++batch;
            if (batch > LIMIT) {
                insertStmt.executeBatch();
                batch = 0;
            }
        }
        insertStmt.executeBatch();
        return total;

    }

    private static class DbMedia {
        public final long id;
        public final String name;
        public final long fileSize;
        public final String md5Hash;
        public final Map<String, String> paths;

        public DbMedia(long id, String name, long fileSize, String md5Hash, Map<String, String> paths) {
            this.id = id;
            this.name = name;
            this.fileSize = fileSize;
            this.md5Hash = md5Hash;
            this.paths = paths;
        }

        public static DbMedia from(ResultSet resultSet) throws SQLException, JsonProcessingException {
            long id = resultSet.getLong("id");
            String name = resultSet.getString("name");
            long fileSize = resultSet.getLong("file_size");
            String md5Hash = resultSet.getString("hash_md5");
            String pathsAsString = resultSet.getString("paths");
            Map<String, String> map = OBJECT_MAPPER.readValue(pathsAsString, Map.class);
            return new DbMedia(id, name, fileSize, md5Hash, map);
        }

        @Override
        public String toString() {
            return "DbMedia{" +
                   "id=" + id +
                   ", name='" + name + '\'' +
                   ", fileSize=" + fileSize +
                   ", md5Hash='" + md5Hash + '\'' +
                   '}';
        }
    }

    private static void fillInsertStatement(Media media, PreparedStatement preparedStatement) throws SQLException, JsonProcessingException {
        preparedStatement.setString(1, media.name());
        preparedStatement.setTimestamp(2, media.createdAt());
        preparedStatement.setString(3, OBJECT_MAPPER.writeValueAsString(media.metadata()));
        preparedStatement.setString(4, OBJECT_MAPPER.writeValueAsString(media.paths()));
        preparedStatement.setString(5, media.type());
        preparedStatement.setLong(6, media.size());
        String md5Hash = media.md5Hash();
        if (md5Hash != null) {
            preparedStatement.setString(7, md5Hash);
        } else {
            preparedStatement.setNull(7, Types.VARCHAR);
        }
    }

    private static String toLogPath(Map<String, String> paths) {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : paths.entrySet()) {
            stringBuilder.append(entry.getKey()).append("->").append(entry.getValue());
        }
        return stringBuilder.toString();
    }


}
