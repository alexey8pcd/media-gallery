package ru.alejov.media.gallery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.ds.PGSimpleDataSource;

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
import java.util.Objects;
import java.util.Properties;

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
                                             + "       file_size,\n"
                                             + "       hash_md5\n"
                                             + "  FROM media\n"
                                             + " ORDER BY name";
    private static final String UPDATE_MD5_SQL = ""
                                                 + "UPDATE media\n"
                                                 + "   SET hash_md5 = ?\n"
                                                 + " WHERE id = ?";
    private static final String UPDATE_NAME_SQL = ""
                                                  + "UPDATE media\n"
                                                  + "   SET name = ?\n"
                                                  + " WHERE id = ?";
    private static final String UPSERT_SQL = ""
                                             + "INSERT INTO media(name, create_date, metadata, paths, type, file_size, hash_md5)\n"
                                             + "VALUES (?, ?, ?::jsonb, ?::jsonb, ?, ?, ?)\n"
                                             + "ON CONFLICT (name)\n"
                                             + "DO UPDATE\n"
                                             + "SET metadata = media.metadata || excluded.metadata,\n"
                                             + "    paths = media.paths || excluded.paths,\n"
                                             + "    hash_md5 = COALESCE(excluded.hash_md5, media.hash_md5)\n"
                                             + "WHERE media.file_size = excluded.file_size\n"
                                             + "  AND (media.hash_md5 IS NULL\n"
                                             + "   OR excluded.hash_md5 IS NULL\n"
                                             + "   OR media.hash_md5 = excluded.hash_md5)";
    private static final int LIMIT = 500;


    public static boolean fillEmptyDatabase(String jdbcPropertiesFilePath, List<Media> mediaList) throws IOException, SQLException {
        Properties properties = new Properties();
        try (InputStream inputStream = Files.newInputStream(Paths.get(jdbcPropertiesFilePath))) {
            properties.load(inputStream);
        }
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(properties.getProperty("pg.url"));
        dataSource.setUser(properties.getProperty("pg.user"));
        dataSource.setPassword(properties.getProperty("pg.password"));
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(true);
            try (PreparedStatement insertStatement = connection.prepareStatement(INSERT_SQL);
                 PreparedStatement testStatement = connection.prepareStatement(TEST_SELECT)) {
                boolean empty;
                try (ResultSet resultSet = testStatement.executeQuery()) {
                    empty = !resultSet.next();
                }
                if (empty) {
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
                return empty;
            }
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public static int mergeToDatabase(String jdbcPropertiesFilePath, List<Media> mediaList) throws IOException, SQLException {
        if (mediaList.isEmpty()) {
            return 0;
        }
        Properties properties = new Properties();
        try (InputStream inputStream = Files.newInputStream(Paths.get(jdbcPropertiesFilePath))) {
            properties.load(inputStream);
        }
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(properties.getProperty("pg.url"));
        dataSource.setUser(properties.getProperty("pg.user"));
        dataSource.setPassword(properties.getProperty("pg.password"));
        int insertedCount = 0;
        int updatedCount = 0;
        int commitChunk = 10_000;
        int commitThreshold = commitChunk;
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement selectStatement = connection.prepareStatement(SELECT_SQL);
                 PreparedStatement updateMd5Statement = connection.prepareStatement(UPDATE_MD5_SQL);
                 PreparedStatement insertStmt = connection.prepareStatement(INSERT_SQL);
                 PreparedStatement updateNameStmt = connection.prepareStatement(UPDATE_NAME_SQL)) {
                selectStatement.setFetchSize(LIMIT);
                Iterator<Media> mediaIterator = mediaList.iterator();
                Media media = nextMedia(mediaIterator);
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    DbMedia dbMedia = nextFromDb(resultSet);
                    while (dbMedia != null && media != null) {
                        int compared = media.name().compareTo(dbMedia.name);
                        if (compared == 0) {
                            //Записи одинаковые. Проверяем fileSize и md5
                            if (media.size() == dbMedia.fileSize) {
                                if (Objects.equals(media.md5Hash(), dbMedia.md5Hash)) {
                                    //записи абсолютно одинаковые, пропускаем
                                } else if (dbMedia.md5Hash == null) {
                                    //допишем в БД md5
                                    updateMd5Statement.setString(1, media.md5Hash());
                                    updateMd5Statement.setLong(2, dbMedia.id);
                                    updateMd5Statement.executeUpdate();
                                    ++updatedCount;
                                } else {
                                    //в базе есть хеш, в памяти нет, пропускаем
                                }
                            } else {
                                //Разные файлы с одним названием. Считаем, что в базе устарел, запишем старый файл с другим именем
                                //Новый запишем с актуальным именем
                                updateNameStmt.setString(1, "autorenamed_" + dbMedia.name);
                                updateNameStmt.setLong(2, dbMedia.id);
                                updateNameStmt.executeUpdate();
                                ++updatedCount;
                                fillInsertStatement(media, insertStmt);
                                insertStmt.executeUpdate();
                                ++insertedCount;
                            }
                            if (resultSet.next()) {
                                dbMedia = DbMedia.from(resultSet);
                            } else {
                                dbMedia = null;
                            }
                            if (mediaIterator.hasNext()) {
                                media = mediaIterator.next();
                            } else {
                                media = null;
                            }
                        } else if (compared > 0) {
                            //Файл в памяти больше, чем в базе - возможно файл удалили. Но он может быть на другом устройстве.
                            //Ничего не делаем, выбираем следующий из базы
                            dbMedia = nextFromDb(resultSet);
                        } else {
                            //Файл в базе больше, чем в памяти. Файл надо добавить.
                            fillInsertStatement(media, insertStmt);
                            insertStmt.executeUpdate();
                            ++insertedCount;
                            //Выбираем следующий из памяти
                            media = nextMedia(mediaIterator);
                        }
                        if (insertedCount + updatedCount > commitThreshold) {
                            connection.commit();
                            commitThreshold += commitChunk;
                        }
                    }
                    if (dbMedia == null && media != null) {
                        //не больше записей в БД
                        fillInsertStatement(media, insertStmt);
                        insertStmt.executeUpdate();
                        ++insertedCount;
                        insertedCount += insertRestMedia(mediaIterator, insertStmt);
                    } else if (dbMedia != null) {
                        //остальное есть в БД, пропускаем
                    } else {
                        //ничего не осталось
                    }
                }
                connection.commit();
            }
        }
        return insertedCount + updatedCount;
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
    private static DbMedia nextFromDb(ResultSet resultSet) throws SQLException {
        if (resultSet.next()) {
            return DbMedia.from(resultSet);
        } else {
            return null;
        }
    }

    private static int insertRestMedia(Iterator<Media> mediaIterator, PreparedStatement insertStmt) throws SQLException, JsonProcessingException {
        int total = 0;
        int batch = 0;
        while (mediaIterator.hasNext()) {
            Media media = mediaIterator.next();
            fillInsertStatement(media, insertStmt);
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

        public DbMedia(long id, String name, long fileSize, String md5Hash) {
            this.id = id;
            this.name = name;
            this.fileSize = fileSize;
            this.md5Hash = md5Hash;
        }

        public static DbMedia from(ResultSet resultSet) throws SQLException {
            long id = resultSet.getLong("id");
            String name = resultSet.getString("name");
            long fileSize = resultSet.getLong("file_size");
            String md5Hash = resultSet.getString("hash_md5");
            return new DbMedia(id, name, fileSize, md5Hash);
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

    /*
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
                    int count = preparedStatement.executeUpdate();
                    if (count == 0) {
                        //файл уже существует, ничего не делаем
                    } else {
                        //если файл новый - ничего не делаем
                        try (ResultSet resultSet = preparedStatement.getResultSet()) {

                        }
                    }
     */

}
