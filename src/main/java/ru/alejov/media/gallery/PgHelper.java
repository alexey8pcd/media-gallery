package ru.alejov.media.gallery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("ConcatenationWithEmptyString")
public class PgHelper {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String INSERT_OR_SELECT_SQL = ""
                                                       + "WITH sel AS (\n"
                                                       + "     SELECT *\n"
                                                       + "       FROM media\n"
                                                       + "      WHERE name = ?),\n"
                                                       + "ins AS (\n"
                                                       + "     INSERT INTO media(name, create_date, metadata, paths, type, file_size, hash_md5, last_modify)\n"
                                                       + "     VALUES (?, ?, ?::jsonb, ?::jsonb, ?, ?, ?, ?)\n"
                                                       + "     ON CONFLICT (name)\n"
                                                       + "     DO NOTHING\n"
                                                       + "     RETURNING *)\n"
                                                       + "SELECT *,\n"
                                                       + "       false AS new_file\n"
                                                       + "  FROM sel\n"
                                                       + "UNION ALL\n"
                                                       + "SELECT *,\n"
                                                       + "       true AS new_file\n"
                                                       + "  FROM ins";
    private static final String INSERT_SQL = ""
                                             + "INSERT INTO media(name, create_date, metadata, paths, type, file_size, hash_md5, last_modify)\n"
                                             + "VALUES (?, ?, ?::jsonb, ?::jsonb, ?, ?, ?, ?)\n"
                                             + "ON CONFLICT (name)\n"
                                             + "DO NOTHING\n"
                                             + "RETURNING id";
    private static final String SELECT_BY_NAME = ""
                                                 + "SELECT *\n"
                                                 + "  FROM media\n"
                                                 + " WHERE name = ?";
    private static final String TEST_SELECT = ""
                                              + "SELECT name\n"
                                              + "  FROM media\n"
                                              + " LIMIT 1";
    private static final String SELECT_SQL = ""
                                             + "SELECT id,\n"
                                             + "       name,\n"
                                             + "       last_modify,\n"
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
    private static final int COMMIT_CHUNK = 10_000;
    private final Logger log;

    public PgHelper(Logger log) {
        this.log = log;
    }

    public void fillEmptyDatabase(String jdbcPropertiesFilePath, List<Media> mediaList) throws IOException, SQLException {
        log.info("Start fillEmptyDatabase");
        DataSource dataSource = getDataSource(jdbcPropertiesFilePath);
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
                        fillInsertStatement(media, insertStatement, media.getName());
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
    }

    public void mergeToDatabase(String jdbcPropertiesFilePath, List<Media> mediaList, String hostName, boolean detailLog) throws IOException, SQLException {
        log.info("Start mergeToDatabase");
        if (mediaList.isEmpty()) {
            return;
        }
        DataSource dataSource = getDataSource(jdbcPropertiesFilePath);
        try (DbProcessor dbProcessor = new DbProcessor(dataSource, log, detailLog)) {
            dbProcessor.process(mediaList, hostName);
        }
        log.info("Finish mergeToDatabase");
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

    private static class DbMedia {
        public final long id;
        public final String name;
        public final long fileSize;
        public final String md5Hash;
        public final Timestamp lastModify;
        public final Map<String, String> paths;

        public DbMedia(long id, String name, long fileSize, String md5Hash, Timestamp lastModify, Map<String, String> paths) {
            this.id = id;
            this.name = name;
            this.fileSize = fileSize;
            this.md5Hash = md5Hash;
            this.lastModify = lastModify;
            this.paths = paths;
        }

        public static DbMedia from(ResultSet resultSet) throws SQLException, JsonProcessingException {
            long id = resultSet.getLong("id");
            String name = resultSet.getString("name");
            long fileSize = resultSet.getLong("file_size");
            String md5Hash = resultSet.getString("hash_md5");
            String pathsAsString = resultSet.getString("paths");
            Timestamp lastModify = resultSet.getTimestamp("last_modify");
            Map<String, String> map = OBJECT_MAPPER.readValue(pathsAsString, Map.class);
            return new DbMedia(id, name, fileSize, md5Hash, lastModify, map);
        }

        @Override
        public String toString() {
            return "DbMedia{" +
                   "id=" + id +
                   ", name='" + name + '\'' +
                   ", fileSize=" + fileSize +
                   ", lastModify=" + lastModify +
                   ", md5Hash='" + md5Hash + '\'' +
                   '}';
        }
    }

    private static void fillInsertStatement(Media media,
                                            PreparedStatement insertStatement,
                                            String mediaName) throws SQLException, JsonProcessingException {
        insertStatement.setString(1, mediaName);
        insertStatement.setTimestamp(2, media.getCreatedAt());
        insertStatement.setString(3, OBJECT_MAPPER.writeValueAsString(media.getMetadata()));
        insertStatement.setString(4, OBJECT_MAPPER.writeValueAsString(media.getPaths()));
        insertStatement.setString(5, media.getType());
        insertStatement.setLong(6, media.getSize());
        String md5Hash = media.getMd5Hash();
        if (md5Hash != null) {
            insertStatement.setString(7, md5Hash);
        } else {
            insertStatement.setNull(7, Types.VARCHAR);
        }
        insertStatement.setTimestamp(8, media.getLastModify());
    }

    private static String toLogPath(Map<String, String> paths) {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : paths.entrySet()) {
            stringBuilder.append(entry.getKey()).append("->").append(entry.getValue());
        }
        return stringBuilder.toString();
    }

    private static DataSource getDataSource(String jdbcPropertiesFilePath) throws IOException {
        Properties properties = new Properties();
        try (InputStream inputStream = Files.newInputStream(Paths.get(jdbcPropertiesFilePath))) {
            properties.load(inputStream);
        }
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(properties.getProperty("pg.url"));
        dataSource.setUser(properties.getProperty("pg.user"));
        dataSource.setPassword(properties.getProperty("pg.password"));
        return dataSource;
    }

    private static class DbProcessor implements AutoCloseable {

        private final AtomicInteger insertedCount;
        private final AtomicInteger updatedCount;
        private final AtomicInteger existsHereCount;
        private final AtomicInteger existsElsewhereCount;
        private final DataSource dataSource;
        private final Logger log;
        private final boolean detailLog;

        private Connection connection;
        private PreparedStatement insertStmt;
        private PreparedStatement selectByNameStmt;
        private PreparedStatement insertOrSelectStmt;
        private PreparedStatement updateMd5Statement;
        private PreparedStatement updatePathsStmt;
        private int commitThreshold = PgHelper.COMMIT_CHUNK;

        public DbProcessor(DataSource dataSource, Logger log, boolean detailLog) {
            this.dataSource = dataSource;
            this.log = log;
            this.detailLog = detailLog;
            insertedCount = new AtomicInteger();
            updatedCount = new AtomicInteger();
            existsHereCount = new AtomicInteger();
            existsElsewhereCount = new AtomicInteger();
        }

        @Override
        public void close() {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public void process(List<Media> mediaList, String hostName) throws SQLException, JsonProcessingException {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            insertStmt = connection.prepareStatement(INSERT_SQL);
            selectByNameStmt = connection.prepareStatement(SELECT_BY_NAME);
            insertOrSelectStmt = connection.prepareStatement(INSERT_OR_SELECT_SQL);
            updateMd5Statement = connection.prepareStatement(UPDATE_MD5_SQL);
            updatePathsStmt = connection.prepareStatement(UPDATE_PATHS_SQL);

            PreparedStatement selectStatement = connection.prepareStatement(SELECT_SQL);
            selectStatement.setFetchSize(LIMIT);
            Iterator<Media> mediaIterator = mediaList.iterator();
            Media media = nextMedia(mediaIterator);
            try (ResultSet resultSet = selectStatement.executeQuery()) {
                DbMedia dbMedia = nextFromDb(resultSet);
                while (dbMedia != null && media != null) {
                    int compared = media.getName().compareTo(dbMedia.name);
                    if (compared == 0) {
                        mergeSameFiles(media, dbMedia, hostName);
                        dbMedia = nextFromDb(resultSet);
                        media = nextMedia(mediaIterator);
                    } else if (compared > 0) {
                        //Файл в памяти больше, чем в базе - возможно файл удалили. Но он может быть на другом устройстве.
                        //Ничего не делаем, выбираем следующий из базы
                        logFileNotExists(hostName, dbMedia);
                        dbMedia = nextFromDb(resultSet);
                    } else {
                        //Файл в базе больше, чем в памяти. Файл надо добавить.
                        DbMedia existed = tryInsert(media, media.getName());
                        if (existed == null) {
                            this.log.info("File '{}' inserted", getLocalPath(media));
                            insertedCount.incrementAndGet();
                        } else {
                            mergeSameFiles(media, existed, hostName);
                        }
                        //Выбираем следующий из памяти
                        media = nextMedia(mediaIterator);
                    }
                    if (insertedCount.get() + updatedCount.get() > commitThreshold) {
                        connection.commit();
                        commitThreshold += COMMIT_CHUNK;
                    }
                }
                if (dbMedia == null && media != null) {
                    //нет больше записей в БД
                    fillInsertStatement(media, insertStmt, media.getName());
                    insertStmt.execute();
                    insertedCount.incrementAndGet();
                    this.log.info("File '{}' inserted", getLocalPath(media));

                    int count = insertRestMedia(mediaIterator);
                    if (count > 0) {
                        insertedCount.addAndGet(count);
                        this.log.info("Inserted '{}' new files", count);
                    }
                } else if (dbMedia != null) {
                    //остальное есть в БД, пропускаем
                } else {
                    //ничего не осталось
                }
            }
            connection.commit();
            this.log.info("Finish process. Inserted rows: {}, updated rows: {}, exists here: {}, exists elsewhere: {}",
                          insertedCount, updatedCount, existsHereCount, existsElsewhereCount);
        }

        @Nullable
        private DbMedia tryInsert(Media media, String mediaName) throws SQLException, JsonProcessingException {
            fillInsertOrSelectStatement(media, mediaName);
            DbMedia existed = null;
            try (ResultSet resultSetLocal = insertOrSelectStmt.executeQuery()) {
                if (resultSetLocal.next()) {
                    boolean inserted = resultSetLocal.getBoolean("new_file");
                    if (!inserted) {
                        existed = DbMedia.from(resultSetLocal);
                    }
                }
            }
            return existed;
        }

        @Nullable
        private static DbMedia nextFromDb(ResultSet resultSet) throws SQLException, JsonProcessingException {
            if (resultSet.next()) {
                return DbMedia.from(resultSet);
            } else {
                return null;
            }
        }

        private void mergeSameFiles(Media media, DbMedia dbMedia, String hostName) throws SQLException, JsonProcessingException {
            //Записи одинаковые. Проверяем fileSize и md5
            if (media.getSize() == dbMedia.fileSize) {
                if (media.getLastModify().equals(dbMedia.lastModify) || Objects.equals(media.getMd5Hash(), dbMedia.md5Hash)) {
                    //записи абсолютно одинаковые, допишем путь, если это другое устройство
                    Map<String, String> paths = media.getPaths();
                    if (dbMedia.paths.keySet().containsAll(paths.keySet())) {
                        Path localPath = media.getLocalPath();
                        if (localPath != null) {
                            String oldLocalPath = dbMedia.paths.get(hostName);
                            Path absolutePath = localPath.toAbsolutePath();
                            if (absolutePath.toString().equals(oldLocalPath)) {
                                existsHereCount.incrementAndGet();
                                if (detailLog) {
                                    this.log.info("File '{}' already exists", media.getName());
                                }
                            } else {
                                //если это то же устройство, обновим путь при перемещении
                                dbMedia.paths.putAll(paths);
                                updatePathsStmt.setString(1, OBJECT_MAPPER.writeValueAsString(dbMedia.paths));
                                updatePathsStmt.setLong(2, dbMedia.id);
                                updatePathsStmt.executeUpdate();
                                updatedCount.incrementAndGet();
                                this.log.info("File '{}' relocated to new path: {}", dbMedia.name, absolutePath);
                            }
                        } else {
                            existsHereCount.incrementAndGet();
                            if (detailLog) {
                                this.log.info("File '{}' already exists", media.getName());
                            }
                        }
                    } else {
                        dbMedia.paths.putAll(paths);
                        updatePathsStmt.setString(1, OBJECT_MAPPER.writeValueAsString(dbMedia.paths));
                        updatePathsStmt.setLong(2, dbMedia.id);
                        updatePathsStmt.executeUpdate();
                        updatedCount.incrementAndGet();
                        this.log.info("File '{}' merged with other path: {}", dbMedia.name, toLogPath(paths));
                    }
                } else if (dbMedia.md5Hash == null) {
                    //допишем в БД md5
                    updateMd5Statement.setString(1, media.getMd5Hash());
                    updateMd5Statement.setLong(2, dbMedia.id);
                    updateMd5Statement.executeUpdate();
                    updatedCount.incrementAndGet();
                    this.log.info("File '{}' merged with MD5: {}", dbMedia.name, media.getMd5Hash());
                } else {
                    //в базе есть хеш, в памяти нет, пропускаем
                }
            } else {
                //Разные файлы с одним названием. Новый запишем с новым именем
                DbMedia dbMediaRenamed;
                String newName = "autorenamed_" + media.getName();
                int index = 1;
                do {
                    selectByNameStmt.setString(1, newName);
                    try (ResultSet resultSet = selectByNameStmt.executeQuery()) {
                        dbMediaRenamed = nextFromDb(resultSet);
                    }
                    if (dbMediaRenamed != null) {
                        //Сравниваем файлы. Если он один и тот же - пропускаем
                        boolean diff = false;
                        if (media.getSize() == dbMediaRenamed.fileSize) {
                            if (!media.getLastModify().equals(dbMediaRenamed.lastModify) && !Objects.equals(media.getMd5Hash(), dbMediaRenamed.md5Hash)) {
                                diff = true;
                            }
                        } else {
                           diff = true;
                        }
                        if (diff) {
                            newName = "autorenamed_" + index + "_" + dbMedia.name;
                            ++index;
                        } else {
                            String pathOnCurrentDevice = media.getPaths().get(hostName);
                            if (pathOnCurrentDevice == null) {
                                existsElsewhereCount.incrementAndGet();
                            } else {
                                existsHereCount.incrementAndGet();
                            }
                            if (detailLog) {
                                this.log.info("File '{}' already exists", newName);
                            }
                            return;
                        }
                    }
                } while (dbMediaRenamed != null);
                DbMedia existed = tryInsert(media, newName);
                if (existed == null) {
                    this.log.info("File '{}' inserted", newName);
                    insertedCount.incrementAndGet();
                }
            }
        }


        private int insertRestMedia(Iterator<Media> mediaIterator) throws SQLException, JsonProcessingException {
            int total = 0;
            int batch = 0;
            while (mediaIterator.hasNext()) {
                Media media = mediaIterator.next();
                fillInsertStatement(media, insertStmt, media.getName());
                log.info("File '{}' inserted", media.getName());
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

        private void fillInsertOrSelectStatement(Media media,
                                                 String mediaName) throws SQLException, JsonProcessingException {
            insertOrSelectStmt.setString(1, mediaName);
            insertOrSelectStmt.setString(2, mediaName);
            insertOrSelectStmt.setTimestamp(3, media.getCreatedAt());
            insertOrSelectStmt.setString(4, OBJECT_MAPPER.writeValueAsString(media.getMetadata()));
            insertOrSelectStmt.setString(5, OBJECT_MAPPER.writeValueAsString(media.getPaths()));
            insertOrSelectStmt.setString(6, media.getType());
            insertOrSelectStmt.setLong(7, media.getSize());
            String md5Hash = media.getMd5Hash();
            if (md5Hash != null) {
                insertOrSelectStmt.setString(8, md5Hash);
            } else {
                insertOrSelectStmt.setNull(8, Types.VARCHAR);
            }
            insertOrSelectStmt.setTimestamp(9, media.getLastModify());
        }

        private static String getLocalPath(Media media) {
            Path localPath = media.getLocalPath();
            String name = media.getName();
            String localPathStr = "UNKNOWN";
            if (localPath != null) {
                localPathStr = localPath.toString();
            }
            return name + " (" + localPathStr + ")";
        }

        private void logFileNotExists(String hostName, DbMedia dbMedia) {
            String path = dbMedia.paths.get(hostName);
            if (path == null) {
                existsElsewhereCount.incrementAndGet();
                if (detailLog) {
                    Set<String> devices = dbMedia.paths.keySet();
                    log.warn("File '{}' from another device(s): {}", dbMedia.name, devices);
                }
            }
        }
    }

}
