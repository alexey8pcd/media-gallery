package ru.alejov.media.gallery;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.ds.PGSimpleDataSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class PgHelper {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String INSERT_SQL = ""
                                             + "INSERT INTO public.media(name, create_date, metadata, paths, type, file_size) "
                                             + "VALUES (?, ?, ?::jsonb, ?::jsonb, ?,?)";

    public static void saveToDatabase(String jdbcPropertiesFilePath, List<Media> mediaList) throws IOException, SQLException {
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
            try (PreparedStatement preparedStatement = connection.prepareStatement(INSERT_SQL)) {
                int count = 0;
                for (Media media : mediaList) {
                    preparedStatement.setString(1, media.name());
                    preparedStatement.setTimestamp(2, media.createdAt());
                    preparedStatement.setString(3, OBJECT_MAPPER.writeValueAsString(media.metadata()));
                    preparedStatement.setString(4, OBJECT_MAPPER.writeValueAsString(media.paths()));
                    preparedStatement.setString(5, media.type());
                    preparedStatement.setLong(6, media.size());
                    preparedStatement.addBatch();
                    ++count;
                    if (count > 100) {
                        preparedStatement.executeBatch();
                    }
                }
                preparedStatement.executeBatch();
            }
        }
    }
}
