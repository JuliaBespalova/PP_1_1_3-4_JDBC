package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

public class UserDaoJDBCImpl extends Util implements UserDao {

    private final Connection connection = getConnection();

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        String sql = """
                CREATE TABLE users (
                    id INT PRIMARY KEY AUTO_INCREMENT, 
                    name VARCHAR(255), 
                    lastName VARCHAR(255), 
                    age INT
                )
                """;

        try (Statement statement = connection.createStatement()) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet resultSet = metaData.getTables(
                    null,
                    null,
                    "users",
                    new String[] {"TABLE"}
            );
            if (!resultSet.next()) {
                statement.execute(sql);
                logger.log(Level.INFO, "Получилось создать таблицу");
            } else {
                logger.log(Level.INFO, "Не получилось создать таблицу! (таблица уже создана)");
            }
        } catch (SQLException e) {
            logger.log(Level.WARNING, "Ну ерунда в создании таблицы");
        }
    }

    public void dropUsersTable() {
        String sql = "DROP TABLE users";

        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
            logger.log(Level.INFO, "Таблица удалена");
        } catch (SQLException e) {
            logger.log(Level.WARNING, "Ну херня ничего не удалилось");
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        String sql = "INSERT INTO users (name, lastName, age) VALUES (?, ?, ?)";

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setInt(3, age);
            preparedStatement.executeUpdate();
            logger.log(Level.INFO, "Пользователь добавлен успешно.");
        } catch (SQLException e) {
            logger.log(Level.WARNING, "Пользователь не добавлен, переделывай");
        }
    }


    public void removeUserById(long id) {
        String sql = "DELETE FROM users WHERE id = ?";

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
            logger.log(Level.INFO, "Пользователь удален успешно.");
        } catch (SQLException e) {
            logger.log(Level.WARNING, "Пользователь не удален успешно.");
        }
    }

    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        String sql = "SELECT * FROM users";

        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong("id"));
                user.setName(resultSet.getString("name"));
                user.setLastName(resultSet.getString("lastName"));
                user.setAge(resultSet.getByte("age"));

                users.add(user);
                logger.log(Level.INFO, "Смотри кого добавили");
            }

        } catch (SQLException e) {
            logger.log(Level.WARNING, "Забудь ты не узнаешь список");
        }
        return users;
    }

    public void cleanUsersTable() {
        String sql = "TRUNCATE TABLE users";

        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
            logger.info("Таблица очищена успешно.");
        } catch (SQLException e) {
            logger.log(Level.WARNING, "Ошибка при очистке таблицы ");
        }
    }
}