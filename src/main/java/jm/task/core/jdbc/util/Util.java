package jm.task.core.jdbc.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Util {
    protected static final Logger logger = Logger.getLogger(Util.class.getName());
    private static final String URL = "jdbc:mysql://localhost:3306/sys";
    private static final String USER = "root";
    private static final String PASSWORD = "3107945kK";


    public static Connection getConnection() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
            logger.log(Level.INFO, "Отлично, подключились успешно");
        } catch (SQLException e) {
            logger.log(Level.INFO, "Всё плохо " + e.getMessage());
        }
        return connection;
    }
}
