package hu.webarticum.holodb.calcite.launch;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import hu.webarticum.miniconnect.api.MiniSession;
import hu.webarticum.miniconnect.client.client.SqlRepl;
import hu.webarticum.miniconnect.client.repl.Repl;
import hu.webarticum.miniconnect.client.repl.ReplRunner;
import hu.webarticum.miniconnect.client.repl.RichReplRunner;
import hu.webarticum.miniconnect.jdbcadapter.JdbcAdapterSession;

public class ReplMain {

    public static void main(String[] args) throws SQLException {
        String modelConfigPath = args[0];
        File modelConfigFile = new File(modelConfigPath);
        if (!modelConfigFile.isFile() || !modelConfigFile.canRead()) {
            throw new IllegalArgumentException("Config file not found: " + modelConfigPath);
        }
        Connection connection = DriverManager.getConnection(
                "jdbc:calcite:model=" + modelConfigPath
                        + ";unquotedCasing=UNCHANGED"
                        + ";conformance=BABEL"
                ,
                "admin",
                "admin");
        MiniSession session = new JdbcAdapterSession(connection);
        Repl repl = new SqlRepl(session);
        ReplRunner replRunner = new RichReplRunner();
        replRunner.run(repl);
        connection.close();
    }
    
}
