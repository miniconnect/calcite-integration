package hu.webarticum.holodb.calcite.launch;

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
        Connection connection = DriverManager.getConnection(
                "jdbc:calcite:model=/home/horvath/projects/sajat/holodb/git/calcite-integration/etc/calcite.yaml"
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
