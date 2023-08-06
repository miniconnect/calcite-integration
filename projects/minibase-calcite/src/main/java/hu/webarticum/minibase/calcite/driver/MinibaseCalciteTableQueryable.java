package hu.webarticum.minibase.calcite.driver;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

public class MinibaseCalciteTableQueryable<T> extends AbstractTableQueryable<T> {

    public MinibaseCalciteTableQueryable(
            QueryProvider queryProvider, SchemaPlus schema, MinibaseCalciteTable table, String tableName) {
        super(queryProvider, schema, table, tableName);
    }

    
    @Override
    @SuppressWarnings("unchecked")
    public Enumerator<T> enumerator() {
        return (Enumerator<T>) ((MinibaseCalciteTable) table).scan().enumerator();
    }

}
