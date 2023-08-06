package hu.webarticum.minibase.calcite.driver;

import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import hu.webarticum.minibase.storage.api.NamedResourceStore;
import hu.webarticum.minibase.storage.api.Schema;

public class MinibaseCalciteSchema extends AbstractSchema {
    
    private final Schema schema;
    
    
    public MinibaseCalciteSchema(Schema schema) {
        this.schema = schema;
    }
    
    
    @Override
    protected Map<String, Table> getTableMap() {
        NamedResourceStore<hu.webarticum.minibase.storage.api.Table> tables = schema.tables();
        return schema.tables().names().assign(n -> (Table) new MinibaseCalciteTable(tables.get(n))).asMap();
    }
    
}
