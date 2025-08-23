package hu.webarticum.minibase.calcite.driver;

import java.util.Map;
import java.util.function.Supplier;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import hu.webarticum.minibase.storage.api.Schema;
import hu.webarticum.miniconnect.lang.ImmutableList;

public class MinibaseCalciteSchema extends AbstractSchema {
    
    private final Supplier<Schema> schemaSupplier;
    
    
    public MinibaseCalciteSchema(Schema schema) {
        this(() -> schema);
    }

    public MinibaseCalciteSchema(Supplier<Schema> schemaSupplier) {
        this.schemaSupplier = schemaSupplier;
    }
    
    
    @Override
    protected Map<String, Table> getTableMap() {
        ImmutableList<String> tableNames = schemaSupplier.get().tables().names();
        return tableNames.assign(n -> (Table) new MinibaseCalciteTable(() -> storageTable(n))).asMap();
    }
    
    private hu.webarticum.minibase.storage.api.Table storageTable(String name) {
        return schemaSupplier.get().tables().get(name);
    }
    
}
