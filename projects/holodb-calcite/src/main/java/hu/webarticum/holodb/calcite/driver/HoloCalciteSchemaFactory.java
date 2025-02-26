package hu.webarticum.holodb.calcite.driver;

import java.io.File;
import java.util.Map;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import hu.webarticum.holodb.config.HoloConfig;
import hu.webarticum.holodb.app.factory.ConfigLoader;
import hu.webarticum.holodb.app.factory.EngineBuilder;
import hu.webarticum.minibase.calcite.driver.MinibaseCalciteSchema;
import hu.webarticum.minibase.engine.impl.SimpleEngine;
import hu.webarticum.minibase.storage.api.NamedResourceStore;
import hu.webarticum.minibase.storage.api.StorageAccess;

public class HoloCalciteSchemaFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        Object configFilePathObject = operand.get("configFile");
        if (!(configFilePathObject instanceof String)) {
            throw new IllegalArgumentException("Parameter configFile is missing or not a string");
        }
        
        File configFile = new File((String) configFilePathObject);
        if (!configFile.canRead()) {
            throw new IllegalArgumentException("Configuration file is not readable");
        }

        Object schemaNameObject = operand.get("schemaName");
        if (!(schemaNameObject instanceof String)) {
            throw new IllegalArgumentException("Parameter schemaNameObject is missing or not a string");
        }
        String schemaName = (String) schemaNameObject;
        
        HoloConfig config = new ConfigLoader(configFile).load();
        StorageAccess storageAccess = ((SimpleEngine) EngineBuilder.ofConfig(config).build()).storageAccess();
        
        NamedResourceStore<hu.webarticum.minibase.storage.api.Schema> schemas = storageAccess.schemas();
        if (!schemas.contains(schemaName)) {
            throw new IllegalArgumentException("Schema not found: '" + schemaName + "'");
        }
        
        hu.webarticum.minibase.storage.api.Schema schema = schemas.get(schemaName);
        return new MinibaseCalciteSchema(schema);
    }

}
