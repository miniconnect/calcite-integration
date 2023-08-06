package hu.webarticum.minibase.calcite.driver;

import java.nio.charset.Charset;
import java.util.List;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import hu.webarticum.minibase.storage.api.Column;
import hu.webarticum.minibase.storage.api.ColumnDefinition;
import hu.webarticum.minibase.storage.api.Table;
import hu.webarticum.miniconnect.lang.ImmutableList;

public class MinibaseCalciteRowType implements RelDataType, RelDataTypeFamily {
    
    private final Table table;
    
    private final RelDataTypeFactory typeFactory;
    
    private final ImmutableList<RelDataTypeField> fields;
    
    
    public MinibaseCalciteRowType(Table table, RelDataTypeFactory typeFactory) {
        this.table = table;
        this.typeFactory = typeFactory;
        this.fields = table.columns().resources().map((i, r) -> createField(i, r, typeFactory));
    }

    private static RelDataTypeField createField(int index, Column column, RelDataTypeFactory typeFactory) {
        RelDataType dataType = extractDataType(column.definition(), typeFactory);
        return new RelDataTypeFieldImpl(column.name(), index, dataType);
    }
    
    private static RelDataType extractDataType(ColumnDefinition columnDefinition, RelDataTypeFactory typeFactory) {
        RelDataType defaultDataType = typeFactory.createJavaType(columnDefinition.clazz());
        if (defaultDataType.getSqlTypeName() != SqlTypeName.OTHER) {
            return typeFactory.createTypeWithNullability(defaultDataType, columnDefinition.isNullable());
        }
        
        RelDataTypeFactoryImpl factory;
        if (typeFactory instanceof RelDataTypeFactoryImpl) {
            factory = (RelDataTypeFactoryImpl) typeFactory;
        } else {
            factory = new JavaTypeFactoryImpl(typeFactory.getTypeSystem());
        }
        return new MinibaseCalciteColumnType(columnDefinition, factory);
    }
    

    @Override
    public boolean isStruct() {
        return true;
    }

    @Override
    public List<RelDataTypeField> getFieldList() {
        return fields.asList();
    }

    @Override
    public List<String> getFieldNames() {
        return table.columns().names().asList();
    }

    @Override
    public int getFieldCount() {
        return table.columns().names().size();
    }

    @Override
    public StructKind getStructKind() {
        return StructKind.FULLY_QUALIFIED;
    }

    @Override
    public @Nullable RelDataTypeField getField(String fieldName, boolean caseSensitive, boolean elideRecord) {
        for (RelDataTypeField field : fields) {
            if (areEqual(field.getName(), fieldName, caseSensitive)) {
                return field;
            }
        }

        throw new IllegalArgumentException(
                "No such column (case sensitive: " + caseSensitive + "): '" + fieldName + "'");
    }
    
    private boolean areEqual(String str1, String str2, boolean caseSensitive) {
        if (caseSensitive) {
            return str1.equals(str2);
        } else {
            return str1.equalsIgnoreCase(str2);
        }
    }
    
    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public @Nullable RelDataType getComponentType() {
        return null;
    }

    @Override
    public @Nullable RelDataType getKeyType() {
        return typeFactory.createJavaType(String.class);
    }

    @Override
    public @Nullable RelDataType getValueType() {
        return null;
    }

    @Override
    public @Nullable Charset getCharset() {
        return null;
    }

    @Override
    public @Nullable SqlCollation getCollation() {
        return null;
    }

    @Override
    public @Nullable SqlIntervalQualifier getIntervalQualifier() {
        return null;
    }

    @Override
    public int getPrecision() {
        return 0;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public SqlTypeName getSqlTypeName() {
        return SqlTypeName.STRUCTURED;
    }

    @Override
    public @Nullable SqlIdentifier getSqlIdentifier() {
        return SqlIdentifier.STAR;
    }

    @Override
    public String getFullTypeString() {
        return null;
    }

    @Override
    public RelDataTypeFamily getFamily() {
        return this;
    }

    @Override
    public RelDataTypePrecedenceList getPrecedenceList() {
        return new SqlTypeExplicitPrecedenceList(ImmutableList.of());
    }

    @Override
    public RelDataTypeComparability getComparability() {
        return RelDataTypeComparability.ALL;
    }

    @Override
    public boolean isDynamicStruct() {
        return false;
    }

}
