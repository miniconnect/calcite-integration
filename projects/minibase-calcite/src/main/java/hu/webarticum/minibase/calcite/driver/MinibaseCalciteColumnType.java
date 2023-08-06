package hu.webarticum.minibase.calcite.driver;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl.JavaType;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.google.common.collect.ImmutableList;

import hu.webarticum.minibase.storage.api.ColumnDefinition;
import hu.webarticum.miniconnect.lang.ByteString;
import hu.webarticum.miniconnect.lang.ImmutableMap;
import hu.webarticum.miniconnect.lang.LargeInteger;

public class MinibaseCalciteColumnType extends JavaType { // NOSONAR: no need to override equals
    
    private final RelDataTypeFactoryImpl typeFactory;
    
    
    public MinibaseCalciteColumnType(ColumnDefinition columnDefinition, RelDataTypeFactoryImpl typeFactory) {
        typeFactory.super(columnDefinition.clazz(), columnDefinition.isNullable());
        this.typeFactory = typeFactory;
    }
    
    
    @Override
    public @Nullable RelDataType getKeyType() {
        Class<?> clazz = getJavaClass();

        // TODO
        if (clazz == ImmutableMap.class) {
            return typeFactory.createSqlType(SqlTypeName.ANY);
        }
        
        return super.getKeyType();
    }

    @Override
    public int getPrecision() {
        Class<?> clazz = getJavaClass();

        if (clazz == LargeInteger.class) {
            return PRECISION_NOT_SPECIFIED;
        }
        
        return super.getPrecision();
    }
    
    @Override
    public int getScale() {
        Class<?> clazz = getJavaClass();

        if (clazz == LargeInteger.class) {
            return 0;
        }
        
        return super.getScale();
    }
    
    @Override
    public @Nullable RelDataType getValueType() {
        Class<?> clazz = getJavaClass();

        // TODO
        if (clazz == ImmutableMap.class) {
            return typeFactory.createSqlType(SqlTypeName.ANY);
        }
        
        return super.getValueType();
    }

    @Override
    public SqlTypeName getSqlTypeName() {
        Class<?> clazz = getJavaClass();

        // TODO
        if (clazz == LargeInteger.class) {
            return SqlTypeName.DECIMAL; // FIXME otherwise it doesn't work in calcite avatica
        } else if (clazz == ByteString.class) {
            return SqlTypeName.BINARY;
        } else if (clazz == ImmutableList.class) {
            return SqlTypeName.ARRAY;
        } else if (clazz == ImmutableMap.class) {
            return SqlTypeName.MAP;
        }

        return super.getSqlTypeName();
    }

    @Override
    public RelDataTypeFamily getFamily() {
        // FIXME
        RelDataTypeFamily family = super.getFamily();
        return family == this ? getFamilyForSpecialType() : family;
    }

    private RelDataTypeFamily getFamilyForSpecialType() {
        Class<?> clazz = getJavaClass();
        
        // TODO
        if (clazz == LargeInteger.class) {
            return SqlTypeName.BIGINT.getFamily();
        }
        
        return this;
    }

}
