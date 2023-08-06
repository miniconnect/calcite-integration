package hu.webarticum.minibase.calcite.driver;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import hu.webarticum.minibase.calcite.util.CounterIterator;
import hu.webarticum.minibase.storage.api.Column;
import hu.webarticum.minibase.storage.api.Table;
import hu.webarticum.miniconnect.lang.ImmutableList;
import hu.webarticum.miniconnect.util.IteratorAdapter;

public class MinibaseCalciteTable implements ModifiableTable, QueryableTable, ScannableTable {
    
    private final Table table;
    
    
    public MinibaseCalciteTable(Table table) {
        this.table = table;
    }

    
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return new MinibaseCalciteRowType(table, typeFactory);
    }

    @Override
    public Statistic getStatistic() {
        return Statistics.of(
                table.size().doubleValue(),
                Arrays.asList(ImmutableBitSet.fromBitSet(explorePrimaryKeyColumns())));
    }

    private BitSet explorePrimaryKeyColumns() {
        ImmutableList<Column> columns = table.columns().resources();
        int columnCount = columns.size();
        BitSet result = new BitSet(columnCount);
        for (int i = 0; i < columnCount; i++) {
            if (columns.get(i).definition().isAutoIncremented()) {
                result.set(i);
            }
        }
        return result;
    }

    @Override
    public TableType getJdbcTableType() {
        return TableType.TABLE;
    }

    @Override
    public boolean isRolledUp(String column) {
        return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(
            String column, SqlCall call, @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config) {
        return true;
    }

    @Override
    public @Nullable Collection<?> getModifiableCollection() {
        return null;
    }

    @Override
    public TableModify toModificationRel(
            RelOptCluster cluster,
            RelOptTable table,
            CatalogReader catalogReader,
            RelNode child,
            Operation operation,
            @Nullable List<String> updateColumnList,
            @Nullable List<RexNode> sourceExpressionList,
            boolean flattened) {
        
        // TODO Auto-generated method stub
        return null;
        
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        return new MinibaseCalciteTableQueryable<>(queryProvider, schema, this, tableName);
    }

    @Override
    public Type getElementType() {
        return Object[].class;
    }

    @Override
    public Expression getExpression(SchemaPlus schema, String tableName, @SuppressWarnings("rawtypes") Class clazz) {
        return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
        
    }
    
    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
        return scan();
    }

    public Enumerable<@Nullable Object[]> scan() {
        return Linq4j.asEnumerable(this::createScannerIterator);
    }
    
    private Iterator<Object[]> createScannerIterator() {
        return new IteratorAdapter<>(
                new CounterIterator(table.size()),
                i -> table.row(i).getAll().toArray());
    }

}
