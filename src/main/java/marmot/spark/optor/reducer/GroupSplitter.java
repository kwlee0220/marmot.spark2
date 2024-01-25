package marmot.spark.optor.reducer;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import marmot.RecordSchema;
import marmot.io.MultiColumnKey;
import marmot.optor.KeyColumn;
import marmot.plan.Group;
import marmot.spark.RecordLite;
import scala.Tuple2;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GroupSplitter implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final RecordSchema m_schema;
	private final RecordSchema m_keySchema;
	private final RecordSchema m_valueSchema;
	
	private final MultiColumnKey m_keyCols;
	private final int[] m_keyColIdxes;
	private final MultiColumnKey m_tagCols;
	private final int[] m_tagColIdxes;
	private final MultiColumnKey m_orderCols;
	private final int[] m_orderColIdxes;
	private final int[] m_valueColIdxes;
	
	public static GroupSplitter on(Group group, RecordSchema schema) {
		MultiColumnKey keyCols = MultiColumnKey.fromString(group.keys());
		MultiColumnKey tagCols = group.tags().map(MultiColumnKey::fromString)
										.getOrElse(MultiColumnKey.EMPTY);
		MultiColumnKey orderCols = group.orderBy().map(MultiColumnKey::fromString)
											.getOrElse(MultiColumnKey.EMPTY);
		
		return new GroupSplitter(schema, keyCols, tagCols, orderCols);
	}
	
	public static GroupSplitter withoutOrder(Group group, RecordSchema schema) {
		MultiColumnKey keyCols = MultiColumnKey.fromString(group.keys());
		MultiColumnKey tagCols = group.tags().map(MultiColumnKey::fromString)
										.getOrElse(MultiColumnKey.EMPTY);
		
		return new GroupSplitter(schema, keyCols, tagCols, MultiColumnKey.EMPTY);
	}
	
	private GroupSplitter(RecordSchema schema, MultiColumnKey keyCols, MultiColumnKey tagCols,
							MultiColumnKey orderCols) {
		m_schema = schema;
		
		m_keyCols = keyCols;
		m_keyColIdxes = getColumnIndexes(m_keyCols, schema);
		
		m_tagCols = tagCols;
		m_tagColIdxes = getColumnIndexes(m_tagCols, schema);
		
		m_orderCols = orderCols;
		m_orderColIdxes = getColumnIndexes(m_orderCols, schema);
		
		MultiColumnKey fullKeyCols = MultiColumnKey.concat(keyCols, tagCols);
		int[] keyColIdxes = fullKeyCols.streamKeyColumns()
										.mapToInt(kc -> schema.getColumn(kc.name()).ordinal())
										.toArray();
		m_keySchema = schema.project(keyColIdxes);
		
		m_valueColIdxes = MultiColumnKey.concat(keyCols, tagCols)
										.complement(schema).streamKeyColumns()
										.mapToInt(kc -> schema.getColumn(kc.name()).ordinal())
										.toArray();
		m_valueSchema = schema.project(m_valueColIdxes);
	}
	
	public RecordSchema getGroupSchema() {
		return m_keySchema;
	}
	
	public RecordSchema getValueSchema() {
		return m_valueSchema;
	}
	
	public MultiColumnKey keyColumnsExpr() {
		return m_keyCols;
	}
	
	public MultiColumnKey tagColumnsExpr() {
		return m_tagCols;
	}
	
	public MultiColumnKey orderColumnsExpr() {
		return m_orderCols;
	}
	
	public JavaPairRDD<GroupRecordLite,RecordLite> split(JavaRDD<RecordLite> jrdd) {
		return jrdd.mapToPair(rec -> {
			RecordLite keys = rec.project(m_keyColIdxes);
			RecordLite tags = rec.project(m_tagColIdxes);
			RecordLite orders = rec.project(m_orderColIdxes);
			GroupRecordLite key = new GroupRecordLite(keys, tags, orders);
			return new Tuple2<>(key, rec.project(m_valueColIdxes));
		});
	}
	
	private static int[] getColumnIndexes(MultiColumnKey mcKey, RecordSchema schema) {
		return mcKey.streamKeyColumns()
					.map(KeyColumn::name)
					.mapToInt(n -> schema.getColumn(n).ordinal())
					.toArray();
	}
}
