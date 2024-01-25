package marmot.spark.optor.reducer;

import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import marmot.Column;
import marmot.RecordSchema;
import marmot.io.MultiColumnKey;
import marmot.plan.Group;
import marmot.spark.RecordLite;
import scala.Tuple2;
import utils.CSV;
import utils.func.Tuple;
import utils.func.Tuple4;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GroupUtils {
	private GroupUtils() {
		throw new AssertionError("Should not be called: " + getClass());
	}
	
	public static Column[] getColumns(RecordSchema schema, String colExpr) {
		return CSV.parseCsv(colExpr)
					.map(schema::getColumn)
					.toArray(Column.class);
	}
	
	public static int[] getColumnIndexes(RecordSchema schema, String colExpr) {
		return toMultiColumnKey(schema, colExpr)
					.streamKeyColumns()
					.mapToInt(kc -> schema.getColumn(kc.name()).ordinal())
					.toArray();
	}
	
	public static MultiColumnKey toMultiColumnKey(RecordSchema schema, String colExpr) {
		return MultiColumnKey.fromString(colExpr);
	}
	
	public static int[] getGroupColumIndexes(Group group, RecordSchema schema) {
		return CSV.parseCsv(group.keys())
					.mapToInt(n -> schema.getColumn(n).ordinal())
					.toArray();
	}
	
	public static Tuple4<int[],int[],int[],int[]> splitColumnIndexes(Group group, RecordSchema schema) {
		int[] keyIdxes = GroupUtils.getColumnIndexes(schema, group.keys());
		int[] tagIdxes = GroupUtils.getColumnIndexes(schema, group.tags().getOrElse(""));
		int[] orderIdxes = GroupUtils.getColumnIndexes(schema, group.orderBy().getOrElse(""));
		int[] valueIdxes = GroupUtils.getValueColumnIndexes(schema, group);
		
		return Tuple.of(keyIdxes, tagIdxes, orderIdxes, valueIdxes);
	}
	
	public static JavaPairRDD<GroupRecordLite,RecordLite> split(Group group, RecordSchema schema,
																JavaRDD<RecordLite> jrdd) {
		int[] keyIdxes = GroupUtils.getColumnIndexes(schema, group.keys());
		int[] tagIdxes = GroupUtils.getColumnIndexes(schema, group.tags().getOrElse(""));
		int[] orderIdxes = GroupUtils.getColumnIndexes(schema, group.orderBy().getOrElse(""));
		int[] valueIdxes = GroupUtils.getValueColumnIndexes(schema, group);
		
		return jrdd.mapToPair(rec -> {
			GroupRecordLite key = new GroupRecordLite(rec.project(keyIdxes), rec.project(tagIdxes),
														rec.project(orderIdxes));
			return new Tuple2<>(key, rec.project(valueIdxes));
		});
	}
	
	public static int[] getValueColumnIndexes(RecordSchema schema, Group group) {
		Set<String> keyCols = CSV.parseCsv(group.keys())
									.concatWith(CSV.parseCsv(group.tags().getOrElse("")))
									.toSet();
		return schema.streamColumns()
					.filter(col -> !keyCols.contains(col.name()))
					.mapToInt(Column::ordinal)
					.toArray();
	}
}
