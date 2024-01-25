package marmot.spark.optor.reducer;

import java.io.Serializable;

import marmot.Column;
import marmot.RecordSchema;
import marmot.optor.AggregateType;
import marmot.spark.RecordLite;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface ValueAggregator<T extends ValueAggregator<T>> extends Serializable {
	public AggregateType getAggregateType();
	/**
	 * 집계 대상 컬럼 이름을 반환한다.
	 * 
	 * @return	컬럼 이름
	 */
	public String getInputColumnName();
	
	/**
	 * 집계 결과 출력 컬럼 이름을 반환한다.
	 * 
	 * @return	출력 컬럼 이름
	 */
	public Column getOutputColumn();
	public T as(String outColName);
	public DataType getOutputType(DataType inputType);

	public void initialize(int pos, RecordSchema inputSchema);
	
	public Object getZeroValue();
	public void collect(RecordLite input, RecordLite aggregate);
	public void combine(RecordLite accum1, RecordLite accum2);
	public void toFinalValue(RecordLite accum);
	
	public static Count COUNT() {
		return new Count();
	}
	
	public static Max MAX(String col) {
		return new Max(col);
	}
	
	public static Min MIN(String col) {
		return new Min(col);
	}
	
	public static Sum SUM(String col) {
		return new Sum(col);
	}
	
	public static Avg AVG(String col) {
		return new Avg(col);
	}
	
	public static CollectEnvelope ENVELOPE(String col) {
		return new CollectEnvelope(col);
	}
	
	public static ConvexHull CONVEX_HULL(String col) {
		return new ConvexHull(col);
	}
	
	public static UnionGeom UNION_GEOM(String col) {
		return new UnionGeom(col);
	}
	
	public static ConcatString CONCAT_STR(String col, String delim) {
		return new ConcatString(col, delim);
	}
}
