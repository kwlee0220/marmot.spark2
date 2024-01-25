package marmot.spark.optor.reducer;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.stddev;
import static org.apache.spark.sql.functions.sum;

import java.util.List;

import org.apache.spark.sql.Column;

import utils.stream.FStream;

import marmot.RecordSchema;
import marmot.optor.AggregateFunction;
import marmot.optor.AggregateType;
import marmot.spark.type.ConcatStrUDAF;
import marmot.spark.type.ConvexHullUDAF;
import marmot.spark.type.EnvelopeUDAF;
import marmot.spark.type.UnionGeomUDAF;
import marmot.type.DataType;
import marmot.type.TypeCode;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AggregateFunctions {
	private AggregateFunctions() {
		throw new AssertionError();
	}
	
	public static ValueAggregator<?> toValueAggregator(AggregateFunction func) {
		switch ( func.m_type ) {
			case COUNT:
				return ValueAggregator.COUNT();
			case MAX:
				return ValueAggregator.MAX(func.m_aggrColumn).as(func.m_resultColumn);
			case MIN:
				return ValueAggregator.MIN(func.m_aggrColumn).as(func.m_resultColumn);
			case SUM:
				return ValueAggregator.SUM(func.m_aggrColumn).as(func.m_resultColumn);
			case AVG:
				return ValueAggregator.AVG(func.m_aggrColumn).as(func.m_resultColumn);
			case ENVELOPE:
				return ValueAggregator.ENVELOPE(func.m_aggrColumn).as(func.m_resultColumn);
			case UNION_GEOM:
				return ValueAggregator.UNION_GEOM(func.m_aggrColumn).as(func.m_resultColumn);
			case CONCAT_STR:
				return ValueAggregator.CONCAT_STR(func.m_aggrColumn, func.m_args).as(func.m_resultColumn);
			case CONVEX_HULL:
				return ValueAggregator.CONVEX_HULL(func.m_aggrColumn).as(func.m_resultColumn);
			default:
				throw new IllegalArgumentException("unregistered AggregateFunction: " + func);
		}
	}
	
	public static List<Column> toAggrColumnList(List<AggregateFunction> funcs) {
		return FStream.from(funcs).map(f -> toAggrColumnExpr(f)).toList();
	}
	
	public static Column toAggrColumnExpr(AggregateFunction aggr) {
		Column colExpr;
		switch ( aggr.m_type ) {
			case COUNT:
				colExpr = count("*");
				break;
			case AVG:
				colExpr = avg(col(aggr.m_aggrColumn));
				break;
			case STDDEV:
				colExpr = stddev(col(aggr.m_aggrColumn));
				break;
			case SUM:
				colExpr = sum(col(aggr.m_aggrColumn));
				break;
			case MAX:
				colExpr = max(aggr.m_aggrColumn);
				break;
			case MIN:
				colExpr = min(aggr.m_aggrColumn);
				break;
			case ENVELOPE:
				colExpr = new EnvelopeUDAF().apply(col(aggr.m_aggrColumn));
				break;
			case CONVEX_HULL:
				colExpr = new ConvexHullUDAF().apply(col(aggr.m_aggrColumn));
				break;
			case CONCAT_STR:
				colExpr = new ConcatStrUDAF().apply(col(aggr.m_aggrColumn));
				break;
			case UNION_GEOM:
				colExpr = new UnionGeomUDAF().apply(col(aggr.m_aggrColumn));
				break;
			default:
				throw new IllegalArgumentException("unsupported AggregateFunction: " + aggr);
		}
		
		return colExpr.as(aggr.m_resultColumn);
	}
	
	public static RecordSchema calcOutputRecordSchema(RecordSchema schema,
														List<AggregateFunction> aggrs) {
		return FStream.from(aggrs)
						.map(a -> new marmot.Column(a.m_resultColumn, calcOutputDataType(schema, a)))
						.fold(RecordSchema.builder(),  (b,c) -> b.addColumn(c))
						.build();
	}
	
	public static DataType calcOutputDataType(RecordSchema schema, AggregateFunction aggrFunc) {
		if ( aggrFunc.m_type == AggregateType.COUNT ) {
			return DataType.LONG;
		}
		
		DataType type = schema.getColumn(aggrFunc.m_aggrColumn).type();
		TypeCode tc = type.getTypeCode();
		
		switch ( aggrFunc.m_type ) {
			case SUM:
				switch ( tc ) {
					case BYTE: case SHORT: case INT: case LONG:
						return DataType.LONG;
					case FLOAT: case DOUBLE:
						return DataType.DOUBLE;
					default:
						throw new IllegalArgumentException("Invalid argument type: AggregateFunction=" + aggrFunc);
				}
			case MAX:
			case MIN:
				return type;
			case AVG:
			case STDDEV:
				return DataType.DOUBLE;
			case CONVEX_HULL:
				return DataType.POLYGON;
			case ENVELOPE:
				return DataType.ENVELOPE;
			case CONCAT_STR:
				return DataType.STRING;
			default:
				throw new IllegalArgumentException("unknown AggregationFunction: " + aggrFunc);
		}
	}
}
