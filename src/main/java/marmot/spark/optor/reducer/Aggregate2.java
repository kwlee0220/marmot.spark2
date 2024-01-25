package marmot.spark.optor.reducer;

import static marmot.spark.optor.reducer.AggregateFunctions.calcOutputDataType;

import java.io.Serializable;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import marmot.RecordSchema;
import marmot.optor.AggregateFunction;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Aggregate2 implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final AggregateFunction[] m_aggrs;
	
	public Aggregate2(AggregateFunction... aggrs) {
		m_aggrs = aggrs;
	}
	
	public RecordSchema calcOutputRecordSchema(RecordSchema inputSchema) {
		return FStream.of(m_aggrs)
						.map(a -> new marmot.Column(a.m_resultColumn,
													calcOutputDataType(inputSchema, a)))
						.fold(RecordSchema.builder(),  (b,c) -> b.addColumn(c))
						.build();
	}

	public Dataset<Row> apply(Dataset<Row> input) {
		Column[] colsExpr = FStream.of(m_aggrs)
									.map(AggregateFunctions::toAggrColumnExpr)
									.toArray(Column.class);
		Column head = colsExpr[0];
		Column[] tails = FStream.of(colsExpr).drop(1).toArray(Column.class);
		return input.agg(head, tails);
	}
}
