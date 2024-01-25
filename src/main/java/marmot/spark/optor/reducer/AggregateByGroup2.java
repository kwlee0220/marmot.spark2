package marmot.spark.optor.reducer;

import static marmot.spark.optor.reducer.AggregateFunctions.calcOutputRecordSchema;
import static marmot.spark.optor.reducer.AggregateFunctions.toAggrColumnList;

import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import com.google.common.collect.Lists;

import utils.CSV;
import utils.stream.FStream;

import marmot.GRecordSchema;
import marmot.RecordSchema;
import marmot.optor.AggregateFunction;
import marmot.plan.Group;
import marmot.spark.MarmotSpark;
import marmot.spark.optor.AbstractDataFrameFunction;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AggregateByGroup2 extends AbstractDataFrameFunction {
	private static final long serialVersionUID = 1L;
	
	private final Group m_group;
	private final List<AggregateFunction> m_aggrs;
	
	// set when initialized
	private RecordSchema m_keySchema;
	private RecordSchema m_valueSchema;
	
	public AggregateByGroup2(Group group, AggregateFunction... aggrs) {
		this(group, Lists.newArrayList(aggrs));
	}
	
	public AggregateByGroup2(Group group, List<AggregateFunction> aggrs) {
		m_group = group;
		m_aggrs = aggrs;
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inputGSchema) {
		RecordSchema inputSchema = inputGSchema.getRecordSchema();
		m_keySchema = CSV.parseCsv(m_group.keys())
									.concatWith(m_group.tags().flatMapFStream(CSV::parseCsv))
									.map(inputSchema::getColumn)
									.fold(RecordSchema.builder(), RecordSchema.Builder::addColumn)
									.build();
		m_valueSchema = calcOutputRecordSchema(inputSchema, m_aggrs);
		
		RecordSchema outputSchema = RecordSchema.concat(m_keySchema, m_valueSchema);
		return inputGSchema.derive(outputSchema);
	}

	@Override
	protected Dataset<Row> mapDataFrame(Dataset<Row> input) {
		Column[] grpCols = CSV.parseCsv(m_group.keys()).map(input::col).toArray(Column.class);
		List<Column> aggrExprList = m_group.tags()
											.flatMapFStream(tag -> CSV.parseCsv(tag))
											.map(functions::first)
											.concatWith(FStream.from(toAggrColumnList(m_aggrs)))
											.toList();
		Column aggrExprHead = aggrExprList.get(0);
		Column[] aggrExprTail = FStream.from(aggrExprList).drop(1).toArray(Column.class);
		return input.groupBy(grpCols).agg(aggrExprHead, aggrExprTail);
	}
}
