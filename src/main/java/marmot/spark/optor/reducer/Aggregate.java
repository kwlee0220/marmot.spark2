package marmot.spark.optor.reducer;

import static marmot.spark.optor.reducer.AggregateFunctions.calcOutputDataType;

import java.util.Collections;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;

import marmot.GRecordSchema;
import marmot.RecordSchema;
import marmot.optor.AggregateFunction;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.spark.optor.AbstractRDDFunction;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Aggregate extends AbstractRDDFunction {
	private static final long serialVersionUID = 1L;

	private final AggregateFunction[] m_aggrs;
	
	public Aggregate(AggregateFunction... aggrs) {
		super(false);
		
		m_aggrs = aggrs;
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inputGSchema) {
		RecordSchema inputSchema = inputGSchema.getRecordSchema();
		RecordSchema outputSchema
				= FStream.of(m_aggrs)
						.map(a -> new marmot.Column(a.m_resultColumn,
													calcOutputDataType(inputSchema, a)))
						.fold(RecordSchema.builder(),  (b,c) -> b.addColumn(c))
						.build();
		return inputGSchema.derive(outputSchema);
	}

	@Override
	public JavaRDD<RecordLite> apply(JavaRDD<RecordLite> input) {
		ValueAggregator<?>[] vaggrs = FStream.of(m_aggrs)
											.map(AggregateFunctions::toValueAggregator)
											.toArray(ValueAggregator.class);
		ValueAggregatorList aggrList = new ValueAggregatorList(vaggrs);
		aggrList.initialize(getInputRecordSchema());
		
		RecordLite zeroValue = aggrList.getZeroValue();
		RecordLite aggregated = input.aggregate(zeroValue, aggrList.getSequencer(),
												aggrList.getCombiner());
		aggregated = aggrList.toFinalValue(aggregated);
		return m_marmot.getJavaSparkContext().parallelize(Collections.singletonList(aggregated));
	}

	@Override
	protected Iterator<RecordLite> mapPartition(Iterator<RecordLite> iter) {
		throw new AssertionError("should not be called");
	}
}
