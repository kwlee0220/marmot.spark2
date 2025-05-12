package marmot.spark.optor.reducer;

import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.google.common.collect.Lists;

import utils.Tuple4;
import utils.Utilities;
import utils.stream.FStream;

import marmot.GRecordSchema;
import marmot.RecordSchema;
import marmot.optor.AggregateFunction;
import marmot.plan.Group;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.spark.optor.AbstractRDDFunction;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AggregateByGroup extends AbstractRDDFunction {
	private static final long serialVersionUID = 1L;
	
	private final Group m_group;
	private final ValueAggregator<?>[] m_aggrs;
	
	// set when initialized
	private RecordSchema m_valueSchema;
	
	public AggregateByGroup(Group group, AggregateFunction... aggrs) {
		this(group, Lists.newArrayList(aggrs));
	}
	
	public AggregateByGroup(Group group, List<AggregateFunction> aggrs) {
		super(false);
		
		m_group = group;
		m_aggrs = FStream.from(aggrs)
						.map(AggregateFunctions::toValueAggregator)
						.toArray(ValueAggregator.class);
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inputGSchema) {
		RecordSchema inputSchema = inputGSchema.getRecordSchema();
		Tuple4<int[],int[],int[],int[]> colIdxes = GroupUtils.splitColumnIndexes(m_group, inputSchema);
		
		RecordSchema grpSchema = inputSchema.project(Utilities.concat(colIdxes._1, colIdxes._2));
		m_valueSchema = inputSchema.project(colIdxes._4);
		RecordSchema aggrSchema = FStream.of(m_aggrs)
										.zipWithIndex()
										.peek(t -> t.value().initialize(t.index(), m_valueSchema))
										.map(t -> t.value().getOutputColumn())
										.fold(RecordSchema.builder(), (b,c) -> b.addColumn(c))
										.build();
		RecordSchema outputSchema = RecordSchema.concat(grpSchema, aggrSchema);
		return inputGSchema.derive(outputSchema);
	}

	@Override
	public JavaRDD<RecordLite> apply(JavaRDD<RecordLite> input) {
		ValueAggregatorList aggrList = new ValueAggregatorList(m_aggrs);
		aggrList.initialize(m_valueSchema);
		
		RecordLite zeroValue = aggrList.getZeroValue();
		return GroupUtils.split(m_group, getInputRecordSchema(), input)
						.aggregateByKey(zeroValue, aggrList.getSequencer(), aggrList.getCombiner())
						.mapValues(rec -> aggrList.toFinalValue(rec))
						.map(t -> {
							return RecordLite.of(Utilities.concat(t._1.values(), t._2.values()));
						});
	}

	@Override
	protected Iterator<RecordLite> mapPartition(Iterator<RecordLite> iter) {
		throw new AssertionError("should not be called");
	}
}
