package marmot.spark.optor.geo.join;

import marmot.GRecordSchema;
import marmot.RecordSet;
import marmot.optor.geo.join.NestedLoopMatch;
import marmot.plan.SpatialJoinOptions;
import marmot.spark.MarmotSpark;
import marmot.spark.dataset.SparkDataSet;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialSemiJoin extends NestedLoopSpatialJoin {
	private static final long serialVersionUID = 1L;
	
	private final boolean m_negated;
	private transient RecordSet m_emptyResult;

	public SpatialSemiJoin(SparkDataSet inner, SpatialJoinOptions opts) {
		super(inner, true, opts, true);
		
		m_negated = opts.negated().getOrElse(false);
	}

	@Override
	protected GRecordSchema initialize(MarmotSpark marmot, GRecordSchema outerGSchema,
										GRecordSchema innerGSchema) {
		return outerGSchema;
	}

	@Override
	protected void buildContext() {
		m_emptyResult = RecordSet.empty(m_inputGSchema.getRecordSchema());
	}

	@Override
	protected RecordSet doInnerLoop(NestedLoopMatch match) {
		boolean isEmpty = !match.getInnerRecords().next().isPresent();
		boolean hasMatches = m_negated ? isEmpty : !isEmpty;
		if ( hasMatches ) {
			return RecordSet.of(match.getOuterRecord());
		}
		else {
			return m_emptyResult;
		}
	}
}
