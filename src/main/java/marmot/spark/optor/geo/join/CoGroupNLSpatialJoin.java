package marmot.spark.optor.geo.join;

import java.util.Iterator;

import utils.stream.FStream;

import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.geo.SpatialRelation;
import marmot.optor.geo.join.SpatialJoinMatcher;
import marmot.optor.geo.join.SpatialJoinMatchers;
import marmot.optor.geo.join.SpatialLookupTable;
import marmot.plan.SpatialJoinOptions;
import marmot.spark.RecordLite;
import marmot.support.EnvelopeTaggedRecord;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class CoGroupNLSpatialJoin extends CoGroupSpatialJoin {
	private static final long serialVersionUID = 1L;
	
	private final boolean m_semiJoin;

	protected abstract void buildContext();
	protected abstract FStream<Record> combine(Record left, FStream<Record> right);
	
	protected CoGroupNLSpatialJoin(boolean semiJoin, SpatialJoinOptions opts) {
		super(opts);
		
		m_semiJoin = semiJoin;
	}
	
	@Override
	protected Iterator<RecordLite> joinPartions(String quadKey, Iterator<RecordLite> left,
												Iterator<RecordLite> right) {
		RecordSchema lSchema = m_leftGSchema.getRecordSchema();
		QTreeLookupTable slut = new QTreeLookupTable(quadKey, m_rightGSchema, right);

		SpatialRelation joinExpr = m_opts.joinExpr().map(SpatialRelation::parse)
										.getOrElse(SpatialRelation.INTERSECTS);
		SpatialJoinMatcher sjMatcher = SpatialJoinMatchers.from(joinExpr);
		sjMatcher.open(m_leftGeomColIdx, m_rightGeomColIdx, m_rightGSchema.getSrid());
		
		buildContext();
		
		return FStream.from(left)
						.map(r -> r.toRecord(lSchema))
						.flatMap(r -> combine(r, inner(sjMatcher, r, slut)))
						.map(RecordLite::from)
						.iterator();
	}
	
	private FStream<Record> inner(SpatialJoinMatcher sjMatcher, Record outer, SpatialLookupTable slut) {
		FStream<Record> records = sjMatcher.match(outer, slut)
											.map(EnvelopeTaggedRecord::getRecord);
		if ( m_semiJoin ) {
			records = records.take(1);
		}
		return records;
	}
}
