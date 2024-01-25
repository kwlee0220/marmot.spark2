package marmot.spark.optor.geo.join;

import java.util.Iterator;
import java.util.Map;

import com.google.common.collect.Maps;

import marmot.GRecordSchema;
import marmot.Record;
import marmot.RecordSchema;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.geo.SpatialRelation;
import marmot.optor.geo.join.SpatialJoinMatcher;
import marmot.optor.geo.join.SpatialJoinMatchers;
import marmot.optor.support.JoinUtils;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.plan.SpatialJoinOptions;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CoGroupSpatialBlockJoin extends CoGroupSpatialJoin {
	private static final long serialVersionUID = 1L;
	
	private final GeometryColumnInfo m_gcInfo;
	
	// initialized when executed
	private transient SpatialJoinMatcher m_sjMatcher;
	private transient ColumnSelector m_selector;
	private transient Map<String,Record> m_binding;
	
	public CoGroupSpatialBlockJoin(GeometryColumnInfo gcInfo, SpatialJoinOptions opts) {
		super(opts);
		
		m_gcInfo = gcInfo;
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema leftGSchema,
										GRecordSchema rightGSchema) {
		ColumnSelector selector = JoinUtils.createJoinColumnSelector(m_leftGSchema.getRecordSchema(),
																	m_rightGSchema.getRecordSchema(),
																	m_opts.outputColumns());
		return new GRecordSchema(m_gcInfo, selector.getRecordSchema());
	}
	
	@Override
	protected Iterator<RecordLite> joinPartions(String quadKey, Iterator<RecordLite> leftGroup,
												Iterator<RecordLite> rightGroup) {
		RecordSchema leftSchema = m_leftGSchema.getRecordSchema();
		RecordSchema rightSchema = m_rightGSchema.getRecordSchema();
		
		SpatialRelation joinExpr = m_opts.joinExpr().map(SpatialRelation::parse)
										.getOrElse(SpatialRelation.INTERSECTS);
		m_sjMatcher = SpatialJoinMatchers.from(joinExpr);
		m_sjMatcher.open(m_leftGeomColIdx, m_rightGeomColIdx, m_leftGSchema.getSrid());
		m_selector = JoinUtils.createJoinColumnSelector(leftSchema, rightSchema, m_opts.outputColumns());
		m_binding = Maps.newHashMap();
		
		QTreeLookupTable slut = new QTreeLookupTable(quadKey, m_rightGSchema, rightGroup);
		return FStream.from(leftGroup)
						.map(left -> left.toRecord(leftSchema))
						.flatMap(left -> combineInners(left, slut))
						.map(RecordLite::from)
						.iterator();
	}

	private FStream<Record> combineInners(Record left, QTreeLookupTable slut) {
		return m_sjMatcher.match(left, slut)
							.map(right -> {
								m_binding.put("left", left);
								m_binding.put("right", right.getRecord());
								return m_selector.select(m_binding);
							});
	}
}
