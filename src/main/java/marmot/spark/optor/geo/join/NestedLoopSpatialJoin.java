package marmot.spark.optor.geo.join;


import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import utils.KeyValue;
import utils.Tuple;
import utils.stream.FStream;

import marmot.GRecordSchema;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.geo.cluster.QuadClusterCache;
import marmot.optor.geo.SpatialRelation;
import marmot.optor.geo.join.NestedLoopMatch;
import marmot.optor.geo.join.SpatialJoinMatcher;
import marmot.optor.geo.join.SpatialJoinMatchers;
import marmot.optor.support.QuadKeyBinder;
import marmot.optor.support.QuadKeyBinder.QuadKeyBinding;
import marmot.plan.SpatialJoinOptions;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.spark.dataset.SparkDataSet;
import marmot.spark.optor.AbstractRDDFunction;
import marmot.support.EnvelopeTaggedRecord;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class NestedLoopSpatialJoin extends AbstractRDDFunction {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(NestedLoopSpatialJoin.class);
	
	private static final long CACHE_SIZE = 5;
	private static int MAX_WINDOW_SIZE = 4 * 1024;

	protected final SparkDataSet m_inner;
	private final boolean m_semiJoin; 
	private final SpatialJoinOptions m_opts;
	
	protected GRecordSchema m_outerGSchema;
	protected GRecordSchema m_innerGSchema;
	protected transient int m_outerGeomColIdx;
	private transient RecordSchema m_outerSchama;
	private transient RecordSchema m_innerSchama;
	private transient int m_innerGeomColIdx;
	private transient QuadClusterCache m_cache;
	private transient SpatialJoinMatcher m_sjMatcher;
	
	private long m_outerCount = 0;		// outer record read count
	private long m_resultCount = 0;

	protected abstract GRecordSchema initialize(MarmotSpark marmot, GRecordSchema outerGSchema,
												GRecordSchema innerGSchema);
	protected abstract void buildContext();
	protected abstract RecordSet doInnerLoop(NestedLoopMatch match);
	
	protected NestedLoopSpatialJoin(SparkDataSet inner, boolean semiJoin, SpatialJoinOptions opts,
									boolean preservePartitions) {
		super(preservePartitions);
		
		m_inner = inner;
		m_semiJoin = semiJoin;
		m_opts = opts;
		
		setLogger(s_logger);
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inputGSchema) {
		m_outerGSchema = inputGSchema;
		inputGSchema.assertGeometryColumnInfo();
		
		m_innerGSchema = m_inner.getGRecordSchema();
		if ( !m_innerGSchema.hasGeometryColumn() ) {
			String details = String.format("parameter dataset does not have Geometry column: op=%s",
											this);
			throw new IllegalArgumentException(details);
		}
		
		if ( !inputGSchema.getSrid().equals(m_innerGSchema.getSrid()) ) {
			throw new IllegalArgumentException("input srid is not compatible to parameter srid: "
										+ inputGSchema.getSrid() + "<->" + m_innerGSchema.getSrid());
		}
		m_innerGeomColIdx = m_innerGSchema.getGeometryColumnIdx();
		
		return initialize(marmot, inputGSchema, m_innerGSchema);
	}
	
	protected String getRightDataSetId() {
		return m_inner.getId();
	}
	
	protected final Geometry getOuterGeometry(Record outerRecord) {
		return outerRecord.getGeometry(m_outerGeomColIdx);
	}
	
	protected final Geometry getInnerGeometry(Record innerRecord) {
		return innerRecord.getGeometry(m_innerGeomColIdx);
	}

	@Override
	protected Iterator<RecordLite> mapPartition(Iterator<RecordLite> iter) {
		throw new AssertionError("should not be called");
	}
	
	protected String toString(long outerCount, long count, long velo, int clusterLoadCount) {
		String str = String.format("%s: nouters=%d, nmatches=%d", this, outerCount, count);
		if ( velo >= 0 ) {
			str = str + String.format(", velo=%d/s", velo);
		}
		str = str + String.format(", load_count=%d", clusterLoadCount);
		
		return str;
	}

	@Override
	protected Iterator<RecordLite> mapPartitionWithIndex(int idx, Iterator<RecordLite> iter) {
		if ( getLogger().isInfoEnabled() ) {
			getLogger().info("{}: partition={}", getClass().getSimpleName(), idx);
		}
		
		m_outerSchama = m_outerGSchema.getRecordSchema();
		m_outerGeomColIdx = m_outerGSchema.getGeometryColumnIdx();
		m_cache = new QuadClusterCache(m_inner.getSpatialClusterFile(), CACHE_SIZE);
		
		m_innerSchama = m_inner.getRecordSchema();
		int innerGeomColIdx = m_inner.getGRecordSchema().getGeometryColumnIdx();
		SpatialRelation joinExpr = m_opts.joinExpr().map(SpatialRelation::parse)
										.getOrElse(SpatialRelation.INTERSECTS);
		m_sjMatcher = SpatialJoinMatchers.from(joinExpr);
		m_sjMatcher.open(m_outerGeomColIdx, innerGeomColIdx, m_inner.getSrid());
		
		Set<String> qkSrc = m_cache.getClusterKeyAll();
		QuadKeyBinder qkBinder = new QuadKeyBinder(qkSrc, false);
		
		buildContext();
		
		return FStream.from(iter)
						.map(outer -> sortOut(qkBinder, outer))
						.filter(kv -> kv.key() != 0)
						.toKeyValueStream(kv -> kv)
						.liftKeyValues(inStrm -> new FindBiggestGroupWithinWindow<>(inStrm, 2 * MAX_WINDOW_SIZE))
//						.findBiggestGroupWithinWindow(2 * MAX_WINDOW_SIZE, 2 * MAX_WINDOW_SIZE)
						.flatMap(kv -> kv.key() == 1 ? joinWithClustering(kv.value())
													: joinWithoutClustering(kv.value()))
						.map(RecordLite::from)
						.peek(r -> ++m_resultCount)
						.iterator();
	}
	
	private FStream<Record> joinWithClustering(List<Tuple<RecordLite,List<String>>> list) {
		return FStream.from(list)
                        .toKeyValueStream(t -> KeyValue.of(t._2.get(0), t._1))
						.liftKeyValues(inStrm -> new FindBiggestGroupWithinWindow<>(inStrm, MAX_WINDOW_SIZE, 1))
//						.findBiggestGroupWithinWindow(MAX_WINDOW_SIZE, 1)
						.map(kv -> new ClusteringNLJoinJob(kv.key(), kv.value()))
						.flatMap(job -> job.run());
	}
	
	private class ClusteringNLJoinJob {
		private String m_quadKey;
		private List<RecordLite> m_outers;
		private int m_inputCount = 0;
		
		ClusteringNLJoinJob(String quadKey, List<RecordLite> outers) {
			m_quadKey = quadKey;
			m_outers = outers;
		}
		
		public FStream<Record> run() {
			if ( getLogger().isDebugEnabled() ) {
				getLogger().debug(toString());
			}
			return FStream.from(m_outers)
							.peek(r -> ++m_inputCount)
							.map(outer -> matchOuter(outer))
							.flatMap(m -> doInnerLoop(m).fstream());
		}
		
		@Override
		public String toString() {
			return String.format("%s: cluster=%s, progress=%d/%d (total=%d -> %d, load=%d)", getClass().getSimpleName(),
								m_quadKey, m_inputCount, m_outers.size(),
								m_outerCount, m_resultCount, m_cache.getLoadCount());
		}
	}
	
	private FStream<Record> joinWithoutClustering(List<Tuple<RecordLite,List<String>>> tuples) {
		List<Tuple<RecordLite,List<String>>> records = tuples;
		List<NonClusteringNLJoinJob> jobs = Lists.newArrayList();
		
		while ( records.size() > 0 ) {
			KeyValue<String,List<RecordLite>> selected
								= FStream.from(records)
										.flatMap(t -> FStream.from(t._2)
															.map(k -> KeyValue.of(k, t._1)))
										.toKeyValueStream(Function.identity())
										.groupByKey()
										.fstream()
										.max(kv -> kv.value().size())
										.get();
			
			jobs.add(new NonClusteringNLJoinJob(selected.key(), selected.value()));
			records = FStream.from(records)
							.filter(t -> !t._2.contains(selected.key()))
							.toList();
		}
		
		return FStream.from(jobs).flatMap(job -> job.run());
	}
	
	private class NonClusteringNLJoinJob {
		private String m_quadKey;
		private List<RecordLite> m_outers;
		private int m_inputCount = 0;
		
		NonClusteringNLJoinJob(String quadKey, List<RecordLite> outers) {
			m_quadKey = quadKey;
			m_outers = outers;
		}
		
		public FStream<Record> run() {
			if ( getLogger().isDebugEnabled() ) {
				getLogger().debug(toString());
			}
			return FStream.from(m_outers)
							.peek(r -> ++m_inputCount)
							.map(outer -> matchOuter(outer))
							.flatMap(m -> doInnerLoop(m).fstream());
		}
		
		@Override
		public String toString() {
			return String.format("%s: head=%s, progress=%d/%d (total=%d -> %d, load=%d)", getClass().getSimpleName(),
								m_quadKey, m_inputCount, m_outers.size(),
								m_outerCount, m_resultCount, m_cache.getLoadCount());
		}
	}
	
	private KeyValue<Integer,Tuple<RecordLite,List<String>>>
	sortOut(QuadKeyBinder qkBinder, RecordLite outer) {
		Geometry geom = outer.getGeometry(m_outerGeomColIdx);
		
		// 공간 정보가 없는 경우는 무시한다.
		if ( geom == null || geom.isEmpty() ) {
			return KeyValue.of(0, Tuple.of(outer, null));
		}
		
		// 조인조건에 따른 검색 키를 생성하고, 이를 바탕으로 관련된
		// inner cluster를 검색한다.
		Envelope envl84 = m_sjMatcher.toMatchKey(geom);
		List<QuadKeyBinding> bindings = qkBinder.bindQuadKeys(envl84);
		List<String> qkeys = FStream.from(bindings).map(QuadKeyBinding::quadkey).toList();
		return KeyValue.of(Math.min(bindings.size(),2), Tuple.of(outer,qkeys));
	}
	
	private NestedLoopMatch matchOuter(RecordLite rec) {
		Record outer = rec.toRecord(m_outerSchama);
		Geometry outerGeom = outer.getGeometry(m_outerGeomColIdx);
		if ( outerGeom == null || outerGeom.isEmpty() ) {
			return new NestedLoopMatch(outer, m_innerSchama, FStream.empty());
		}
		
		Envelope key = m_sjMatcher.toMatchKey(outerGeom);
		FStream<Record> inners = m_cache.queryClusterKeys(key)
										.map(m_cache::getCluster)
										.flatMap(cluster -> m_sjMatcher.match(outer, cluster))
										.map(EnvelopeTaggedRecord::getRecord);
		if ( m_semiJoin ) {
			inners = inners.take(1);
		}
		
		++m_outerCount;
		
		return new NestedLoopMatch(outer, m_innerSchama, inners);
	}
}
