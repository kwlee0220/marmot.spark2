package marmot.spark.optor.geo.join;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.locationtech.jts.geom.Geometry;

import marmot.GRecordSchema;
import marmot.Record;
import marmot.plan.SpatialJoinOptions;
import marmot.spark.MarmotRDD;
import marmot.spark.MarmotSpark;
import marmot.spark.QuadKeyRDD;
import marmot.spark.RecordLite;
import marmot.spark.geo.cluster.QuadSpacePartitioner;
import scala.Tuple2;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class CoGroupSpatialJoin implements Serializable {
	private static final long serialVersionUID = 1L;
	
	protected final SpatialJoinOptions m_opts;
	
	// when initialized
	protected MarmotSpark m_marmot;
	protected GRecordSchema m_leftGSchema;
	protected GRecordSchema m_rightGSchema;
	protected GRecordSchema m_outputGSchema;
	protected int m_leftGeomColIdx = -1;
	protected int m_rightGeomColIdx = -1;

	protected abstract GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema leftGSchema,
													GRecordSchema rightGSchema);
	protected abstract Iterator<RecordLite> joinPartions(String quadKey, Iterator<RecordLite> left,
														Iterator<RecordLite> right);
	
	protected CoGroupSpatialJoin(SpatialJoinOptions opts) {
		m_opts = opts;
	}
	
	/**
	 * 데이터프레임 함수를 초기화시킨다.
	 * 
	 * @param marmot	marmot 객체.
	 * @param inputSchema	입력 레코드세트의 스키마 정보.
	 */
	public void initialize(MarmotSpark marmot, GRecordSchema leftGSchema, GRecordSchema rightGSchema) {
		m_marmot = marmot;
		m_leftGSchema = leftGSchema;
		m_rightGSchema = rightGSchema;
		m_leftGeomColIdx = m_leftGSchema.getGeometryColumnIdx();
		m_rightGeomColIdx = m_rightGSchema.getGeometryColumnIdx();
		m_outputGSchema = _initialize(marmot, leftGSchema, rightGSchema);
	}
	
	public boolean isInitialized() {
		return m_marmot != null;
	}
	
	public void checkInitialized() {
		if ( m_marmot == null ) {
			throw new IllegalStateException("not initialized");
		}
	}
	
	public final GRecordSchema getGRecordSchema() {
		checkInitialized();
		
		return m_outputGSchema;
	}
	
	public JavaRDD<RecordLite> apply(MarmotRDD left, MarmotRDD right, Iterable<String> quadKeys) {
		QuadSpacePartitioner partitioner = QuadSpacePartitioner.from(quadKeys);
		return apply(left, right, partitioner);
	}
	
	public JavaRDD<RecordLite> apply(MarmotRDD left, MarmotRDD right, QuadSpacePartitioner partitioner) {
		QuadKeyRDD leftPair = left.partitionByQuadkey(partitioner, false);
		QuadKeyRDD rightPair = right.partitionByQuadkey(partitioner, false);
		
		return apply(leftPair.getRDD(), rightPair.getRDD());
	}
	
	public JavaRDD<RecordLite> apply(JavaPairRDD<String,RecordLite> left,
										JavaPairRDD<String,RecordLite> right) {
		checkInitialized();
		
		return left.cogroup(right)
					.flatMap(t -> joinPartions(t._1, t._2._1.iterator(), t._2._2.iterator()));
	}
	
	public JavaRDD<RecordLite> apply(MarmotRDD left,
									Broadcast<Tuple2<GRecordSchema, List<RecordLite>>> right) {
		checkInitialized();

		return left.getJavaRDD().mapPartitions(iter -> {
			return joinPartions("", iter, right.getValue()._2.iterator());
		});
	}
	
	protected final Geometry getLeftGeometry(Record outerRecord) {
		return outerRecord.getGeometry(m_leftGeomColIdx);
	}
	
	protected final Geometry getRightGeometry(Record innerRecord) {
		return innerRecord.getGeometry(m_rightGeomColIdx);
	}
}
