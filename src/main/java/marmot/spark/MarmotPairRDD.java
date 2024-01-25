package marmot.spark;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import marmot.GRecordSchema;
import marmot.RecordSchema;
import marmot.dataset.DataSetType;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.StoreDataSetOptions;
import marmot.spark.dataset.SequenceFileDataSet;
import marmot.spark.dataset.SparkDataSet;
import marmot.spark.optor.StoreAsClusteredFilePartitions;
import marmot.spark.optor.UpdateDataSetInfo;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotPairRDD implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final MarmotSpark m_marmot;
	private final GRecordSchema m_keyGSchema;
	private final GRecordSchema m_valueGSchema;
	private final JavaPairRDD<RecordLite, RecordLite> m_jpairs;
	
	public MarmotPairRDD(MarmotSpark marmot, GRecordSchema keyGSchema, GRecordSchema valueGSchema,
						JavaPairRDD<RecordLite,RecordLite> jpairs) {
		m_marmot = marmot;
		m_keyGSchema = keyGSchema;
		m_valueGSchema = valueGSchema;
		m_jpairs = jpairs;
	}
	
	public MarmotSpark getMarmotSpark() {
		return m_marmot;
	}
	
	public GRecordSchema getKeyGRecordSchema() {
		return m_keyGSchema;
	}
	
	public GRecordSchema getGRecordSchema() {
		return m_valueGSchema;
	}
	
	public RecordSchema getRecordSchema() {
		return m_valueGSchema.getRecordSchema();
	}
	
	public FOption<GeometryColumnInfo> getGeometryColumnInfo() {
		return m_valueGSchema.getGeometryColumnInfo();
	}
	public GeometryColumnInfo assertGeometryColumnInfo() {
		return m_valueGSchema.assertGeometryColumnInfo();
	}
	
	public JavaPairRDD<RecordLite, RecordLite> getRDD() {
		return m_jpairs;
	}
	
	public MarmotRDD keys() {
		return new MarmotRDD(m_marmot, m_keyGSchema, m_jpairs.keys());
	}
	
	public FOption<Partitioner> partitioner() {
		return FOption.ofNullable(m_jpairs.partitioner().orNull());
	}
	
	public MarmotPairRDD partitionByKey(int partitionCount) {
		Partitioner partitioner = new KeyHashPartitioner(partitionCount);
		JavaPairRDD<RecordLite, RecordLite> partitioned = m_jpairs.partitionBy(partitioner);
		return new MarmotPairRDD(m_marmot, m_keyGSchema, m_valueGSchema, partitioned);
	}
	
	public MarmotPairRDD partitionByKey() {
		JavaPairRDD<RecordLite,RecordLite> cached = m_jpairs.cache();
		List<RecordLite> keys = cached.keys()
										.distinct()
										.sortBy(k -> k, true, 1)
										.collect();
		KeyPartitioner partitioner = new KeyPartitioner(keys);
		JavaPairRDD<RecordLite, RecordLite> partitioned = cached.partitionBy(partitioner);
		return new MarmotPairRDD(m_marmot, m_keyGSchema, m_valueGSchema, partitioned);
	}

	public void store(String dsId, StoreDataSetOptions opts) {
		// option에 GeometryColumnInfo가 없더라도 RDD에 공간 정보가 존재하는 경우에는
		// option에 포함시킨다.
		if ( opts.geometryColumnInfo().isAbsent() && getGeometryColumnInfo().isPresent() ) {
			opts = opts.geometryColumnInfo(assertGeometryColumnInfo());
		}
		
		// force가 true인 경우에는 무조건 대상 데이터세트를 먼저 삭제한다.
		SparkDataSet ds = m_marmot.getDataSetOrNull(dsId);
		if ( opts.force() && ds != null ) {
			ds.delete();
		}

		SequenceFileDataSet seqDs = null;
		RecordSchema schema = m_valueGSchema.getRecordSchema();
		
		// 'append' 요청이 없으면 새로운 SequenceFileDataSet을 생성한다.
		// 만일 동일 식별자의 SparkDataSet가 이미 존재하는 경우에는 오류가 발생한다.
		if ( !opts.append().getOrElse(false) ) {
			seqDs = (SequenceFileDataSet)m_marmot.createDataSet(dsId, schema, opts.toCreateOptions());
		}
		else if ( seqDs.getType() != DataSetType.FILE ) {
			// append 요청이지만, 기존 파일의 타입이 'FILE'이 아닌 경우는 예외를 발생시킨다.
			throw new IllegalArgumentException("target dataset is not FILE: id=" + dsId);
		}
		
		StoreAsClusteredFilePartitions store = new StoreAsClusteredFilePartitions(dsId, opts);
		JavaRDD<RecordLite> infos = store.apply(this).getRDD().map(t -> t._2);
		MarmotRDD dsPartInfos = new MarmotRDD(m_marmot, m_valueGSchema, infos);
		dsPartInfos.apply(new UpdateDataSetInfo(dsId));
	}
	
	private static class KeyPartitioner extends Partitioner {
		private static final long serialVersionUID = 1L;
		
		private final List<RecordLite> m_keys;
		
		KeyPartitioner(List<RecordLite> keys) {
			m_keys = keys;
		}

		@Override
		public int getPartition(Object key) {
			return m_keys.indexOf(key);
		}

		@Override
		public int numPartitions() {
			return m_keys.size();
		}
	}
	
	private static class KeyHashPartitioner extends Partitioner {
		private static final long serialVersionUID = 1L;
		
		private final int m_partitionCount;
		
		KeyHashPartitioner(int partitionCount) {
			m_partitionCount = partitionCount;
		}

		@Override
		public int getPartition(Object key) {
			return key.hashCode() % m_partitionCount;
		}

		@Override
		public int numPartitions() {
			return m_partitionCount;
		}
	}
}
