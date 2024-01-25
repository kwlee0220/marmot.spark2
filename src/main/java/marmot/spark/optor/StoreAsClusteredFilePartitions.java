package marmot.spark.optor;

import java.io.Serializable;
import java.util.Iterator;
import java.util.UUID;

import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.GRecordSchema;
import marmot.RecordSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.io.DataSetPartitionInfo;
import marmot.io.HdfsPath;
import marmot.io.MarmotFileWriteOptions;
import marmot.io.MarmotSequenceFile;
import marmot.optor.StoreDataSetOptions;
import marmot.spark.MarmotPairRDD;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.spark.Rows;
import marmot.spark.geo.cluster.PreGroupedKeyedRecordSetFactory;
import scala.Tuple2;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreAsClusteredFilePartitions implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(StoreAsClusteredFilePartitions.class);
	
	private final String m_dsId;
	private final StoreDataSetOptions m_opts;
	
	// set when initialized
	private MarmotSpark m_marmot;
	private HdfsPath m_path;
	private GRecordSchema m_gschema;
	
	public StoreAsClusteredFilePartitions(String dsId, StoreDataSetOptions opts) {
		m_dsId = dsId;
		m_opts = opts;
	}
	
	/**
	 * 주어진 입력 데이터프레임을 이용하여 본 연산을 수행한 결과 데이터프레임을 반환한다.
	 * 
	 * @param input		입력 데이터프레임.
	 * @param gcInfo	공간 컬럼 등록 정보
	 * @return	연산 수행결과로 생성된 레코드세트 객체.
	 */
	public MarmotPairRDD apply(MarmotPairRDD input) {
		m_marmot = input.getMarmotSpark();
		m_path = HdfsPath.of(m_marmot.getHadoopFileSystem(),
							m_marmot.getCatalog().generateFilePath(m_dsId));
		m_path = m_path.child(UUID.randomUUID().toString());
		m_gschema = input.getGRecordSchema();
		
		JavaPairRDD<RecordLite,RecordLite> applied = apply(input.getRDD());
		return new MarmotPairRDD(input.getMarmotSpark(), input.getKeyGRecordSchema(),
								new GRecordSchema(DataSetPartitionInfo.SCHEMA), applied);
	}
	
	protected JavaPairRDD<RecordLite,RecordLite> apply(JavaPairRDD<RecordLite, RecordLite> input) {
		return input.mapPartitionsWithIndex(this::mapPartitionWithIndex, false)
					.mapToPair(t -> t);
	}

	protected Iterator<Tuple2<RecordLite,RecordLite>>
	mapPartitionWithIndex(int partIdx, Iterator<Tuple2<RecordLite,RecordLite>> iter) {
		@SuppressWarnings("resource")
		PreGroupedKeyedRecordSetFactory<RecordLite> rsetFact = new PreGroupedKeyedRecordSetFactory<>(iter);
		return rsetFact.map(this::store)
						.map(t -> new Tuple2<>(t._1, RecordLite.from(t._2.toRecord())))
						.iterator();
	}

	private Tuple2<RecordLite,DataSetPartitionInfo> store(RecordLite key, FStream<RecordLite> group) {
		String keyString = toKeyString(key);
		HdfsPath partPath = m_path.child(String.format("%s", keyString));
		
		GeometryColumnInfo gcInfo = m_gschema.assertGeometryColumnInfo();
		RecordSet rset = Rows.toRecordSet(m_gschema.getRecordSchema(), group);
		MarmotFileWriteOptions opts = MarmotFileWriteOptions.APPEND_IF_EXISTS
															.blockSize(m_opts.blockSize());
		
		DataSetPartitionInfo dspInfo = MarmotSequenceFile.store(partPath, rset, gcInfo, opts).call();
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info("cluster stored: key={}, {}", keyString, dspInfo);
		}
		return new Tuple2<>(key, dspInfo);
	}
	
	private String toKeyString(RecordLite key) {
		return FStream.of(key.values())
						.map(Object::toString)
						.join('_');
	}
}
