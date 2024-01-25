package marmot.spark.dataset;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Envelope;

import com.clearspring.analytics.util.Lists;

import marmot.GRecordSchema;
import marmot.dataset.DataSetType;
import marmot.dataset.HdfsDataSet;
import marmot.dataset.IndexNotFoundException;
import marmot.dataset.NoGeometryColumnException;
import marmot.dataset.NotSpatiallyClusteredException;
import marmot.geo.catalog.DataSetInfo;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.geo.command.ClusterSpatiallyOptions;
import marmot.io.HdfsPath;
import marmot.io.RecordWritable;
import marmot.io.geo.cluster.CacheableQuadCluster;
import marmot.io.geo.cluster.QuadClusterFile;
import marmot.io.geo.cluster.SpatialClusterFile;
import marmot.io.mapreduce.spcluster.SpatialClusterInputFileFormat;
import marmot.io.mapreduce.spcluster.SpatialClusterInputFileFormat.Parameters;
import marmot.spark.MarmotRDD;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.spark.geo.cluster.QuadSpacePartitioner;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialClusterDataSet implements SparkDataSet, HdfsDataSet<MarmotRDD>, Serializable {
	private static final long serialVersionUID = 1L;
	
	private final MarmotSpark m_marmot;
	private DataSetInfo m_info;				// append() 호출시 변경될 수 있음
	private final HdfsPath m_path;
	private final SpatialClusterFile m_scFile;
	
	public SpatialClusterDataSet(MarmotSpark marmot, DataSetInfo info) {
		m_marmot = marmot;
		m_info = info;
		
		m_path = HdfsPath.of(marmot.getHadoopFileSystem(), new Path(m_info.getFilePath()));
		m_scFile = SpatialClusterFile.of(m_path);
	}

	@Override
	public String getId() {
		return m_info.getId();
	}
	
	public DataSetInfo getDataSetInfo() {
		return m_info;
	}

	@Override
	public DataSetType getType() {
		return m_info.getType();
	}

	@Override
	public GRecordSchema getGRecordSchema() {
		return new GRecordSchema(m_info.getGeometryColumnInfo(), m_info.getRecordSchema());
	}

	@Override
	public Envelope getBounds() throws NoGeometryColumnException {
		if ( hasGeometryColumn() ) {
			return new Envelope(m_info.getBounds());
		}
		else {
			throw new NoGeometryColumnException();
		}
	}

	@Override
	public HdfsPath getHdfsPath() {
		return m_path;
	}

	@Override
	public long getBlockSize() {
		return m_info.getBlockSize();
	}

	@Override
	public long getRecordCount() {
		return m_scFile.getOwnedRecordCount();
	}

	@Override
	public long getLength() {
		return FStream.from(m_scFile.getClusterInfoAll())
						.mapToLong(info -> info.length())
						.sum();
	}

	@Override
	public MarmotRDD read() {
		Configuration conf = m_marmot.getHadoopConfiguration();
		
		Parameters params = new Parameters(m_scFile.getPath().getPath(), m_scFile.getGRecordSchema(),
											null, null);
		SpatialClusterInputFileFormat.setParameters(conf, params);
		
		JavaPairRDD<NullWritable,RecordWritable> pairs = m_marmot.getJavaSparkContext()
				.newAPIHadoopFile(m_path.toString(), SpatialClusterInputFileFormat.class,
									NullWritable.class, RecordWritable.class, conf);
		JavaRDD<RecordLite> jrdd = pairs.map(t -> RecordLite.of(t._2.get()).duplicate());
		return new MarmotRDD(m_marmot, getGRecordSchema(), jrdd);
	}

	@Override
	public MarmotRDD query(Envelope range) throws NoGeometryColumnException {
		return null;
	}

	@Override
	public FOption<SpatialIndexInfo> getSpatialIndexInfo() {
		return FOption.empty();
	}

	@Override
	public void cluster(ClusterSpatiallyOptions opts) {
	}

	@Override
	public SpatialClusterFile getSpatialClusterFile() throws NotSpatiallyClusteredException {
		return m_scFile;
	}

	@Override
	public QuadClusterFile<? extends CacheableQuadCluster>
	loadOrBuildQuadClusterFile() throws IndexNotFoundException {
		return m_scFile;
	}

	@Override
	public List<String> getSpatialQuadKeyAll() throws NotSpatiallyClusteredException {
		return Lists.newArrayList(m_scFile.getClusterKeyAll());
	}

	@Override
	public QuadSpacePartitioner getSpatialPartitioner() {
		return QuadSpacePartitioner.from(m_scFile.getClusterKeyAll());
	}

	@Override
	public void delete() {
		if ( m_info != null ) {
//			// Thumbnail이 존재하면 삭제한다.
//			deleteThumbnail();
//			
//			// 인덱스가 존재하면 해당 인덱스도 제거한다.
//			deleteSpatialIndex();
			
			// 본 데이터세트 등록정보를 카타로그에서 제거한다.
			m_marmot.getCatalog().deleteDataSetInfo(m_info.getId());
			
//			if ( !m_catalog.isExternalDataSet(hdfsFilePath)
//				&& !info.getType().equals(DataSetType.LINK) ) {
//				getFileServer().deleteFileUpward(new Path(hdfsFilePath));
//			}
		}
		
		// HDFS 파일을 삭제한다.
		m_path.deleteUpward();
	}
	
	@Override
	public String toString() {
		return String.format("%s: id=%s, type=%s, count=%d, schema=%s", getClass().getSimpleName(),
								getId(), getType(), getRecordCount(), getGRecordSchema());
	}
}
