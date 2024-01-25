package marmot.spark.dataset;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.GRecordSchema;
import marmot.RecordSet;
import marmot.dataset.Catalog;
import marmot.dataset.DataSetException;
import marmot.dataset.DataSetType;
import marmot.dataset.GeometryColumnInfo;
import marmot.dataset.HdfsDataSet;
import marmot.dataset.IndexNotFoundException;
import marmot.dataset.NoGeometryColumnException;
import marmot.dataset.NotSpatiallyClusteredException;
import marmot.geo.CoordinateTransform;
import marmot.geo.GeoClientUtils;
import marmot.geo.catalog.DataSetInfo;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.geo.command.ClusterSpatiallyOptions;
import marmot.io.HdfsPath;
import marmot.io.MarmotFileNotFoundException;
import marmot.io.MarmotFileWriteOptions;
import marmot.io.RecordWritable;
import marmot.io.geo.cluster.CacheableQuadCluster;
import marmot.io.geo.cluster.InMemoryIndexedClusterBuilder;
import marmot.io.geo.cluster.QuadClusterFile;
import marmot.io.geo.index.GlobalIndexEntry;
import marmot.io.geo.index.SpatialIndexedFile;
import marmot.io.mapreduce.seqfile.MarmotFileInputFormat;
import marmot.io.serializer.SerializationException;
import marmot.optor.geo.SpatialRelation;
import marmot.plan.PredicateOptions;
import marmot.spark.MarmotRDD;
import marmot.spark.MarmotSpark;
import marmot.spark.QuadKeyRDD;
import marmot.spark.RecordLite;
import marmot.spark.geo.cluster.QuadSpacePartitioner;
import utils.Throwables;
import utils.func.FOption;
import utils.func.UncheckedFunction;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SequenceFileDataSet implements SparkDataSet, HdfsDataSet<MarmotRDD>, Serializable {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(SequenceFileDataSet.class);
//	static final String THUMBNAIL_PREFIX = "database/thumbnails/";
	
	private final MarmotSpark m_marmot;
	private final Catalog m_catalog;
	private DataSetInfo m_info;				// append() 호출시 변경될 수 있음
	private final HdfsPath m_path;
	
	public SequenceFileDataSet(MarmotSpark marmot, DataSetInfo info) {
		m_marmot = marmot;
		m_catalog = marmot.getCatalog();
		m_info = info;
		
		m_path = HdfsPath.of(marmot.getHadoopFileSystem(), new Path(m_info.getFilePath()));
	}

//	@Override
//	public MarmotSpark getMarmotSpark() {
//		return m_marmot;
//	}

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
		return m_info.getRecordCount();
	}

//	@Override
//	public FOption<String> getCompressionCodecName() {
//		return m_info.getCompressionCodecName();
//	}

	@Override
	public long getLength() {
		try {
			return getHdfsPath().walkRegularFileTree()
								.map(UncheckedFunction.sneakyThrow(HdfsPath::getFileStatus))
								.mapToLong(FileStatus::getLen)
								.sum();
		}
		catch ( MarmotFileNotFoundException e ) {
			// DataSet가 catalog에만 등록되고, 실제 레코드가 하나도 삽입되지 않으면,
			// HDFS 파일이 생성되지 않을 수 있기 때문에, 이때는 0L을 반환한다.
			return 0L;
		}
		catch ( Exception e ) {
			throw new DataSetException(m_info.getId(), Throwables.unwrapThrowable(e));
		}
	}

	@Override
	public QuadClusterFile<? extends CacheableQuadCluster> getSpatialClusterFile()
		throws NotSpatiallyClusteredException {
		if ( hasSpatialIndex() ) {
			return getSpatialIndexFile();
		}
		
		throw IndexNotFoundException.fromDataSet(getId());
//
//		// lookup-table dataset가 heap인 경우는 그 크기가 cluster 최대 크기보다 작은 경우는
//		// 즉석해서 clustering해서 사용하고, 그렇지 않은 경우는 예외를 발생시킨다.
//		long size = getLength();
//		if ( size > m_marmot.getDefaultClusterSize() ) {
//		}
//		
//		RecordSet rset = read().toLocalRecordSet();
//		return new InMemoryIndexedClusterBuilder(getGeometryColumnInfo(), rset, 8).get();
	}

	@Override
	public List<String> getSpatialQuadKeyAll() {
		if ( !hasSpatialIndex() ) {
			throw new NotSpatiallyClusteredException("dataset id=" + getId());
		}
		
		return FStream.from(getSpatialIndexFile().getGlobalIndex().getIndexEntryAll())
						.sort((e1,e2) -> e1.quadKey().compareTo(e2.quadKey()))
						.map(GlobalIndexEntry::quadKey)
						.toList();
	}

	@Override
	public QuadSpacePartitioner getSpatialPartitioner() {
		return QuadSpacePartitioner.from(getSpatialQuadKeyAll());
	}

	@Override
	public QuadClusterFile<? extends CacheableQuadCluster>
	loadOrBuildQuadClusterFile() throws IndexNotFoundException {
		if ( hasSpatialIndex() ) {
			return getSpatialIndexFile();
		}

		// lookup-table dataset가 heap인 경우는 그 크기가 cluster 최대 크기보다 작은 경우는
		// 즉석해서 clustering해서 사용하고, 그렇지 않은 경우는 예외를 발생시킨다.
		long size = getLength();
		if ( size > m_marmot.getDefaultClusterSize() ) {
		}
		
		RecordSet rset = read().toLocalRecordSet();
		return new InMemoryIndexedClusterBuilder(getGeometryColumnInfo(), rset, 8).get();
	}

	@Override
	public MarmotRDD read() {
		try {
			String pathes = m_path.walkRegularFileTree()
									.mapOrThrow(p -> p.getAbsolutePath().toString())
									.join(',');
			
			Configuration conf = m_marmot.getHadoopConfiguration();
			conf.setLong(FileInputFormat.SPLIT_MAXSIZE, m_marmot.getDefaultBlockSize(m_info.getType()));
			
			JavaPairRDD<NullWritable,RecordWritable> pairs = m_marmot.getJavaSparkContext()
					.newAPIHadoopFile(pathes, MarmotFileInputFormat.class,
										NullWritable.class, RecordWritable.class, conf);
			JavaRDD<RecordLite> jrdd = pairs.map(t -> RecordLite.of(t._2.get()));
			return new MarmotRDD(m_marmot, getGRecordSchema(), jrdd);
		}
		catch ( IOException e ) {
			throw new IllegalArgumentException("fails to load SequenceFileDataSet: path=" + m_path);
		}
	}

	@Override
	public MarmotRDD query(Envelope range) {
		Geometry key = GeoClientUtils.toPolygon(range);
		return read().filter(SpatialRelation.INTERSECTS, key, PredicateOptions.DEFAULT);
	}

	@Override
	public void cluster(ClusterSpatiallyOptions opts) {
		QuadKeyRDD partitioned;
		if ( opts.quadKeyList().isPresent() ) {
			List<String> qkeys = opts.quadKeyList().getUnchecked();
			partitioned = read().partitionByQuadkey(QuadSpacePartitioner.from(qkeys), true);
		}
		else if ( opts.quadKeyDsId().isPresent() ) {
			SparkDataSet qkDs = m_marmot.getDataSet( opts.quadKeyDsId().getUnchecked());
			List<String> qkeys = qkDs.getSpatialQuadKeyAll();
			partitioned = read().partitionByQuadkey(QuadSpacePartitioner.from(qkeys), true);
		}
		else {
			long blockSize = opts.blockSize()
								.getOrElse(m_marmot.getDefaultBlockSize(DataSetType.SPATIAL_CLUSTER));
			long sampleSize = opts.sampleSize().getOrElse(blockSize);
			long clusterSize = opts.clusterSize().getOrElse(blockSize);
			double sampleRatio = Math.min(1, (double)sampleSize / getLength());
			Envelope validRange = opts.validRange().getOrNull();
			partitioned = read().partitionByQuadkey(sampleRatio, validRange, clusterSize);
		}
		
		MarmotFileWriteOptions wopts = MarmotFileWriteOptions.FORCE(opts.force())
															.blockSize(opts.blockSize());
		partitioned.store(getId(), opts.partitionCount(), wopts);
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
			m_catalog.deleteDataSetInfo(m_info.getId());
			
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
		return String.format("%s: id=%s, type=%s, schema=%s", getClass().getSimpleName(),
							getId(), getType(), getGRecordSchema());
	}
	
//	private MarmotDataFrame readSpatialClusterFile() throws IOException {
//		SpatialClusterFileRDD rdd = new SpatialClusterFileRDD(m_marmot, this);
//		return from(rdd.toJavaRDD().map(Rows::toRow));
//	}
	
	@Override
	public boolean hasSpatialIndex() {
		if ( !hasGeometryColumn() ) {
			return false;
		}
		
		return m_catalog.getSpatialIndexCatalogInfo(getId()).isPresent();
	}
	
	public SpatialIndexedFile getSpatialIndexFile() {
		if ( !hasSpatialIndex() ) {
			throw IndexNotFoundException.fromDataSet(getId());
		}
		
		GeometryColumnInfo gcIinfo = m_info.getGeometryColumnInfo().get();
		Path clusterPath = m_catalog.generateSpatialIndexPath(getId(), gcIinfo.name());

		HdfsPath path = HdfsPath.of(m_marmot.getHadoopConfiguration(), clusterPath);
		return SpatialIndexedFile.load(path);
	}

	@Override
	public FOption<SpatialIndexInfo> getSpatialIndexInfo() {
		if ( !hasSpatialIndex() ) {
			return FOption.empty();
		}
		SpatialIndexedFile idxFile = getSpatialIndexFile();

		SpatialIndexInfo idxInfo = new SpatialIndexInfo(getId(), getGeometryColumnInfo());
		idxInfo.setHdfsFilePath(idxFile.getClusterDir().toString());
		String srcSrid = idxInfo.getGeometryColumnInfo().srid();
		if ( srcSrid.equals("EPSG:4326") ) {
			idxInfo.setTileBounds(idxFile.getQuadBounds());
			idxInfo.setDataBounds(idxFile.getDataBounds());
		}
		else {
			CoordinateTransform trans = CoordinateTransform.get("EPSG:4326", srcSrid);
			idxInfo.setTileBounds(trans.transform(idxFile.getQuadBounds()));
			idxInfo.setDataBounds(trans.transform(idxFile.getDataBounds()));
		}
		idxInfo.setRecordCount(idxFile.getRecordCount());
		idxInfo.setClusterCount(idxFile.getClusterCount());
		idxInfo.setNonDuplicatedRecordCount(idxFile.getOwnedRecordCount());
		
		return FOption.of(idxInfo);
	}

//	public void deleteSpatialIndex() {
//		m_catalog.getSpatialIndexCatalogInfo(getId())
//				.ifPresent(catIdx -> {
//					Path idxFilePath = new Path(catIdx.getHdfsFilePath());
//					
//					Catalog catalog = m_marmot.getCatalog();
//					catalog.deleteSpatialIndexCatalogInfo(getId(), getGeometryColumn());
//					HdfsPath.of(m_marmot.getHadoopConfiguration(), idxFilePath).deleteUpward();
//				}); 
//	}
//
//	public boolean deleteThumbnail() {
//		HdfsPath path = getThumbnailPath();
//		if ( path.exists() ) {
//			return path.delete();
//		}
//		else {
//			return false;
//		}
//	}
//	
//	public HdfsPath getThumbnailPath() {
//		return HdfsPath.of(m_marmot.getHadoopFileSystem(), new Path(THUMBNAIL_PREFIX + getId()));
//	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final MarmotSpark m_marmot;
		private final String m_dsId;
		
		private SerializationProxy(SequenceFileDataSet ds) {
			m_marmot = ds.m_marmot;
			m_dsId = ds.getId();
		}
		
		private Object readResolve() {
			DataSetInfo info = m_marmot.getCatalog().getDataSetInfo(m_dsId).getOrNull();
			if ( info == null || info.getType() != DataSetType.FILE ) {
				throw new SerializationException("data is corrupted: dsId=" + m_dsId);
			}
			
			return new SequenceFileDataSet(m_marmot, info);
		}
	}
}
