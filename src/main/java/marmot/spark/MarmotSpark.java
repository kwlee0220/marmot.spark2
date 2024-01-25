package marmot.spark;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.text.StringSubstitutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UDTRegistration;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import com.google.common.collect.Maps;

import marmot.RecordSchema;
import marmot.dataset.Catalog;
import marmot.dataset.DataSetExistsException;
import marmot.dataset.DataSetNotFoundException;
import marmot.dataset.DataSetType;
import marmot.geo.catalog.DataSetInfo;
import marmot.io.HdfsPath;
import marmot.io.MarmotSequenceFile;
import marmot.io.RecordWritable;
import marmot.io.mapreduce.seqfile.MarmotFileInputFormat;
import marmot.optor.CreateDataSetOptions;
import marmot.spark.dataset.SequenceFileDataSet;
import marmot.spark.dataset.SparkDataSet;
import marmot.spark.dataset.SpatialClusterDataSet;
import marmot.spark.optor.geo.SpatialUDFs;
import marmot.spark.type.EnvelopeUDT;
import marmot.spark.type.FloatArrayUDT;
import marmot.spark.type.GeometryCollectionUDT;
import marmot.spark.type.GeometryUDT;
import marmot.spark.type.GridCellUDT;
import marmot.spark.type.LineStringUDT;
import marmot.spark.type.MultiLineStringUDT;
import marmot.spark.type.MultiPointUDT;
import marmot.spark.type.MultiPolygonUDT;
import marmot.spark.type.PointUDT;
import marmot.spark.type.PolygonUDT;
import marmot.type.GridCell;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotSpark implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private static final long DEF_TEXT_BLOCK_SIZE = UnitUtils.parseByteSize("128m");
	private static final long DEF_BLOCK_SIZE = UnitUtils.parseByteSize("64m");
	private static final long DEF_CLUSTER_SIZE = UnitUtils.parseByteSize("64m");
	private static final int DEF_PARTITIOIN_COUNT = 7;
	
	private transient  Configuration m_conf;
	private transient FileSystem m_fs;
	private transient Catalog m_catalog;
	
	private SparkSession m_spark;
	private transient JavaSparkContext m_jsc;

	public MarmotSpark(Configuration conf, SparkSession spark) {
		m_conf = conf;
		m_fs = HdfsPath.getFileSystem(conf);
		m_catalog = Catalog.initialize(conf);
		
		m_spark = spark;
		registerUdts();
		registerUdfs(m_spark.sqlContext().udf());
		
		m_jsc = JavaSparkContext.fromSparkContext(m_spark.sparkContext());
	}
	
	public SparkSession getSparkSession() {
		return m_spark;
	}
	
	public SQLContext getSqlContext() {
		return m_spark.sqlContext();
	}
	
	public JavaSparkContext getJavaSparkContext() {
		return m_jsc;
	}
	
	/**
	 * 하둡 설정을 반환한다.
	 * 
	 * @return	설정 객체.
	 */
	public Configuration getHadoopConfiguration() {
		return m_conf;
	}
	
	/**
	 * HDFS 파일 시스템 객체를 반환한다.
	 * 
	 * @return	HDFS 파일 시스템
	 */
	public FileSystem getHadoopFileSystem() {
		return m_fs;
	}
	
	/**
	 * 카다로그를 반환한다.
	 * 
	 * @return	카다로그
	 */
	public Catalog getCatalog() {
		return m_catalog;
	}
	
	/**
	 * 식별자에 해당하는 데이터세트 등록정보를 반환한다.
	 * <p>
	 * 식별자에 해당하는 데이터세트가 없는 경우에는
	 * {@link DataSetNotFoundException} 예외를 발생시킨다.
	 * 
	 * @param dsId	데이터세트 식별자.
	 * @return	데이터세트 등록정보 객체
	 * @throws DataSetNotFoundException	식별자에 해당하는 데이터세트 등록정보가 없는 경우
	 */
	public DataSetInfo getDataSetInfo(String dsId) {
		Utilities.checkNotNullArgument(dsId);
		
		return m_catalog.getDataSetInfo(dsId)
						.getOrThrow(()->new DataSetNotFoundException(dsId));
	}
	
	/**
	 * 식별자에 해당하는 데이터세트 객체를 반환한다.
	 * <p>
	 * 식별자에 해당하는 데이터세트가 없는 경우에는 {@link DataSetNotFoundException} 예외를 발생시킨다.
	 * 
	 * @param dsId	데이터세트 식별자.
	 * @return	데이터세트 객체
	 * @throws DataSetNotFoundException	식별자에 해당하는 데이터세트가 없는 경우
	 */
	public SparkDataSet getDataSet(String dsId) {
		Utilities.checkNotNullArgument(dsId);
		
		DataSetInfo info = m_catalog.getDataSetInfo(dsId)
									.getOrThrow(() -> new DataSetNotFoundException(dsId));
		
		return toSparkDataSet(info);
	}
	
	/**
	 * 식별자에 해당하는 데이터세트 객체를 반환한다.
	 * <p>
	 * 식별자에 해당하는 데이터세트가 없는 경우에는 null을 반환한다.
	 * 
	 * @param dsId	데이터세트 식별자.
	 * @return	데이터세트 객체
	 */
	public SparkDataSet getDataSetOrNull(String dsId) {
		Utilities.checkNotNullArgument(dsId);
		
		DataSetInfo info = m_catalog.getDataSetInfo(dsId).getOrNull();
		if ( info == null ) {
			return null;
		}
		
		return toSparkDataSet(info);
	}

	/**
	 * Marmot에 등록된 모든 데이터세트들의 리스트를 반환한다.
	 * 
	 * @return	데이터세트 리스트
	 */
	public List<SparkDataSet> getDataSetAll() {
		return FStream.from(m_catalog.getDataSetInfoAll())
						.map(this::toSparkDataSet)
						.toList();
	}

	/**
	 * 주어진 폴더하에 존재하는 데이터세트들의 리시트를 반환한다.
	 * 
	 * @param folder	대상 폴더 경로명
	 * @param recursive	모든 하위 폴더 검색 여부
	 * @return	데이터세트 리스트
	 */
	public List<SparkDataSet> getDataSetAllInDir(String folder, boolean recursive) {
		Utilities.checkNotNullArgument(folder);
		
		return FStream.from(m_catalog.getDataSetInfoAllInDir(folder, recursive))
						.map(this::toSparkDataSet)
						.toList();
	}
	
	private SparkDataSet toSparkDataSet(DataSetInfo info) {
		switch ( info.getType() ) {
			case FILE:
			case TEXT:
				return new SequenceFileDataSet(this, info);
			case SPATIAL_CLUSTER:
				return new SpatialClusterDataSet(this, info);
			default:
				throw new IllegalArgumentException("unsupported DataSetType: id=" + info.getId()
													+ ", type=" + info.getType());
		}
	}

	/**
	 * 빈 데이터세트를 생성한다.
	 * 
	 * @param dsId	생성될 데이터세트의 식별자.
	 * @param schema	생성될 데이터세트의 스키마.
	 * @param opts	생성 옵션
	 * @return	생성된 데이터세트 객체
	 */
	public SparkDataSet createDataSet(String dsId, RecordSchema schema, CreateDataSetOptions opts)
		throws DataSetExistsException {
		Utilities.checkNotNullArgument(dsId, "dataset id is null");
		Utilities.checkNotNullArgument(schema, "dataset schema is null");
		Utilities.checkNotNullArgument(opts, "CreateDataSetOptions is null");
		
		// 'force' 옵션이 있는 경우는 식별자에 해당하는 미리 삭제한다.
		// 주어진 식별자가 폴더인 경우는 폴더 전체를 삭제한다.
		if ( opts.force() ) {
			m_catalog.deleteDataSetInfo(dsId);
			deleteDir(dsId);
		}
		
		// 데이터세트 관련 정보를 카다로그에 추가시킨다.
		DataSetInfo dsInfo = new DataSetInfo(dsId, opts.type(), schema);
		dsInfo.setGeometryColumnInfo(opts.geometryColumnInfo());
		dsInfo.setBlockSize(opts.blockSize()
								.getOrElse(() -> getDefaultBlockSize(DataSetType.FILE)));
		dsInfo.setCompressionCodecName(opts.compressionCodecName());
		dsInfo.setUpdatedMillis(System.currentTimeMillis());
		m_catalog.initialize(dsInfo);
		m_catalog.insertDataSetInfo(dsInfo);

		return toSparkDataSet(dsInfo);
	}

	public void deleteDir(String folder) {
		List<DataSetInfo> infoList = m_catalog.getDataSetInfoAllInDir(folder, true);
		
		infoList.stream()
				.sorted((i1,i2) -> i1.getId().compareTo(i2.getId()))
				.forEach(info -> {
					String path = info.getFilePath();
					if ( !m_catalog.isExternalDataSet(path) ) {
						toSparkDataSet(info).delete();
					}
				});
		m_catalog.deleteDir(folder);
	}
	
	public JavaRDD<RecordLite> toJavaRDD(List<RecordLite> records) {
		return m_jsc.parallelize(records);
	}
	
	public Dataset<Row> readMarmotSequenceFile(String top) throws IOException {
		List<String> pathList = HdfsPath.walkRegularFileTree(m_fs, new Path(top))
										.mapOrThrow(p -> p.getAbsolutePath().toString())
										.toList();
		String pathes = FStream.from(pathList).join(',');
		
		RecordSchema schema = MarmotSequenceFile.of(HdfsPath.of(m_fs, new Path(pathList.get(0))))
												.getRecordSchema();
		StructType rowType = Rows.toSparkStructType(schema);

		JavaRDD<Row> rows = m_jsc.newAPIHadoopFile(pathes, MarmotFileInputFormat.class, NullWritable.class,
													RecordWritable.class, m_conf)
									.map(t -> Rows.toRow(t._2.get()));
		return m_spark.sqlContext().createDataFrame(rows, rowType);
	}
	
	public HdfsPath getDataSetHdfsPath(String dsId) {
		return HdfsPath.of(m_conf, m_catalog.generateFilePath(dsId));
	}
	
	public long getDefaultBlockSize(DataSetType type) {
		switch ( type ) {
			case FILE:
			case SPATIAL_CLUSTER:
				return getDefaultMarmotBlockSize();
			case TEXT:
			case LINK:
				return getDefaultTextBlockSize();
			case CLUSTER:
				return getDefaultClusterSize();
			default:
				throw new AssertionError();
		}
	}
	
	public long getDefaultMarmotBlockSize() {
		return getProperty("marmot.default.block_size")
				.map(UnitUtils::parseByteSize)
				.getOrElse(DEF_BLOCK_SIZE);
	}
	
	public long getDefaultTextBlockSize() {
		return getProperty("marmot.default.text_block_size")
				.map(UnitUtils::parseByteSize)
				.getOrElse(DEF_TEXT_BLOCK_SIZE);
	}
	
	public long getDefaultClusterSize() {
		return getProperty("marmot.default.cluster_size")
				.map(UnitUtils::parseByteSize)
				.getOrElse(DEF_CLUSTER_SIZE);
	}

	public int getDefaultPartitionCount() {
		return getProperty("marmot.default.reducer_count")
				.map(Integer::parseInt)
				.getOrElse(DEF_PARTITIOIN_COUNT);
	}
	
	public FOption<String> getProperty(String name) {
		return FOption.ofNullable(m_conf.get(name, null));
	}
	
	private static void registerUdts() {
		UDTRegistration.register(Float[].class.getName(), FloatArrayUDT.class.getName());
		
		UDTRegistration.register(Envelope.class.getName(), EnvelopeUDT.class.getName());
		UDTRegistration.register(GridCell.class.getName(), GridCellUDT.class.getName());
		
		UDTRegistration.register(Point.class.getName(), PointUDT.class.getName());
		UDTRegistration.register(MultiPoint.class.getName(), MultiPointUDT.class.getName());
		UDTRegistration.register(Polygon.class.getName(), PolygonUDT.class.getName());
		UDTRegistration.register(MultiPolygon.class.getName(), MultiPolygonUDT.class.getName());
		UDTRegistration.register(LineString.class.getName(), LineStringUDT.class.getName());
		UDTRegistration.register(MultiLineString.class.getName(), MultiLineStringUDT.class.getName());
		UDTRegistration.register(GeometryCollection.class.getName(), GeometryCollectionUDT.class.getName());
		UDTRegistration.register(Geometry.class.getName(), GeometryUDT.class.getName());
	}
	
	private static void registerUdfs(UDFRegistration registry) {
		SpatialUDFs.registerUdf(registry);
	}
	
	private void writeObject(ObjectOutputStream os) throws IOException {
		os.defaultWriteObject();
		
		m_conf.write(os);
	}
	
	private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
		is.defaultReadObject();
		
		m_conf = new Configuration();
		m_conf.readFields(is);
		m_fs = HdfsPath.getFileSystem(m_conf);
		m_catalog = Catalog.initialize(m_conf);
	}
	
	public static File getLog4jPropertiesFile() {
		String homeDir = FOption.ofNullable(System.getenv("MARMOT_SPARK_HOME"))
								.getOrElse(() -> System.getProperty("user.dir"));
		return new File(homeDir, "log4j.properties");
	}
	
	public static File configureLog4j() throws IOException {
		File propsFile = getLog4jPropertiesFile();
		
		Properties props = new Properties();
		try ( InputStream is = new FileInputStream(propsFile) ) {
			props.load(is);
		}
		
		Map<String,String> bindings = Maps.newHashMap();
		bindings.put("marmot.home", propsFile.getParentFile().toString());

		String rfFile = props.getProperty("log4j.appender.rfout.File");
		rfFile = StringSubstitutor.replace(rfFile, bindings);
		props.setProperty("log4j.appender.rfout.File", rfFile);
		
		return propsFile;
	}
}
