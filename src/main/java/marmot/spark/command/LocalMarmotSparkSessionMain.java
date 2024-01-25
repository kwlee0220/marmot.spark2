package marmot.spark.command;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

import marmot.ConfigurationBuilder;
import marmot.dataset.GeometryColumnInfo;
import marmot.spark.MarmotRDD;
import marmot.spark.MarmotSpark;
import marmot.spark.dataset.SparkDataSet;
import picocli.CommandLine.Command;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="marmot_spark_session",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="create a MarmotSparkSession")
public class LocalMarmotSparkSessionMain {
	private static final String SIDO = "구역/시도";
//	@Option(names={"-port"}, paramLabel="number", required=true,
//			description={"marmot spark session port number"})
//	private int m_port;
	
	public static final void main(String... args) throws Exception {
		MarmotSpark.configureLog4j();
		
		Configuration conf = new ConfigurationBuilder()
								.forLocalMR()
								.build();
		
		// create a SparkSession
		SparkSession spark = SparkSession.builder()
										.appName("marmot_spark_server")
										.master("local[*]")
										.config("spark.driver.host", "localhost")
										.config("spark.driver.maxResultSize", "5g")
										.config("spark.executor.memory", "5g")
										.getOrCreate();
		MarmotSpark marmot = new MarmotSpark(conf, spark);
		

//		SparkDataSet ds = marmot.getDataSet("주소/건물POI");
		SparkDataSet sgg = marmot.getDataSet("구역/시군구");
//		SparkDataSet emd = marmot.getDataSet("구역/읍면동");
//		SparkDataSet ds = marmot.getDataSet("POI/주유소_가격");
		SparkDataSet ds = marmot.getDataSet("구역/집계구");
//		SparkDataSet ds = marmot.getDataSet("tmp/result");
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
//		long clusterSize = UnitUtils.parseByteSize("64mb");
//		ClusterSpatialDataSet cluster = new ClusterSpatialDataSet(marmot, ds, "tmp/result",
//														clusterSize, MarmotFileWriteOptions.FORCE)
//											.setSampleSize( UnitUtils.parseByteSize("128mb"))
//											.setBoundaryDataSet(sgg);
//		SparkDataSet clustered = cluster.call();
		
		MarmotRDD mrdd;
		mrdd = ds.read();
//		mdf = mdf.centroid(false);
//		mdf = mdf.estimateQuadKeys(0.01, FOption.of(bounds), 17, clusterSize);
//		mdf = mdf.filter("`휘발유` > 2000");
//		mdf = mdf.expand("휘발유", "`휘발유` / 100");
//		mdf = mdf.project("the_geom,ADM_DR_CD,TOT_OA_CD");
//		mdf = mdf.buffer(100);
//		mdf = mdf.spatialJoin(emd, SpatialJoinOptions.OUTPUT("the_geom,고유번호,지역, param.{EMD_KOR_NM}"));
//		mdf.store("tmp/result", StoreDataSetOptions.FORCE);
		
//		MarmotDataFrame left = sgg.read();
//		MarmotDataFrame right = emd.read().expand("sig_cd", "substr(EMD_CD, 0, 5)");
//		mdf = left.hashJoin(right, "sig_cd", "sig_cd", "param.*,sig_kor_nm", JoinOptions.INNER_JOIN);
//		mdf = mdf.aggregateByGroup(Group.ofKeys("ADM_DR_CD").tags("the_geom"), AggregateFunction.COUNT());
		
		System.out.println(mrdd);
//		df.withColumn("휘발유", df.col("휘발유").cast(DataTypes.DoubleType));
		mrdd.toDataFrame().show();
//		mdf.toRecordSet().forEach(System.out::println);
		
	}
}
