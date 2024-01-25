package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

import marmot.ConfigurationBuilder;
import marmot.geo.command.ClusterSpatiallyOptions;
import marmot.spark.MarmotSpark;
import marmot.spark.dataset.SparkDataSet;
import utils.StopWatch;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestClusterSpatialDataSet {
//	private static final String INPUT = "구역/읍면동";
//	private static final String INPUT = "구역/집계구";
//	private static final String INPUT = "주소/건물POI";
	private static final String INPUT = "나비콜/택시로그";
	private static final String QK_DS = "주소/건물POI";
	static final String RESULT = "tmp/result";
	
	public static void run(MarmotSpark marmot, String dsId) throws Exception {
		SparkDataSet ds = marmot.getDataSet(dsId);
		
		ClusterSpatiallyOptions opts = ClusterSpatiallyOptions.FORCE;
		ds.cluster(opts);
	}
	
	public static final void main(String... args) throws Exception {
		MarmotSpark.configureLog4j();
		
		StopWatch watch = StopWatch.start();
		
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
		
		run(marmot, INPUT);
//		SparkDataSet ds = marmot.getDataSet(INPUT);
//		ds.read().getDataFrame().show();
	}
}
