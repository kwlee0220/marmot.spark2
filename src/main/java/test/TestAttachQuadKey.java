package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

import marmot.ConfigurationBuilder;
import marmot.spark.MarmotRDD;
import marmot.spark.MarmotSpark;
import marmot.spark.dataset.SparkDataSet;
import utils.StopWatch;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestAttachQuadKey {
	private static final String INPUT = "구역/집계구";
	private static final String QK_DS = "구역/연속지적도_2017";
	static final String RESULT = "tmp/result";
	
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
		
		SparkDataSet ds = marmot.getDataSet(INPUT);
		SparkDataSet qkDs = marmot.getDataSet(QK_DS);
		
		MarmotRDD mrdd = ds.read().attachQuadKey(qkDs.getSpatialQuadKeyAll(), null, true, false);
								
//		mdf.store(RESULT, StoreDataSetOptions.FORCE);
//
		mrdd.toDataFrame().show();
	}
}
