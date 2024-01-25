package test;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

import marmot.ConfigurationBuilder;
import marmot.spark.MarmotSpark;
import marmot.spark.dataset.SparkDataSet;
import utils.StopWatch;
import utils.UnitUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestEstimateQuadKeys {
	private static final String INPUT = "구역/집계구";
//	private static final String INPUT = "주소/건물POI";
//	private static final String INPUT = "/지오비전/유동인구/2015/월별_시간대";
	private static final String QK_DS = "구역/연속지적도_2017";
	static final String RESULT = "tmp/result";
	
	public static List<String> run(MarmotSpark marmot, SparkDataSet ds) throws Exception {
		long sampleSize = UnitUtils.parseByteSize("64mb");
		double ratio = (double)sampleSize / ds.getLength();
		
		return ds.read().estimateQuadKeys(ratio, null, -1);
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
										.master("local[3]")
										.config("spark.driver.host", "localhost")
										.config("spark.driver.maxResultSize", "5g")
										.config("spark.executor.memory", "5g")
										.getOrCreate();
		MarmotSpark marmot = new MarmotSpark(conf, spark);
		
		SparkDataSet ds = marmot.getDataSet(INPUT);
		List<String> qkeys = run(marmot, ds);
		watch.stop();
		
		System.out.println(qkeys);
		System.out.println("elapsed=" + watch.getElapsedMillisString());
	}
}
