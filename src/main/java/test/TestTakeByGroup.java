package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

import marmot.ConfigurationBuilder;
import marmot.optor.StoreDataSetOptions;
import marmot.plan.Group;
import marmot.spark.MarmotSpark;
import marmot.spark.dataset.SparkDataSet;
import utils.StopWatch;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestTakeByGroup {
//	private static final String INPUT = "구역/집계구";
	private static final String INPUT = "주소/건물POI";
//	private static final String INPUT = "/지오비전/유동인구/2015/월별_시간대";
//	private static final String INPUT = "구역/연속지적도_2017";
	static final String RESULT = "tmp/result";
	
	public static void run(MarmotSpark marmot) throws Exception {
		SparkDataSet ds = marmot.getDataSet(INPUT);
		
		ds.read()
//			.project("the_geom,시군구코드,건물본번")
//			.distinct("시군구코드")
			.takeByGroup(Group.ofKeys("시군구코드").orderBy("건물본번:desc"), 2)
			.store(RESULT, StoreDataSetOptions.FORCE);
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
		run(marmot);
		
		System.out.println("elapsed=" + watch.getElapsedMillisString());
	}
}
