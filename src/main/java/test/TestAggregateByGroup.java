package test;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.MAX;
import static marmot.optor.AggregateFunction.MIN;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

import marmot.ConfigurationBuilder;
import marmot.plan.Group;
import marmot.spark.MarmotRDD;
import marmot.spark.MarmotSpark;
import marmot.spark.dataset.SparkDataSet;
import utils.StopWatch;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestAggregateByGroup {
//	private static final String INPUT = "구역/집계구";
	private static final String INPUT = "주소/건물POI";
//	private static final String INPUT = "/지오비전/유동인구/2015/월별_시간대";
//	private static final String INPUT = "구역/연속지적도_2017";
	static final String RESULT = "tmp/result";
	
	public static MarmotRDD run(MarmotSpark marmot) throws Exception {
		SparkDataSet ds = marmot.getDataSet(INPUT);
		
		MarmotRDD rdd = ds.read()
							.aggregateByGroup(Group.ofKeys("시군구코드"),
												COUNT(), MIN("건물본번"), MAX("건물본번"))
							.cache();
		System.out.println("count=" + rdd.count());
		
		return rdd;
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
		MarmotRDD rdd = run(marmot);
		
		rdd.toDataFrame().show();
		
		System.out.println("elapsed=" + watch.getElapsedMillisString());
	}
}
