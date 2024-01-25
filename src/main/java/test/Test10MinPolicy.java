package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

import marmot.ConfigurationBuilder;
import marmot.io.MarmotFileWriteOptions;
import marmot.optor.StoreDataSetOptions;
import marmot.plan.SpatialJoinOptions;
import marmot.spark.MarmotRDD;
import marmot.spark.MarmotSpark;
import marmot.spark.dataset.SparkDataSet;
import utils.StopWatch;
import utils.UnitUtils;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Test10MinPolicy {
	private static final String INPUT1 = "POI/노인복지시설";
	private static final String INPUT2 = "주민/인구밀도_2000";
	private static final String INPUT3 = "구역/행정동코드";
	private static final String INPUT4 = "구역/연속지적도_2017";
	static final String TEMP1 = "tmp/10min/step01";
	static final String TEMP2 = "tmp/10min/step02";
	static final String TEMP3 = "tmp/10min/step03";
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
										.master("local[3]")
										.config("spark.driver.host", "localhost")
										.config("spark.driver.maxResultSize", "5g")
										.config("spark.executor.memory", "5g")
										.getOrCreate();
		MarmotSpark marmot = new MarmotSpark(conf, spark);
		
		run(marmot);
		
		System.out.printf("elapsed time=%s%n", watch.getElapsedMillisString());
	}
	
	public static void run(MarmotSpark marmot) throws Exception {
		SparkDataSet ds1 = marmot.getDataSet(INPUT1);
		SparkDataSet ds2 = marmot.getDataSet(INPUT2);
		SparkDataSet ds3 = marmot.getDataSet(INPUT3);
		SparkDataSet ds4 = marmot.getDataSet(INPUT4);
		
		MarmotRDD rdd1 = ds1.read()
							.filterScript("induty_nm == '경로당'")
							.buffer(400);
		MarmotRDD rdd2 = ds2.read()
							.filterScript("value >= 10000")
							.centroid(false);
		MarmotRDD rdd3 = ds3.read()
							.spatialSemiJoin(rdd2, ds3.getSpatialQuadKeyAll(),
											SpatialJoinOptions.DEFAULT);
		
		ds4.read()
			.spatialSemiJoin(rdd1, ds4.getSpatialQuadKeyAll(), SpatialJoinOptions.NEGATED)
			.arcClip(rdd3, ds4.getSpatialQuadKeyAll())
			.store(RESULT, StoreDataSetOptions.FORCE);
	}
	
	public static void run2(MarmotSpark marmot) throws Exception {
		SparkDataSet ds1 = marmot.getDataSet(INPUT1);
		SparkDataSet ds2 = marmot.getDataSet(INPUT2);
		SparkDataSet ds3 = marmot.getDataSet(INPUT3);
		SparkDataSet ds4 = marmot.getDataSet(INPUT4);
		
		ds1.read()
			.filterScript("induty_nm == '경로당'")
			.buffer(400)
			.partitionByQuadkey(0.1, null, UnitUtils.parseByteSize("64mb"))
			.store(TEMP1, FOption.of(1), MarmotFileWriteOptions.FORCE);
		
		ds2.read()
			.filterScript("value >= 10000")
			.centroid(false)
			.partitionByQuadkey(0.1, null, UnitUtils.parseByteSize("64mb"))
			.store(TEMP2, FOption.of(1), MarmotFileWriteOptions.FORCE);
		
		SparkDataSet temp2 = marmot.getDataSet(TEMP2);
		ds3.read()
			.spatialSemiJoin(temp2, SpatialJoinOptions.DEFAULT)
			.partitionByQuadkey(0.1, null, UnitUtils.parseByteSize("64mb"))
			.store(TEMP3, FOption.of(1), MarmotFileWriteOptions.FORCE);
		
		SparkDataSet temp1 = marmot.getDataSet(TEMP1);
		SparkDataSet temp3 = marmot.getDataSet(TEMP3);
		ds4.read()
			.spatialSemiJoin(temp1, SpatialJoinOptions.NEGATED)
			.arcClip(temp3)
			.store(RESULT, StoreDataSetOptions.FORCE);
	}
}
