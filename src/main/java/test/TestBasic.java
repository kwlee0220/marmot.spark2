package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

import marmot.ConfigurationBuilder;
import marmot.optor.StoreDataSetOptions;
import marmot.spark.MarmotDataFrame;
import marmot.spark.MarmotRDD;
import marmot.spark.MarmotSpark;
import marmot.spark.dataset.SparkDataSet;
import utils.StopWatch;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestBasic {
	private static final String QKEY = "구역/연속지적도_2017";
//	private static final String INPUT = "토지/개별공시지가_2018";
//	private static final String INPUT = "tmp/000";
	private static final String INPUT = "주소/건물POI";
//	private static final String INPUT = "나비콜/택시로그";
//	private static final String INPUT = "교통/지하철/서울역사";
//	private static final String INPUT = "지오비전/유동인구/2015/월별_시간대";
//	private static final String INPUT = "구역/집계구";
//	private static final String INPUT = "POI/노인복지시설";
//	private static final String INPUT = "구역/집계구";
//	private static final String INPUT = "POI/주유소_가격";
//	private static final String INPUT = "POI/병원";
//	private static final String INPUT = "구역/행정동코드";
//	private static final String INPUT = "교통/dtg_201609";
	static final String RESULT = "tmp/result";
	
	public static void run(MarmotSpark marmot) throws Exception {
		SparkDataSet ds = marmot.getDataSet(INPUT);
		MarmotRDD rdd = ds.read();
		MarmotDataFrame mdf = rdd.toDataFrame();
		
//		mdf = mdf.buffer(300);
		mdf.store("tmp/result", StoreDataSetOptions.FORCE);
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
//		TestEnergy2.run(marmot);
		run(marmot);
	}
}
