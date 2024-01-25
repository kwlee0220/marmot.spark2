package test;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.StoreDataSetOptions.FORCE;
import static marmot.plan.SpatialJoinOptions.OUTPUT;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import marmot.ConfigurationBuilder;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.command.ClusterSpatiallyOptions;
import marmot.plan.Group;
import marmot.plan.SpatialJoinOptions;
import marmot.spark.MarmotRDD;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.spark.dataset.SparkDataSet;
import marmot.spark.geo.cluster.QuadSpacePartitioner;
import utils.StopWatch;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestBroadcastJoin {
	private static final String PARAM = "구역/행정동코드";
	private static final String TEST_PARAM = "tmp/param";
	private static final String INPUT = "교통/dtg_201609";
//	private static final String INPUT = "나비콜/택시로그";
//	private static final String INPUT = "POI/주유소_가격";
	private static final String RESULT = "tmp/result";
	
	public static void run(MarmotSpark marmot) throws Exception {
		StopWatch watch = StopWatch.start();
		
		prepareParam(marmot);
		runByMarmotRDD(marmot);
		
		System.out.printf("elapsed=%s%n", watch.getElapsedSecondString());
	}
	
	public static final void main(String... args) throws Exception {
		MarmotSpark.configureLog4j();
		
		StopWatch watch = StopWatch.start();
		
		Configuration conf = new ConfigurationBuilder()
								.forLocalMR()
								.build();
		
		// create a SparkSession
		SparkConf sconf = new SparkConf()
							.registerKryoClasses(new Class[] {
								Object[].class, RecordLite.class,
							});
		SparkSession spark = SparkSession.builder()
										.appName("marmot_spark_server")
										.master("local[*]")
										.config("spark.driver.host", "localhost")
										.config("spark.driver.maxResultSize", "5g")
										.config("spark.executor.memory", "5g")
//										.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//										.config("spark.kryo.registrationRequired", "true")
//										.config("spark.kryo.registrator", MyRegistrator.class.getName())
//										.config("spark.kryoserializer.buffer.max", "1gb")
//										.config("spark.kryoserializer.buffer", "64mb")
//										.config(sconf)
										.getOrCreate();
		
		MarmotSpark marmot = new MarmotSpark(conf, spark);
		
		run(marmot);
	}
	
	public static void joinByDataSet(MarmotSpark marmot) throws Exception {
		SparkDataSet input = marmot.getDataSet(INPUT);
		
		marmot.getDataSet(TEST_PARAM)
				.cluster(ClusterSpatiallyOptions.FORCE);
		SparkDataSet param = marmot.getDataSet(TEST_PARAM);
		
		input.read()
			.spatialJoin(param, input.getGeometryColumnInfo(), OUTPUT("right.hcode"))
			.aggregateByGroup(Group.ofKeys("hcode"), COUNT())
			.store(RESULT, FORCE);
	}
	
	public static void runByBroadcast(MarmotSpark marmot) throws Exception {
		SparkDataSet param = marmot.getDataSet(TEST_PARAM);
		SparkDataSet input = marmot.getDataSet(INPUT);
		input.read()
			.spatialJoin(param.read().broadcast(), input.getGeometryColumnInfo(),
							SpatialJoinOptions.OUTPUT("right.hcode"))
			.aggregateByGroup(Group.ofKeys("hcode"), COUNT())
			.store(RESULT, FORCE);
	}
	
	public static void runByMarmotRDD(MarmotSpark marmot) throws Exception {
		MarmotRDD param = marmot.getDataSet(TEST_PARAM).read();
		List<String> qkeys = marmot.getDataSet("구역/연속지적도_2017").getSpatialQuadKeyAll();
		QuadSpacePartitioner partitioner = QuadSpacePartitioner.from(qkeys);
		
		SparkDataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		
		marmot.getDataSet(INPUT)
				.read()
				.spatialJoin(param, gcInfo, partitioner, OUTPUT("right.hcode"))
				.aggregateByGroup(Group.ofKeys("hcode"), COUNT())
				.store(RESULT, FORCE);
	}
	
	private static final void prepareParam(MarmotSpark marmot) throws Exception {
		SparkDataSet input = marmot.getDataSet(INPUT);
		String toSrid = input.getSrid();
		
		marmot.getDataSet(PARAM)
				.read()
				.project("the_geom,hcode")
				.transformCrs(toSrid)
				.store(TEST_PARAM, FORCE);
	}
}
