package test;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.AggregateFunction.UNION_GEOM;
import static marmot.optor.JoinOptions.INNER_JOIN;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Envelope;

import com.google.common.collect.Lists;

import marmot.ConfigurationBuilder;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.JoinOptions;
import marmot.optor.StoreDataSetOptions;
import marmot.plan.Group;
import marmot.plan.SpatialJoinOptions;
import marmot.spark.MarmotDataFrame;
import marmot.spark.MarmotRDD;
import marmot.spark.MarmotSpark;
import marmot.spark.dataset.SparkDataSet;
import marmot.spark.geo.cluster.QuadSpacePartitioner;
import marmot.spark.optor.geo.SquareGrid;
import utils.Size2d;
import utils.StopWatch;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestFindSubwayStations {
	private static final String SID = "구역/시도";
	private static final String STATIONS = "교통/지하철/역사";
	private static final String TAXI_LOG = "나비콜/택시로그";
	private static final String BLOCKS = "지오비전/집계구/2015";
	private static final String FLOW_POP_BYTIME = "지오비전/유동인구/%d/월별_시간대";
	private static final String CARD_BYTIME = "지오비전/카드매출/%d/일별_시간대";
	private static final String RESULT = "분석결과/지하철역사_추천";
//	private static final int[] YEARS = new int[] {2015, 2016, 2017};
	private static final int[] YEARS = new int[] {2015};
	
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
		// 서울지역 지하철 역사의 버퍼 작업을 수행한다.
		MarmotRDD stations = bufferSubway(marmot.getDataSet(STATIONS).read());
		
		// 전국 시도 행정구역 데이터에서 서울특별시 영역만을 추출한다.
		Envelope seoul = getSeoulBoundary(marmot.getDataSet(SID).read());
		
		// 격자별 집계구 비율 계산
		MarmotRDD blocks = calcBlockRatio(marmot, seoul, stations).cache();
		
		// 격자별 택시 승하차 수 집계
		MarmotRDD taxi = gridTaxiLog(marmot, blocks);
//		taxi.store("tmp/taxi", StoreDataSetOptions.FORCE);
//		MarmotRDD taxi = marmot.getDataSet("tmp/taxi").read();
		
		// 격자별 유동인구 집계
		MarmotRDD flowPop = gridFlowPopulation(marmot, blocks);
//		flowPop.store("tmp/flowPop", StoreDataSetOptions.FORCE);
//		MarmotRDD flowPop = marmot.getDataSet("tmp/flowPop").read();
		
		// 격자별 카드매출 집계
		MarmotRDD cards = gridCardSales(marmot, blocks);
//		flowPop.store("tmp/card", StoreDataSetOptions.FORCE);
//		MarmotRDD cards = marmot.getDataSet("tmp/card").read();
		
		// 격자별_유동인구_카드매출_택시승하차_비율 합계
		MarmotRDD result = mergeAll(taxi, flowPop, cards);
		
		// 공간객체 부여하고 저장
		attachGeom(result, blocks).store(RESULT, StoreDataSetOptions.FORCE);
		
		System.out.println("DONE: ");
	}

	static MarmotRDD bufferSubway(MarmotRDD stations) {
		// 서울지역 지하철 역사를 구하고 1km 버퍼를 구한다.
		return stations.filterScript("sig_cd.substring(0,2) == '11'")
						.buffer(1000)
						.project("the_geom");
	}
	
	 static Envelope getSeoulBoundary(MarmotRDD sido) {
		return sido.filterScript("ctprvn_cd == '11'")
					.project(sido.getGeometryColumn())
					.takeFirst()
					.getGeometry(0)
					.getEnvelopeInternal();
	}
		
	static MarmotRDD calcBlockRatio(MarmotSpark marmot, Envelope seoul, MarmotRDD stations) {
		SparkDataSet blocks = marmot.getDataSet(BLOCKS);
		SquareGrid grid = new SquareGrid(seoul, new Size2d(300, 300));
		
		return blocks.read()
					.filterScript("block_cd.substring(0,2) == '11'")
					.differenceJoin(stations, blocks.getSpatialQuadKeyAll())
					.assignGridCell(grid, false)
					.intersection("cell_geom", "overlap")
					.defineColumn("portion:double", "portion = ST_Area(overlap) / ST_Area(cell_geom)")
					.project("overlap as the_geom, cell_id, cell_pos, block_cd, portion");
	}

	static MarmotRDD gridTaxiLog(MarmotSpark marmot, MarmotRDD blocks) {
		SparkDataSet taxi = marmot.getDataSet(TAXI_LOG);
		GeometryColumnInfo gcInfo = taxi.getGeometryColumnInfo();
		QuadSpacePartitioner partitioner = QuadSpacePartitioner.from(taxi.getSpatialQuadKeyAll());
		String outJoinCols = String.format("left.*-{%s},right.*-{%s}", gcInfo.name(),
												blocks.assertGeometryColumnInfo().name());
		return taxi.read()
					.filterScript("status == 1 || status == 2")
					.transformCrs(blocks.getSrid())
					.spatialJoin(blocks, null, partitioner, SpatialJoinOptions.OUTPUT(outJoinCols))
					.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_pos"),
													SUM("portion").as("normalized"))
					.cache()
					.rescale("normalized");
	}

	static MarmotRDD gridFlowPopulation(MarmotSpark marmot, MarmotRDD blocks) {
		List<MarmotRDD> rddList = Lists.newArrayList();
		for ( int year: YEARS ) {
			rddList.add(gridFlowPopulation(marmot, year, blocks));
		}
		
		return MarmotRDD.union(rddList)
						.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_pos"),
											AVG("normalized").as("normalized"));
	}
	
	static MarmotRDD gridFlowPopulation(MarmotSpark marmot, int year, MarmotRDD blocks) {
		String inDsId = String.format(FLOW_POP_BYTIME, year);
		
		return marmot.getDataSet(inDsId)
					// 유동인구를  읽는다.
					.read()
					// 서울지역 데이터만 선택
					.filterScript("ST_StartsWith(block_cd, '11')")
					// 하루동안의 시간대별 평균 유동인구를 계산
					.defineColumn("daily_avg:double", "ST_ArrayAvg(avg)")
					// 집계구별 평균 일간 유동인구 평균 계산
					.aggregateByGroup(Group.ofKeys("block_cd"), AVG("daily_avg"))
					.toDataFrame()
					
					// 격자 정보 부가함
					.hashJoin("block_cd", blocks.toDataFrame(), "block_cd", "right.*,left.avg", INNER_JOIN)
					.toRDD()
					
					// 비율 값 반영
					.updateScript("avg *= portion")
					// 격자별 평균 유동인구을 계산한다.
					.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_pos"),
										SUM("avg").as("normalized"))
					.cache()
					.rescale("normalized");
	}

	static MarmotRDD gridCardSales(MarmotSpark marmot, MarmotRDD blocks) {
		List<MarmotRDD> rddList = Lists.newArrayList();
		for ( int year: YEARS ) {
			rddList.add(gridCardSales(marmot, year, blocks));
		}
		
		return MarmotRDD.union(rddList)
						.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_pos"),
											AVG("normalized").as("normalized"));
	}
	
	static MarmotRDD gridCardSales(MarmotSpark marmot, int year, MarmotRDD blocks) {
		String inDsId = String.format(CARD_BYTIME, year);
		
		return marmot.getDataSet(inDsId)
					// 카드매출 데이터를  읽는다.
					.read()
					// 서울지역 데이터만 선택
					.filterScript("block_cd.startsWith('11')")
					// 하루동안의 카드매출 합계를 계산
					.defineColumn("amount:double", "ST_ArraySum(SALE_AMT)")
					// 집계구별 일간 카드매출 합계 계산
					.aggregateByGroup(Group.ofKeys("block_cd"), AVG("amount").as("amount"))
					.toDataFrame()
					
					// 격자 정보 부가함
					.hashJoin("block_cd", blocks.toDataFrame(), "block_cd", "right.*,left.amount", INNER_JOIN)
					.toRDD()
					
					// 비율 값 반영
					.updateScript("amount *= portion")
					// 격자별 평균 카드매출액을 계산한다.
					.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_pos"),
										SUM("amount").as("normalized"))
					.cache()
					.rescale("normalized");
	}
	
	private static MarmotRDD mergeAll(MarmotRDD taxi, MarmotRDD flowPop, MarmotRDD card) {
		String merge1 = "if ( normalized == null ) {"
					+ "		cell_id = right_cell_id;"
					+ "		normalized = 0;"
					+ "} else if ( right_normalized == null ) {"
					+ "		right_normalized = 0;"
					+ "}"
					+ "normalized = normalized + right_normalized;";
		String merge2 = "if ( normalized == null ) {"
					+ "		cell_id = right_cell_id;"
					+ "		normalized = 0;"
					+ "} else if ( right_normalized == null ) {"
					+ "		right_normalized = 0;"
					+ "}"
					+ "normalized = normalized + right_normalized;";
		
		return card.toDataFrame()
					.hashJoin("cell_id", flowPop.toDataFrame(), "cell_id",
							"left.*,right.{cell_id as right_cell_id, normalized as right_normalized}",
							JoinOptions.FULL_OUTER_JOIN)
					.toRDD()
					
					.updateScript(merge1)
					.toDataFrame()
					
					.project("cell_id,normalized")
					.hashJoin("cell_id", taxi.toDataFrame(), "cell_id",
							"left.*,right.{cell_id as right_cell_id, normalized as right_normalized}",
							JoinOptions.FULL_OUTER_JOIN)
					.toRDD()
					
					.updateScript(merge2)
					.project("cell_id,normalized as value");
	}
	
	private static MarmotDataFrame attachGeom(MarmotRDD merged, MarmotRDD blocks) {
		GeometryColumnInfo gcInfo = blocks.assertGeometryColumnInfo();
		String outJoinCols = String.format("left.{%s,cell_id,cell_pos},right.value", gcInfo.name());
		
		return blocks.toDataFrame()
					// 격자별로 집계구 겹침 영역 union
					.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_pos"),
											UNION_GEOM(gcInfo.name()))
					// 격자별로 결과 데이터 병합
					.hashJoin("cell_id", merged.toDataFrame(), "cell_id", outJoinCols, INNER_JOIN);
	}
	
	static String OUTPUT(String analId) {
		return "/tmp/" + analId;
	}
	
	static String OUTPUT(String analId, int year) {
		return "/tmp/" + analId + "_연도별/" + year;
	}
	
	static String TEMP_OUTPUT(String analId) {
		return "/tmp/" + analId + "_집계";
	}
}
