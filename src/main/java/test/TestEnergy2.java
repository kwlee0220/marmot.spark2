package test;

import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.JoinOptions.RIGHT_OUTER_JOIN;
import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Envelope;

import marmot.ConfigurationBuilder;
import marmot.optor.AggregateFunction;
import marmot.plan.Group;
import marmot.spark.MarmotDataFrame;
import marmot.spark.MarmotRDD;
import marmot.spark.MarmotSpark;
import marmot.spark.dataset.SparkDataSet;
import marmot.spark.optor.geo.SquareGrid;
import utils.Size2d;
import utils.StopWatch;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestEnergy2 {
	private static final String LAND_PRICES_2018 = "토지/개별공시지가_2018";
	private static final String GAS = "건물/건물에너지/가스사용량";
	private static final String ELECTRO = "건물/건물에너지/전기사용량";
	private static final int YEAR = 2018;
	private static final String OUTPUT_GAS_MAP_DIR = "tmp/anyang/gas/map/" + YEAR;
	private static final String OUTPUT_GAS_GRID_DIR = "tmp/anyang/gas/grid/" + YEAR;
	private static final List<String> MONTH_COLS
											= FStream.range(1, 13)
													.map(idx -> String.format("month_%02d", idx))
													.toList();
	
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
		SparkDataSet ds = marmot.getDataSet(LAND_PRICES_2018);
		
		Envelope bounds = ds.getBounds();
		Size2d cellSize = new Size2d(1000, 1000);
		SquareGrid grid = new SquareGrid(bounds, cellSize);
		String monthCols = FStream.from(MONTH_COLS).join(',');
		String outCols = String.format("left.{%s},right.*", monthCols);
		
		MarmotDataFrame cadastral = ds.read().toDataFrame()
										.project("the_geom,고유번호 as pnu")
										.cache();
		
		MarmotDataFrame sumGasYear = sumMonthGasYear(marmot, grid, cadastral).cache();
//		sumGasYear.store("tmp/result", StoreDataSetOptions.FORCE);
//		MarmotDataFrame sumGasYear = marmot.getDataSet("tmp/result").read().toDataFrame();
		gridAnalysis(sumGasYear, grid);
		splitBySido(sumGasYear);
//		
//		MarmotRDD sumElectroYear = sumMonthElectroYear(marmot, grid, cadastral);
////		sumElectroYear.store("tmp/result", StoreDataSetOptions.FORCE);
	}
	
	private static MarmotDataFrame sumMonthGasYear(MarmotSpark marmot, SquareGrid grid,
													MarmotDataFrame cadastral) {
		SparkDataSet ds = marmot.getDataSet(GAS);

		String filterPred = String.format("year == %d", YEAR);
		String monthCols = FStream.from(MONTH_COLS).join(',');
		String outCols = String.format("right.*,left.{%s}", monthCols);
		
		return ds.read()
				.defineColumn("year:short", "사용년월.substring(0, 4)")
				.filterScript(filterPred)
				.defineColumn("month:short", "사용년월.substring(4, 6)")
				.filterScript("사용량 >= 0")
				.aggregateByGroup(Group.ofKeys("pnu,month"), SUM("사용량").as("usage"))
				.project("pnu, month,  usage")
				.defineColumn("month:string", "String.format('month_%02d', month)")
				.pivot("pnu", "month", SUM("usage"))
				.hashJoin("pnu", cadastral, "pnu", outCols, RIGHT_OUTER_JOIN)
				.fillNa(monthCols, 0);
	}
	
	private static void gridAnalysis(MarmotDataFrame mapped, SquareGrid grid) {
		String updateExpr = FStream.from(MONTH_COLS)
									.map(c -> String.format("%s *= ratio", c))
									.join("; ");
		List<AggregateFunction> aggrs = FStream.from(MONTH_COLS).map(c -> SUM(c).as(c)).toList();
		
		MarmotRDD mrdd = mapped.assignGridCell(grid, false)
								.intersection("cell_geom", "overlap")
								.defineColumn("ratio:double", "(ST_Area(overlap)/ST_Area(the_geom))")
								.updateScript(updateExpr)
								.aggregateByGroup(Group.ofKeys("cell_id").withTags("cell_geom,cell_pos"), aggrs)
								.expand("x:long,y:long", "x = cell_pos.getX(); y = cell_pos.getY()")
								.project("cell_geom as the_geom, x, y, *-{cell_geom,x,y}")
								.cache();
		
		for ( int month = 1; month <= 12; ++month ) {
			String output = String.format("%s/%d", OUTPUT_GAS_GRID_DIR, month);
			String projectExpr = String.format("the_geom,x,y,month_%02d as value", month);
			
			MarmotRDD projected = mrdd.project(projectExpr);
			projected.store(output, FORCE);
		}
	}
	
	
	private static void splitBySido(MarmotDataFrame mapped) {
		mapped.defineColumn("sido:string", "pnu.substring(0, 2)")
				.splitToPair("sido")
				.partitionByKey(51)
				.store(OUTPUT_GAS_MAP_DIR, FORCE);
	}

	private static MarmotRDD sumMonthElectroYear(MarmotSpark marmot, SquareGrid grid,
												MarmotRDD cadastral) {
		SparkDataSet ds = marmot.getDataSet(ELECTRO);

		String filterPred = String.format("year == %d", YEAR);
		String monthCols = FStream.from(MONTH_COLS).join(',');
		String outCols = String.format("right.*,left.{%s}", monthCols);
		String updateExpr = FStream.from(MONTH_COLS)
									.map(c -> String.format("%s *= ratio", c))
									.join("; ");
		List<AggregateFunction> aggrs = FStream.from(MONTH_COLS).map(c -> SUM(c).as(c)).toList();
		
		return ds.read()
				.defineColumn("year:short", "사용년월.substring(0, 4)")
				.filterScript(filterPred)
				.defineColumn("month:short", "사용년월.substring(4, 6)")
				.filterScript("사용량 >= 0")
				.aggregateByGroup(Group.ofKeys("pnu,month"), SUM("사용량").as("usage"))
				.project("pnu, month,  usage")
				.defineColumn("month:string", "String.format('month%02d', month)")
				.toDataFrame()
				
				.pivot("pnu", "month", SUM("usage"))
				.hashJoin("pnu", cadastral.toDataFrame(), "pnu", outCols, RIGHT_OUTER_JOIN)
				.fillNa(monthCols, 0)
				.toRDD()
				
				.assignGridCell(grid, false)
				.intersection("cell_geom", "overlap")
				.defineColumn("ratio:double", "(ST_Area(overlap) /  ST_Area(the_geom))")
				.updateScript(updateExpr)
				.aggregateByGroup(Group.ofKeys("cell_id").withTags("cell_geom,cell_pos"), aggrs)
				.expand("x:long,y:long", "x = cell_pos.getX(); y = cell_pos.getY()")
				.project("cell_geom as the_geom, x, y, *-{cell_geom,x,y}");
	}
}
