package test;

import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.JoinOptions.RIGHT_OUTER_JOIN;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Envelope;

import marmot.ConfigurationBuilder;
import marmot.optor.AggregateFunction;
import marmot.optor.StoreDataSetOptions;
import marmot.plan.Group;
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
public class TestEnergy {
	private static final String LAND_PRICES_2018 = "토지/개별공시지가_2018";
	private static final String GAS = "건물/건물에너지/가스사용량";
	private static final String ELECTRO = "건물/건물에너지/전기사용량";
	private static final int YEAR = 2018;
	private static final List<String> MONTH_COLS
											= FStream.range(1, 12)
													.map(idx -> String.format("month%02d", idx))
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
										.master("local[2]")
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
		
		MarmotRDD cadastral = ds.read().project("the_geom,고유번호 as pnu").cache();
		
		MarmotRDD sumGasYear = sumMonthGasYear(marmot, grid, cadastral);
		sumGasYear.store("tmp/result", StoreDataSetOptions.FORCE);
		
		MarmotRDD sumElectroYear = sumMonthElectroYear(marmot, grid, cadastral);
//		sumElectroYear.store("tmp/result", StoreDataSetOptions.FORCE);
	}
	
	private static MarmotRDD sumMonthGasYear(MarmotSpark marmot, SquareGrid grid, MarmotRDD cadastral) {
		SparkDataSet ds = marmot.getDataSet(GAS);

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
				.updateScript("사용량 = Math.max(사용량, 0)")
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
				.updateScript("사용량 = Math.max(사용량, 0)")
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
