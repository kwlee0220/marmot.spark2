package marmot.spark;


import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.locationtech.jts.geom.Geometry;

import marmot.GRecordSchema;
import marmot.PlanBuilder;
import marmot.RecordSchema;
import marmot.RecordScript;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.AggregateFunction;
import marmot.optor.JoinOptions;
import marmot.optor.StoreDataSetOptions;
import marmot.plan.GeomOpOptions;
import marmot.plan.Group;
import marmot.spark.optor.AbstractDataFrameFunction;
import marmot.spark.optor.DefineColumn;
import marmot.spark.optor.HashJoin;
import marmot.spark.optor.Project2;
import marmot.spark.optor.SqlFilter;
import marmot.spark.optor.Update;
import marmot.spark.optor.Update2;
import marmot.spark.optor.geo.AssignSquareGridCell;
import marmot.spark.optor.geo.BufferTransform2;
import marmot.spark.optor.geo.CentroidTransform2;
import marmot.spark.optor.geo.SquareGrid;
import marmot.spark.optor.reducer.AggregateByGroup2;
import utils.CSV;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class MarmotDataFrame implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final MarmotSpark m_marmot;
	private final GRecordSchema m_gschema;
	private final Dataset<Row> m_df;
	
	public MarmotDataFrame(MarmotSpark marmot, GRecordSchema geomSchema, Dataset<Row> df) {
		m_marmot = marmot;
		m_gschema = geomSchema;
		m_df = df;
	}
	
	public MarmotSpark getMarmotSpark() {
		return m_marmot;
	}
	
	public GRecordSchema getGRecordSchema() {
		return m_gschema;
	}
	
	public RecordSchema getRecordSchema() {
		return m_gschema.getRecordSchema();
	}
	
	public FOption<GeometryColumnInfo> getGeometryColumnInfo() {
		return m_gschema.getGeometryColumnInfo();
	}
	public GeometryColumnInfo assertGeometryColumnInfo() {
		return m_gschema.assertGeometryColumnInfo();
	}
	
	public String getGeometryColumn() {
		return assertGeometryColumnInfo().name();
	}
	
	public String getSrid() {
		return assertGeometryColumnInfo().srid();
	}
	
	public Dataset<Row> getDataFrame() {
		return m_df;
	}
	
	public MarmotRDD toRDD() {
		return new MarmotRDD(m_marmot, m_gschema, Rows.toJavaRDD(m_df));
	}

	public MarmotDataFrame filterSql(String filterExpr) {
		return apply(new SqlFilter(filterExpr));
	}

	public MarmotDataFrame project(String prjExpr) {
		return apply(new Project2(prjExpr));
	}

	/**
	 * 주어진 레코드 세트에 포함된 레코드에 주어진 이름의 컬럼을 추가한다.
	 * 추가된 컬럼의 값은 {@code null}로 설정된다.
	 * 
	 * @param colDecl	추가될 컬럼의 이름과 타입 정보
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public MarmotRDD defineColumn(String colDecl) {
		Utilities.checkNotNullArgument(colDecl, "colDecl is null");

		return toRDD().apply(new DefineColumn(colDecl));
	}

	/**
	 * 주어진 레코드 세트에 포함된 레코드에 주어진 이름의 컬럼을 추가하고,
	 * 해당 컬럼의 초기값을 설정한다.
	 * 
	 * @param colDecl	추가될 컬럼의 이름과 타입 정보
	 * @param colInit	추가된 컬럼의 초기값 설정식
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public MarmotRDD defineColumn(String colDecl, String colInit) {
		Utilities.checkNotNullArgument(colDecl, "colDecl is null");
		Utilities.checkNotNullArgument(colInit, "colInit is null");

		return toRDD().apply(new DefineColumn(colDecl, RecordScript.of(colInit)));
	}
	
	public MarmotDataFrame updateSql(String colName, String updateExpr) {
		Utilities.checkNotNullArgument(colName, "colName is null");
		Utilities.checkNotNullArgument(updateExpr, "updateExpr is null");

		return apply(new Update2(colName, colName));
	}
	
	public MarmotRDD updateScript(String updateExpr) {
		return updateScript(RecordScript.of(updateExpr));
	}
	/**
	 * 주어진 갱신 표현식을 이용하여, 레코드 세트에 포함된 모든 레코드에 반영시킨다.
	 * 
	 * @param expr	갱신 표현식
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public MarmotRDD updateScript(RecordScript expr) {
		return toRDD().apply(new Update(expr));
	}
	
	public MarmotDataFrame aggregateByGroup(Group grouper, AggregateFunction... aggrs) {
		return aggregateByGroup(grouper, Arrays.asList(aggrs));
	}
	public MarmotDataFrame aggregateByGroup(Group grouper, List<AggregateFunction> aggrs) {
		return apply(new AggregateByGroup2(grouper, aggrs));
	}
	
	public MarmotDataFrame hashJoin(String leftJoinCols, MarmotDataFrame right, String rightJoinCols,
									String outputCols, JoinOptions jopts) {
		HashJoin op = new HashJoin(leftJoinCols, rightJoinCols, outputCols, jopts);
		op.initialize(m_marmot, m_gschema, right.m_gschema);
		return op.apply(this, right);
	}
	
	public MarmotDataFrame fillNa(String cols, long value) {
		String[] columns = CSV.parseCsvAsArray(cols, ',');
		return new MarmotDataFrame(m_marmot, m_gschema, m_df.na().fill(value, columns));
	}

	public MarmotDataFrame buffer(double dist) {
		return apply(new BufferTransform2(dist, GeomOpOptions.DEFAULT));
	}

	public MarmotDataFrame centroid() {
		return apply(new CentroidTransform2(GeomOpOptions.DEFAULT));
	}
	
	/**
	 * 입력 레코드 세트에 포함된 각 레코드에 주어진 크기의 사각형 Grid 셀 정보를 부여한다.
	 * 
	 * 출력 레코드에는 '{@code cell_id}', '{@code cell_pos}', 그리고 '{@code cell_geom}'
	 * 컬럼이 추가된다. 각 컬럼 내용은 각각 다음과 같다.
	 * <dl>
	 * 	<dt>cell_id</dt>
	 * 	<dd>부여된 그리드 셀의 고유 식별자. {@code long} 타입</dd>
	 * 	<dt>cell_pos</dt>
	 * 	<dd>부여된 그리드 셀의 x/y 좌표 . {@code GridCell} 타입</dd>
	 * 	<dt>cell_geom</dt>
	 * 	<dd>부여된 그리드 셀의 공간 객체 . {@code Polygon} 타입</dd>
	 * </dl>
	 * 
	 * 입력 레코드의 공간 객체가 {@code null}이거나 {@link Geometry#isEmpty()}가
	 * {@code true}인 경우, 또는 공간 객체의 위치가 그리드 전체 영역 밖에 있는 레코드의
	 * 처리는 {@code ignoreOutside} 인자에 따라 처리된다.
	 * 만일 {@code ignoreOutside}가 {@code false}인 경우 영역 밖 레코드의 경우는
	 * 출력 레코드 세트에 포함되지만, '{@code cell_id}', '{@code cell_pos}',
	 * '{@code cell_geom}'의 컬럼 값은 {@code null}로 채워진다.
	 * 
	 * @param geomCol	Grid cell 생성에 사용할 공간 컬럼 이름.
	 * @param grid		생성할 격자 정보.
	 * @param assignOutside	입력 공간 객체가 주어진 그리드 전체 영역에서 벗어난 경우
	 * 						무시 여부. 무시하는 경우는 결과 레코드 세트에 포함되지 않음.
	 * @return	명령이 추가된 {@link PlanBuilder} 객체.
	 */
	public MarmotRDD assignGridCell(SquareGrid grid, boolean assignOutside) {
		Utilities.checkNotNullArgument(grid, "SquareGrid is null");
		
		return toRDD().apply(new AssignSquareGridCell(grid, assignOutside));
	}
	
	public MarmotDataFrame pivot(String groupCols, String pivotCol, AggregateFunction aggr) {
		org.apache.spark.sql.Column[] keys = CSV.parseCsv(groupCols)
												.map(name -> functions.col(name))
												.toArray(org.apache.spark.sql.Column.class);
		RelationalGroupedDataset pivotted = m_df.groupBy(keys).pivot(pivotCol);
		Dataset<Row> rows;
		switch ( aggr.m_type ) {
			case SUM:
				rows = pivotted.sum(aggr.m_aggrColumn);
				break;
			default:
				throw new IllegalArgumentException("unsupported pivot aggregation: " + aggr);
		}
		
		return new MarmotDataFrame(m_marmot, m_gschema.derive(Rows.toRecordSchema(rows)), rows);
	}
	
	public long count() {
		return m_df.count();
	}
	
	public MarmotDataFrame cache() {
		return new MarmotDataFrame(m_marmot, m_gschema, m_df.cache());
	}
	
	public void store(String dsId, StoreDataSetOptions opts) {
		toRDD().store(dsId, opts);
	}
	
	public void show() {
		m_df.show();
	}
	
	public MarmotDataFrame apply(AbstractDataFrameFunction func) {
		func.initialize(m_marmot, m_gschema);
		return func.apply(this);
	}
	
	@Override
	public String toString() {
		return String.format("%s: schema=%s", getClass().getSimpleName(), m_gschema);
	}
}
