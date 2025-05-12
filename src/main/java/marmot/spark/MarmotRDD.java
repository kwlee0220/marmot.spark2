package marmot.spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import marmot.Column;
import marmot.GRecordSchema;
import marmot.PlanBuilder;
import marmot.RecordSchema;
import marmot.RecordScript;
import marmot.RecordSet;
import marmot.dataset.DataSetType;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.AggregateFunction;
import marmot.optor.StoreDataSetOptions;
import marmot.optor.geo.SpatialRelation;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.optor.support.colexpr.ColumnSelectorFactory;
import marmot.plan.GeomOpOptions;
import marmot.plan.Group;
import marmot.plan.PredicateOptions;
import marmot.plan.SpatialJoinOptions;
import marmot.spark.dataset.SequenceFileDataSet;
import marmot.spark.dataset.SparkDataSet;
import marmot.spark.geo.cluster.QuadSpacePartitioner;
import marmot.spark.module.Rescale;
import marmot.spark.optor.AssignUid;
import marmot.spark.optor.DefineColumn;
import marmot.spark.optor.Expand;
import marmot.spark.optor.Project;
import marmot.spark.optor.RDDConsumer;
import marmot.spark.optor.RDDFilter;
import marmot.spark.optor.RDDFunction;
import marmot.spark.optor.ScriptFilter;
import marmot.spark.optor.StoreAsSequenceFilePartitions;
import marmot.spark.optor.Update;
import marmot.spark.optor.UpdateDataSetInfo;
import marmot.spark.optor.geo.ArcBufferTransform;
import marmot.spark.optor.geo.AssignSquareGridCell;
import marmot.spark.optor.geo.BinarySpatialIntersection;
import marmot.spark.optor.geo.BufferTransform;
import marmot.spark.optor.geo.CentroidTransform;
import marmot.spark.optor.geo.FilterSpatially;
import marmot.spark.optor.geo.SquareGrid;
import marmot.spark.optor.geo.ToGeometryPoint;
import marmot.spark.optor.geo.TransformCrs;
import marmot.spark.optor.geo.cluster.AttachQuadKey;
import marmot.spark.optor.geo.cluster.EstimateQuadKeys;
import marmot.spark.optor.geo.join.ArcClip;
import marmot.spark.optor.geo.join.CoGroupDifferenceJoin;
import marmot.spark.optor.geo.join.CoGroupSpatialBlockJoin;
import marmot.spark.optor.geo.join.CoGroupSpatialSemiJoin;
import marmot.spark.optor.geo.join.SpatialBlockJoin;
import marmot.spark.optor.geo.join.SpatialDifferenceJoin;
import marmot.spark.optor.geo.join.SpatialSemiJoin;
import marmot.spark.optor.reducer.Aggregate;
import marmot.spark.optor.reducer.AggregateByGroup;
import marmot.spark.optor.reducer.TakeByGroup;
import marmot.type.GeometryDataType;
import scala.Tuple2;

import utils.Indexed;
import utils.Tuple;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class MarmotRDD implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final MarmotSpark m_marmot;
	private final GRecordSchema m_gschema;
	private final JavaRDD<RecordLite> m_jrdd;
	
	public MarmotRDD(MarmotSpark marmot, GRecordSchema geomSchema, JavaRDD<RecordLite> jrdd) {
		m_marmot = marmot;
		m_gschema = geomSchema;
		m_jrdd = jrdd;
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
	
	public JavaRDD<RecordLite> getJavaRDD() {
		return m_jrdd;
	}
	
	public MarmotDataFrame toDataFrame() {
		JavaRDD<Row> rows = m_jrdd.map(Rows::toRow);
		Dataset<Row> df = Rows.toDataFrame(m_marmot.getSqlContext(), m_gschema.getRecordSchema(), rows);
		
		return new MarmotDataFrame(m_marmot, m_gschema, df);
	}
	
	public FOption<QuadSpacePartitioner> getPartitioner() {
		Partitioner part = m_jrdd.partitioner().orNull();
		if ( part != null ) {
			return (part instanceof QuadSpacePartitioner)
					? FOption.of((QuadSpacePartitioner)part) : FOption.empty();
		}
		else {
			return FOption.empty();
		}
	}
	
	public static JavaPairRDD<RecordLite, RecordLite>
	mapToPair(JavaRDD<RecordLite> input, GRecordSchema gschema, String keyCols) {
		ColumnSelector selector = ColumnSelectorFactory.create(gschema.getRecordSchema(), keyCols);
		
		List<String> keyColList = FStream.from(selector.getColumnSelectionInfoAll())
										.map(info -> info.getColumn().name())
										.toList();
		int[] keyColIdxes = FStream.from(selector.getColumnSelectionInfoAll())
									.mapToInt(info -> info.getColumn().ordinal())
									.toArray();
		int[] valueColIdxs = gschema.getRecordSchema()
									.complement(keyColList)
									.streamColumns()
									.mapToInt(col -> col.ordinal())
									.toArray();
		
		return input.mapToPair(rec -> new Tuple2<>(rec.select(keyColIdxes),
													rec.select(valueColIdxs)));
	}

	public MarmotRDD filterScript(String filterExpr) {
		return apply(new ScriptFilter(RecordScript.of(filterExpr)));
	}

	public MarmotRDD project(String prjExpr) {
		return apply(new Project(prjExpr));
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
		return apply(new Update(expr));
	}
	
	public MarmotRDD expand(String colDecls) {
		Utilities.checkNotNullArgument(colDecls, "colDecls is null");
		
		return apply(new Expand(colDecls));
	}
	public MarmotRDD expand(String colDecls, String colInit) {
		Utilities.checkNotNullArgument(colDecls, "colDecls is null");
		Utilities.checkNotNullArgument(colInit, "colInit is null");
		
		return expand(colDecls, RecordScript.of(colInit));
	}
	public MarmotRDD expand(String colDecls, RecordScript colInitScript) {
		Utilities.checkNotNullArgument(colDecls, "colDecls is null");
		Utilities.checkNotNullArgument(colInitScript, "colInitScript is null");
		
		return apply(new Expand(colDecls).setInitializer(colInitScript));
	}
	
	public MarmotRDD sample(double ratio) {
		return new MarmotRDD(m_marmot, m_gschema, m_jrdd.sample(false, ratio));
	}
	
	public MarmotRDD filter(SpatialRelation rel, Geometry key, PredicateOptions opts) {
		return apply(new FilterSpatially(rel, key, opts));
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

		return apply(new DefineColumn(colDecl));
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

		return defineColumn(colDecl, RecordScript.of(colInit));
	}

	/**
	 * 주어진 레코드 세트에 포함된 레코드에 주어진 이름의 컬럼을 추가하고,
	 * 해당 컬럼의 초기값을 설정한다.
	 * 
	 * @param colDecl	추가될 컬럼의 이름과 타입 정보
	 * @param colInitScript	추가된 컬럼의 초기값 설정식
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public MarmotRDD defineColumn(String colDecl, RecordScript colInitScript) {
		Utilities.checkNotNullArgument(colDecl, "colDecl is null");

		return apply(new DefineColumn(colDecl, colInitScript));
	}

	/**
	 * 입력 레코드 세트에 포함된 모든 레코드에 유일 식별자 컬럼을 추가한다.
	 * <p>
	 * 부여된 식별자는 {@code long} 타입으로 정의된다. 식별자는 일반적으로 0부터 순차적으로
	 * 커지는 값이 부여되지만, 반드시 1씩 증가되는 값이 부여되지는 않는다.
	 * 
	 * @param uidColName	식별자 컬럼 이름.
	 * @return 연산이 추가된 {@link PlanBuilder} 객체.
	 */
	public MarmotRDD assignUid(String uidColName) {
		Utilities.checkNotNullArgument(uidColName, "uid column is null");
		
		return apply(new AssignUid(uidColName));
	}
	
	public MarmotRDD aggregate(AggregateFunction... aggrs) {
		return new Aggregate(aggrs).apply(this);
	}
	
	public MarmotRDD aggregateByGroup(Group grouper, AggregateFunction... aggrs) {
		return apply(new AggregateByGroup(grouper, Arrays.asList(aggrs)));
	}
	public MarmotRDD aggregateByGroup(Group grouper, List<AggregateFunction> aggrs) {
		return apply(new AggregateByGroup(grouper, aggrs));
	}
	
	/**
	 * 입력 레코드들을 {@code group}을 기준으로 그룹핑하고, 각 그룹에 포함된
	 * 레코드들 중에서 주어진 갯수만큼만 뽑은 레코드 세트를 생성시킨다.
	 * <p>
	 * 그룹에 포함된 레코드의 수가 주어진 {@code count}보다 작은 경우는 해당 레코드들만
	 * 선택된다.
	 * 
	 * @param group	그룹 기준
	 * @param count	각 그룹에서 선택할 레코드의 수
	 * @return 작업이 추가된 {@link MarmotRDD} 객체.
	 */
	public MarmotRDD takeByGroup(Group group, int count) {
		return apply(new TakeByGroup(group, count));
	}
	
	public MarmotDataFrame pivot(String groupCols, String pivotCol, AggregateFunction aggr) {
		return toDataFrame().pivot(groupCols, pivotCol, aggr);
	}

	/**
	 * 입력 레코드 세트에 포함된 레코드들에서 중복된 레코드를 제거한 레코드세트를 출력하는
	 * 명령을 추가한다.
	 * 
	 * @param keyCols	중복 레코드 여부를 판단할 키 컬럼 이름 (리스트).
	 * @return 작업이 추가된 {@link PlanBuilder} 객체.
	 */
	public MarmotRDD distinct(String keyCols) {
		return takeByGroup(Group.ofKeys(keyCols), 1);
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

		return apply(new AssignSquareGridCell(grid, assignOutside));
	}
	
	public MarmotRDD transformCrs(String toSrid) {
		if ( !getSrid().equals(toSrid) ) {
			return apply(new TransformCrs(toSrid));
		}
		else {
			return this;
		}
	}

	public MarmotRDD centroid(boolean inside) {
		CentroidTransform trans = new CentroidTransform(inside, GeomOpOptions.DEFAULT);
		return apply(trans);
	}

	public MarmotRDD buffer(double dist) {
		BufferTransform trans = new BufferTransform(dist, GeomOpOptions.DEFAULT);
		return apply(trans);
	}
	
	public MarmotRDD arcBuffer(double dist, boolean dissolve, GeomOpOptions opts) {
		return apply(new ArcBufferTransform(dist, dissolve, opts));
	}

	public MarmotRDD toPoint(String xCol, String yCol, GeometryColumnInfo gcInfo) {
		return apply(new ToGeometryPoint(xCol, yCol, gcInfo));
	}

	public MarmotRDD intersection(String paramGeomCol, String outputGeomCol,
									GeometryDataType outputGeomType) {
		Utilities.checkNotNullArgument(paramGeomCol, "parameter Geometry column name");
		Utilities.checkNotNullArgument(outputGeomCol, "output Geometry column name");
		Utilities.checkNotNullArgument(outputGeomType, "output Geometry column type");

		String geomCol = m_gschema.getGeometryColumnName();
		return apply(new BinarySpatialIntersection(geomCol, paramGeomCol, outputGeomCol,
													FOption.ofNullable(outputGeomType)));
	}
	public MarmotRDD intersection(String paramGeomCol, String outputGeomCol) {
		Utilities.checkNotNullArgument(paramGeomCol, "parameter Geometry column name");
		Utilities.checkNotNullArgument(outputGeomCol, "output Geometry column name");

		String geomCol = m_gschema.getGeometryColumnName();
		return apply(new BinarySpatialIntersection(geomCol, paramGeomCol, outputGeomCol,
													FOption.empty()));
	}

	public MarmotRDD attachQuadKey(List<String> qkeys, @Nullable Envelope validBounds,
										boolean bindOutlier, boolean bindOnlyToOwner) {
		AttachQuadKey attach = new AttachQuadKey(qkeys, validBounds, bindOutlier, bindOnlyToOwner);
		return apply(attach);
	}
	
	public List<String> estimateQuadKeys(double sampleRatio, @Nullable Envelope validRange,
											long clusterSize) {
		EstimateQuadKeys estimate = new EstimateQuadKeys(sampleRatio)
										.setValidBounds(validRange)
										.setClusterSize(clusterSize);
		return estimate.apply(this)
						.getJavaRDD()
						.map(rec -> rec.getString(0))
						.collect();
	}

	public QuadKeyRDD partitionByQuadkey(QuadSpacePartitioner partitioner, boolean bindOutlier) {
		int ncols = m_gschema.getColumnCount();
		List<String> qkeys = partitioner.getQuadKeyAll();
		JavaPairRDD<String,RecordLite> pairs
				= apply(new AttachQuadKey(qkeys, null, bindOutlier, false))
					.getJavaRDD()
					.mapToPair(rec -> new Tuple2<>(rec.getString(ncols), rec.copyOfRange(0, ncols)))
					.partitionBy(partitioner);
		return new QuadKeyRDD(m_marmot, m_gschema, qkeys, pairs);
	}

	public QuadKeyRDD partitionByQuadkey(double sampleRatio, @Nullable Envelope validRange,
											long clusterSize) {
		MarmotRDD cached = cache();
		List<String> qkeys = cached.estimateQuadKeys(sampleRatio, validRange, clusterSize);
		return cached.partitionByQuadkey(QuadSpacePartitioner.from(qkeys), true);
	}
	
	public MarmotRDD spatialJoin(MarmotRDD right, GeometryColumnInfo gcInfo,
								QuadSpacePartitioner partitioner, SpatialJoinOptions opts) {
		CoGroupSpatialBlockJoin op = new CoGroupSpatialBlockJoin(gcInfo, opts);
		op.initialize(m_marmot, m_gschema, right.getGRecordSchema());
		return new MarmotRDD(m_marmot, op.getGRecordSchema(), op.apply(this, right, partitioner));
	}
	public MarmotRDD spatialJoin(Broadcast<Tuple2<GRecordSchema, List<RecordLite>>> right,
								GeometryColumnInfo gcInfo, SpatialJoinOptions opts) {
		CoGroupSpatialBlockJoin op = new CoGroupSpatialBlockJoin(gcInfo, opts);
		op.initialize(m_marmot, m_gschema, right.value()._1);
		return new MarmotRDD(m_marmot, op.getGRecordSchema(), op.apply(this, right));
	}
	public MarmotRDD spatialJoin(SparkDataSet right, GeometryColumnInfo gcInfo,
								SpatialJoinOptions opts) {
		return SpatialBlockJoin.apply(this, right, gcInfo, opts);
	}
	
	public MarmotRDD spatialSemiJoin(SparkDataSet right, SpatialJoinOptions opts) {
		return apply(new SpatialSemiJoin(right, opts));
	}
	public MarmotRDD spatialSemiJoin(MarmotRDD right, List<String> quadKeys,
										SpatialJoinOptions opts) {
		CoGroupSpatialSemiJoin op = new CoGroupSpatialSemiJoin(opts);
		op.initialize(m_marmot, m_gschema, right.getGRecordSchema());
		return new MarmotRDD(m_marmot, op.getGRecordSchema(), op.apply(this, right, quadKeys));
	}
	
	public MarmotRDD arcClip(SparkDataSet right) {
		return apply(new ArcClip(right));
	}
	public MarmotRDD arcClip(MarmotRDD right, List<String> quadKeys) {
		CoGroupSpatialSemiJoin op = new CoGroupSpatialSemiJoin(SpatialJoinOptions.DEFAULT);
		op.initialize(m_marmot, m_gschema, right.getGRecordSchema());
		return new MarmotRDD(m_marmot, op.getGRecordSchema(), op.apply(this, right, quadKeys));
	}

	public MarmotRDD differenceJoin(SparkDataSet paramDs) {
		return apply(new SpatialDifferenceJoin(paramDs, SpatialJoinOptions.DEFAULT));
	}
	public MarmotRDD differenceJoin(MarmotRDD right, List<String> quadKeys) {
		CoGroupDifferenceJoin op = new CoGroupDifferenceJoin(SpatialJoinOptions.DEFAULT);
		op.initialize(m_marmot, m_gschema, right.getGRecordSchema());
		return CoGroupDifferenceJoin.apply(this, right, quadKeys, SpatialJoinOptions.DEFAULT);
	}
	public MarmotRDD differenceJoin(MarmotRDD right) {
		CoGroupDifferenceJoin op = new CoGroupDifferenceJoin(SpatialJoinOptions.DEFAULT);
		op.initialize(m_marmot, m_gschema, right.getGRecordSchema());
		SpatialJoinOptions opts = SpatialJoinOptions.DEFAULT;
		
		QuadSpacePartitioner partitioner1 = getPartitioner().getOrNull();
		QuadSpacePartitioner partitioner2 = right.getPartitioner().getOrNull();
		if ( partitioner1 != null && partitioner2 != null ) {
			List<String> qkeys1 = partitioner1.getQuadKeyAll();
			List<String> qkeys2 = partitioner2.getQuadKeyAll();
			if ( qkeys1.size() >= qkeys2.size() ) {
				return CoGroupDifferenceJoin.apply(this, right, partitioner1, opts);
			}
			else {
				return CoGroupDifferenceJoin.apply(this, right, partitioner2, opts);
			}
		}
		else if ( partitioner1 != null && partitioner2 == null ) {
			return CoGroupDifferenceJoin.apply(this, right, partitioner1, opts);
		}
		else if ( partitioner1 == null && partitioner2 != null ) {
			return CoGroupDifferenceJoin.apply(this, right, partitioner2, opts);
		}
		else {
			throw new IllegalStateException("no QuadSpacePartitioner");
		}
	}
	
	public void store(String dsId, StoreDataSetOptions opts) {
		// option에 GeometryColumnInfo가 없더라도 RDD에 공간 정보가 존재하는 경우에는
		// option에 포함시킨다.
		if ( opts.geometryColumnInfo().isAbsent() && getGeometryColumnInfo().isPresent() ) {
			opts = opts.geometryColumnInfo(assertGeometryColumnInfo());
		}
		
		// force가 true인 경우에는 무조건 대상 데이터세트를 먼저 삭제한다.
		SparkDataSet ds = m_marmot.getDataSetOrNull(dsId);
		if ( opts.force() && ds != null ) {
			ds.delete();
		}

		SequenceFileDataSet seqDs = null;
		RecordSchema schema = m_gschema.getRecordSchema();
		
		// 'append' 요청이 없으면 새로운 SequenceFileDataSet을 생성한다.
		// 만일 동일 식별자의 SparkDataSet가 이미 존재하는 경우에는 오류가 발생한다.
		if ( !opts.append().getOrElse(false) ) {
			seqDs = (SequenceFileDataSet)m_marmot.createDataSet(dsId, schema, opts.toCreateOptions());
		}
		else if ( seqDs.getType() != DataSetType.FILE ) {
			// append 요청이지만, 기존 파일의 타입이 'FILE'이 아닌 경우는 예외를 발생시킨다.
			throw new IllegalArgumentException("target dataset is not FILE: id=" + dsId);
		}
		
		MarmotRDD dsPartInfos = apply(new StoreAsSequenceFilePartitions(dsId, opts));
		dsPartInfos.apply(new UpdateDataSetInfo(dsId));
	}
	
	public MarmotRDD coalesce(int partCount) {
		return new MarmotRDD(m_marmot, m_gschema, m_jrdd.coalesce(partCount));
	}
	
	public MarmotRDD rescale(String col) {
		return new Rescale(col).apply(this);
	}
	
	public static MarmotRDD union(Iterable<MarmotRDD> rdds) {
		Utilities.checkNotNullArgument(rdds);
		
		MarmotRDD[] rddList = FStream.from(rdds).toArray(MarmotRDD.class);
		Utilities.checkArgument(rddList.length > 0, "empty MarmotRDDs");
		
		GRecordSchema first = rddList[0].getGRecordSchema();
		for ( int i =1; i < rddList.length; ++i ) {
			if ( !first.equals(rddList[i].getGRecordSchema()) ) {
				throw new IllegalArgumentException("incompatable GRecordSchema: 0 <-> " + i);
			}
		}
		
		@SuppressWarnings("unchecked")
		JavaRDD<RecordLite>[] jrdds = FStream.of(rddList)
											.map(rdd -> rdd.getJavaRDD())
											.toArray(JavaRDD.class);
		JavaRDD<RecordLite> merged = rddList[0].getMarmotSpark().getJavaSparkContext().union(jrdds);
		return new MarmotRDD(rddList[0].getMarmotSpark(), first, merged);
	}
	
	public MarmotPairRDD mapToPair(String keyCols) {
		ColumnSelector selector = ColumnSelectorFactory.create(getRecordSchema(), keyCols);
		int[] keyColIdxes = FStream.from(selector.getColumnSelectionInfoAll())
									.mapToInt(info -> info.getColumn().ordinal())
									.toArray();
		RecordSchema keySchema = getRecordSchema().project(keyColIdxes);
		GRecordSchema keyGSchema = getGRecordSchema().derive(keySchema);
		JavaPairRDD<RecordLite,RecordLite> jpairs
							= m_jrdd.mapToPair(rec -> new Tuple2<>(rec.select(keyColIdxes), rec));
		return new MarmotPairRDD(m_marmot, keyGSchema, m_gschema, jpairs);
	}
	
	public MarmotPairRDD splitToPair(String keyCols) {
		ColumnSelector selector = ColumnSelectorFactory.create(getRecordSchema(), keyCols);
		int[] keyColIdxes = FStream.from(selector.getColumnSelectionInfoAll())
									.mapToInt(info -> info.getColumn().ordinal())
									.toArray();
		RecordSchema keySchema = getRecordSchema().project(keyColIdxes);
		GRecordSchema keyGSchema = getGRecordSchema().derive(keySchema);
		
		Set<Integer> idxes = Sets.newHashSet(Ints.asList(keyColIdxes));
		int[] valueColIdxes = FStream.from(getRecordSchema().getColumns())
									.zipWithIndex()
									.filter(t -> !idxes.contains(t.index()))
									.map(Indexed::value)
									.mapToInt(Column::ordinal)
									.toArray();
		RecordSchema valueSchema = getRecordSchema().project(valueColIdxes);
		GRecordSchema valueGSchema = getGRecordSchema().derive(valueSchema);
		
		JavaPairRDD<RecordLite,RecordLite> jpairs
							= m_jrdd.mapToPair(rec -> new Tuple2<>(rec.select(keyColIdxes),
																	rec.select(valueColIdxes)));
		return new MarmotPairRDD(m_marmot, keyGSchema, valueGSchema, jpairs);
	}
	
	public long count() {
		return m_jrdd.count();
	}
	
	public List<RecordLite> collect() {
		return m_jrdd.collect();
	}
	
	public Broadcast<Tuple2<GRecordSchema,List<RecordLite>>> broadcast() {
		return m_marmot.getJavaSparkContext().broadcast(new Tuple2<>(m_gschema, collect()));
	}
	
	public RecordLite takeFirst() {
		return m_jrdd.take(1).get(0);
	}
	
	public MarmotRDD cache() {
		return new MarmotRDD(m_marmot, m_gschema, m_jrdd.cache());
	}
	
	public MarmotRDD unpersist() {
		return new MarmotRDD(m_marmot, m_gschema, m_jrdd.unpersist());
	}
	
	public MarmotRDD derive(RecordSchema schema, JavaRDD<RecordLite> jrdd) {
		return new MarmotRDD(m_marmot, m_gschema.derive(schema), jrdd);
	}
	
	public MarmotRDD apply(RDDFunction func) {
		return func.apply(this);
	}
	public void apply(RDDConsumer consumer) {
		consumer.consume(this);
	}
	private MarmotRDD apply(RDDFilter filter) {
		return filter.apply(this);
	}
	
	public RecordSet toLocalRecordSet() {
		return new LocalRecordSet(m_gschema.getRecordSchema(), m_jrdd.toLocalIterator());
	}
	
	@Override
	public String toString() {
		String partStr = getPartitioner().map(part -> part.getQuadKeyAll().size())
										.map(cnt -> ", partitions=" + cnt)
										.getOrElse("");
		return String.format("%s%s", m_gschema, partStr);
	}
	
	public static RecordSet toLocalRecordSet(RecordSchema schema, JavaRDD<RecordLite> jrdd) {
		return new LocalRecordSet(schema, jrdd.toLocalIterator());
	}
}
