package marmot.spark.optor;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import marmot.GRecordSchema;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.JoinOptions;
import marmot.optor.JoinType;
import marmot.optor.support.JoinUtils;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.optor.support.colexpr.SelectedColumnInfo;
import marmot.spark.MarmotSpark;
import utils.CSV;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class HashJoin extends AbstractBinaryDataFrameFunction {
	private static final long serialVersionUID = 1L;
	
	private final String m_leftJoinCols;
	private final String m_rightJoinCols;
	private final String m_outputCols;
	private final JoinOptions m_jopts;
	
	// set while initialization
	private ColumnSelector m_selector;
	
	public HashJoin(String leftJoinCols, String rightJoinCols, String outputCols, JoinOptions jopts) {
		m_leftJoinCols = leftJoinCols;
		m_rightJoinCols = rightJoinCols;
		m_outputCols = outputCols;
		m_jopts = jopts;
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema leftGSchema,
										GRecordSchema rightGSchema) {
		m_selector = JoinUtils.createJoinColumnSelector(leftGSchema.getRecordSchema(),
														rightGSchema.getRecordSchema(), m_outputCols);
		GeometryColumnInfo gcInfo = leftGSchema.getGeometryColumnInfo()
										.flatMap(gc -> findGeometryColumnInfo(m_selector, "left", gc))
										.getOrNull();
		if ( gcInfo == null ) {
			gcInfo = rightGSchema.getGeometryColumnInfo()
								.flatMap(gc -> findGeometryColumnInfo(m_selector, "right", gc))
								.getOrNull();
		}
		return new GRecordSchema(gcInfo, m_selector.getRecordSchema());
	}

	@Override
	protected Dataset<Row> combine(Dataset<Row> left, Dataset<Row> right) {
		Dataset<Row> l = left.alias("left");
		Dataset<Row> r = right.alias("right");
		Column joinExpr = CSV.parseCsv(m_leftJoinCols).map(l::col)
							.zipWith(CSV.parseCsv(m_rightJoinCols).map(r::col))
							.map(t -> t._1.equalTo(t._2))
							.reduce((e1,e2) -> e1.and(e2));
		
		Column[] outCols = FStream.from(m_selector.getColumnSelectionInfoAll())
									.map(c -> toColumnExpr(c, l, r))
									.toArray(Column.class);
		return l.join(r, joinExpr, toJoinString(m_jopts.joinType()))
				.select(outCols);
	}
	
	public static String toJoinString(JoinType type) {
		switch ( type ) {
			case INNER_JOIN: return "inner";
			case LEFT_OUTER_JOIN: return "left_outer";
			case RIGHT_OUTER_JOIN: return "right_outer";
			case FULL_OUTER_JOIN: return "full_outer";
			case SEMI_JOIN: return "semi";
			default:
				throw new IllegalArgumentException("unsupported join type: " + type);
		}
	}
	
	private FOption<GeometryColumnInfo> findGeometryColumnInfo(ColumnSelector selector,
															String ns, GeometryColumnInfo gcInfo) {
		return selector.findColumn(ns, gcInfo.name())
						.map(col -> new GeometryColumnInfo(col.name(), gcInfo.srid()));
	}
	
	private Column toColumnExpr(SelectedColumnInfo scInfo, Dataset<Row> left, Dataset<Row> right) {
		Dataset<Row> dset = ( scInfo.getNamespace().equals("left") ) ? left : right;
		return dset.col(scInfo.getColumn().name()).alias(scInfo.getAlias());
	}
}
