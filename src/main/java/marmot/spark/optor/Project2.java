package marmot.spark.optor;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import com.google.common.base.Preconditions;

import marmot.GRecordSchema;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.optor.support.colexpr.ColumnSelectorFactory;
import marmot.optor.support.colexpr.SelectedColumnInfo;
import marmot.spark.MarmotSpark;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Project2 extends AbstractDataFrameFunction {
	private static final long serialVersionUID = 1L;

	private final String m_columnSelection;
	
	private Column[] m_sqlCols;

	/**
	 * 주어진 컬럼 이름들로 구성된 projection 스트림 연산자를 생성한다.
	 * 연산 수행 결과로 생성된 레코드 세트는 입력 레코드 세트에 포함된 각 레코드들에 대해
	 * 주어진 이름의 컬럼만으로 구성된 레코드들로 구성된다. 
	 * 
	 * @param	columnSelection	projection 연산에 사용될 컬럼들의 이름 배열.
	 */
	public Project2(String columnSelection) {
		Preconditions.checkArgument(columnSelection != null, "Column seelection expression is null");
		
		m_columnSelection = columnSelection;
	}
	
	public String getColumnSelection() {
		return m_columnSelection;
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inGSchema) {
		try {
			ColumnSelector selector = ColumnSelectorFactory.create(inGSchema.getRecordSchema(),
																	m_columnSelection);
			m_sqlCols = FStream.from(selector.getColumnSelectionInfoAll())
								.map(this::toSqlColumn)
								.toArray(Column.class);
			return new GRecordSchema(inGSchema.getGeometryColumnInfo().getOrNull(),
									selector.getRecordSchema());
		}
		catch ( Exception e ) {
 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s", this, e));
		}
	}

	@Override
	protected Dataset<Row> mapDataFrame(Dataset<Row> df) {
		return df.select(m_sqlCols);
	}
	
	@Override
	public String toString() {
		return String.format("%s: '%s'", getClass().getSimpleName(), m_columnSelection);
	}
	
	private Column toSqlColumn(SelectedColumnInfo scInfo) {
		return functions.col(scInfo.getColumn().name()).alias(scInfo.getAlias());
	}
}