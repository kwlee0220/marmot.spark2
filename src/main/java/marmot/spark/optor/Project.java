package marmot.spark.optor;

import com.google.common.base.Preconditions;

import marmot.GRecordSchema;
import marmot.Record;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.optor.support.colexpr.ColumnSelectorFactory;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Project extends RecordLevelRDDFunction {
	private static final long serialVersionUID = 1L;

	private final String m_columnSelection;
	
	// initialized when this function is executed
	private transient ColumnSelector m_selector;

	/**
	 * 주어진 컬럼 이름들로 구성된 projection 스트림 연산자를 생성한다.
	 * 연산 수행 결과로 생성된 레코드 세트는 입력 레코드 세트에 포함된 각 레코드들에 대해
	 * 주어진 이름의 컬럼만으로 구성된 레코드들로 구성된다. 
	 * 
	 * @param	columnSelection	projection 연산에 사용될 컬럼들의 이름 배열.
	 */
	public Project(String columnSelection) {
		super(true);
		Preconditions.checkArgument(columnSelection != null, "Column seelection expression is null");
		
		m_columnSelection = columnSelection;
	}
	
	public String getColumnSelection() {
		return m_columnSelection;
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inGSchema) {
		try {
			m_selector = ColumnSelectorFactory.create(inGSchema.getRecordSchema(), m_columnSelection);
			return new GRecordSchema(inGSchema.getGeometryColumnInfo().getOrNull(),
									m_selector.getRecordSchema());
		}
		catch ( Exception e ) {
 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s", this, e));
		}
	}

	@Override
	protected void buildContext() {
		try {
			m_selector = ColumnSelectorFactory.create(getInputRecordSchema(), m_columnSelection);
		}
		catch ( Exception e ) {
 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s", this, e));
		}
	}

	@Override
	public RecordLite mapRecord(RecordLite input) {
		Record output = m_selector.select(input.toRecord(getInputRecordSchema()));
		return RecordLite.from(output);
	}
	
	@Override
	public String toString() {
		return String.format("%s: '%s'", getClass().getSimpleName(), m_columnSelection);
	}
}