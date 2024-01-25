package marmot.spark.optor.reducer;

import marmot.Column;
import marmot.RecordSchema;
import marmot.optor.AggregateType;
import marmot.spark.RecordLite;
import marmot.support.DataUtils;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Sum implements ValueAggregator<Sum> {
	private static final long serialVersionUID = 1L;
	private static final String DEFAULT_OUT_COLUMN = "sum";
	
	private final String m_colName;
	private String m_outColName;
	
	// set when initialized
	private Column m_inputCol;
	private DataType m_outputType;
	private int m_outputColIdx = -1;
	
	public Sum(String colName, String outColName) {
		m_colName = colName;
		m_outColName = outColName;
	}
	
	public Sum(String colName) {
		this(colName, DEFAULT_OUT_COLUMN);
	}

	@Override
	public AggregateType getAggregateType() {
		return AggregateType.SUM;
	}
	
	@Override
	public String getInputColumnName() {
		return m_colName;
	}

	@Override
	public Column getOutputColumn() {
		return new Column(m_outColName, m_outputType);
	}

	@Override
	public Sum as(String outColName) {
		m_outColName = outColName;
		return this;
	}
	
	@Override
	public DataType getOutputType(DataType inputType) {
		return m_outputType;
	}

	@Override
	public void initialize(int pos, RecordSchema inputSchema) {
		m_inputCol = inputSchema.getColumn(m_colName);
		m_outputType = computeOutputType(m_inputCol.type());
		m_outputColIdx = pos;
	}

	@Override
	public Object getZeroValue() {
		return m_outputType.newInstance();
	}

	@Override
	public void collect(RecordLite accum, RecordLite input) {
		Object sum = accum.get(m_outputColIdx);
		Object value = input.get(m_inputCol.ordinal());
		
		switch ( m_outputType.getTypeCode() ) {
			case LONG:
				accum.set(m_outputColIdx, (Long)sum + DataUtils.asLong(value));
				break;
			case DOUBLE:
				accum.set(m_outputColIdx, (Double)sum + DataUtils.asDouble(value));
				break;
			default:
				throw new AssertionError("invalid SUM aggregate type: " + m_outputType);
		}
	}

	@Override
	public void combine(RecordLite accum1, RecordLite accum2) {
		Object v1 = accum1.get(m_outputColIdx);
		Object v2 = accum2.get(m_outputColIdx);
		
		switch ( m_outputType.getTypeCode() ) {
			case LONG:
				accum1.set(m_outputColIdx, (Long)v1 + (Long)v2);
				break;
			case DOUBLE:
				accum1.set(m_outputColIdx, (Double)v1 + (Double)v2);
				break;
			default:
				throw new AssertionError("invalid SUM aggregate type: " + m_outputType);
		}
	}

	@Override
	public void toFinalValue(RecordLite accum) { }
	
	@Override
	public String toString() {
		return String.format("%s(%s):%s", getClass().getSimpleName(), m_colName, m_outColName);
	}
	
	private DataType computeOutputType(DataType inputDataType) {
		switch ( inputDataType.getTypeCode() ) {
			case BYTE:
			case SHORT:
			case INT:
			case LONG:
				return DataType.LONG;
			case FLOAT:
			case DOUBLE:
				return DataType.DOUBLE;
			default:
				throw new IllegalArgumentException("unsupported 'sum' data type: " + inputDataType);
		}
	}
}
