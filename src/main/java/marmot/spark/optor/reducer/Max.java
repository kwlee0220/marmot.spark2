package marmot.spark.optor.reducer;

import java.util.Comparator;

import marmot.Column;
import marmot.RecordSchema;
import marmot.optor.AggregateType;
import marmot.spark.RecordLite;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Max implements ValueAggregator<Max> {
	private static final long serialVersionUID = 1L;
	private static final String DEFAULT_OUT_COLUMN = "max";
	
	private final String m_colName;
	private String m_outColName;
	
	// set when initialized
	private Column m_inputCol;
	private Comparator<Object> m_cmptor;
	private DataType m_outputType;
	private int m_outputColIdx = -1;
	
	public Max(String colName, String outColName) {
		m_colName = colName;
		m_outColName = outColName;
	}
	
	public Max(String colName) {
		this(colName, DEFAULT_OUT_COLUMN);
	}

	@Override
	public AggregateType getAggregateType() {
		return AggregateType.MAX;
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
	public Max as(String outColName) {
		m_outColName = outColName;
		return this;
	}
	
	@Override
	public DataType getOutputType(DataType inputType) {
		return m_outputType;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void initialize(int pos, RecordSchema inputSchema) {
		m_inputCol = inputSchema.getColumn(m_colName);
		m_cmptor = (Comparator<Object>)m_inputCol.type();
		m_outputType = computeOutputType(m_inputCol.type());
		m_outputColIdx = pos;
	}

	@Override
	public Object getZeroValue() {
		return null;
	}

	@Override
	public void collect(RecordLite accum, RecordLite input) {
		Object max = accum.get(m_outputColIdx);
		Object data = input.get(m_inputCol.ordinal());
		if ( max == null || m_cmptor.compare(max, data) < 0 ) {
			accum.set(m_outputColIdx, data);
		}
	}

	@Override
	public void combine(RecordLite accum1, RecordLite accum2) {
		Object v1 = accum1.get(m_outputColIdx);
		Object v2 = accum2.get(m_outputColIdx);
		if ( m_cmptor.compare(v1, v2) < 0 ) {
			accum1.set(m_outputColIdx, v2);
		}
	}

	@Override
	public void toFinalValue(RecordLite accum) { }

	private DataType computeOutputType(DataType inputDataType) {
		if ( inputDataType instanceof Comparator<?> ) {
			return inputDataType;
		}
		else {
			throw new IllegalArgumentException("unsupported 'max' data type: " + inputDataType);
		}
	}
}
