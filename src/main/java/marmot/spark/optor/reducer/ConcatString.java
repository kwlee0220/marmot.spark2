package marmot.spark.optor.reducer;

import marmot.Column;
import marmot.RecordSchema;
import marmot.optor.AggregateType;
import marmot.spark.RecordLite;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ConcatString implements ValueAggregator<ConcatString> {
	private static final long serialVersionUID = 1L;
	private static final String DEFAULT_OUT_COLUMN = "concated";
	
	private final String m_colName;
	private String m_outColName;
	private String m_delim;
	
	// set when initialized
	private Column m_inputCol;
	private int m_outputColIdx = -1;
	
	public ConcatString(String colName, String outColName, String delim) {
		m_colName = colName;
		m_outColName = outColName;
		m_delim = delim;
	}
	
	public ConcatString(String colName, String delim) {
		this(colName, DEFAULT_OUT_COLUMN, delim);
	}

	@Override
	public AggregateType getAggregateType() {
		return AggregateType.CONCAT_STR;
	}
	
	@Override
	public String getInputColumnName() {
		return m_colName;
	}

	@Override
	public Column getOutputColumn() {
		return new Column(m_outColName, DataType.STRING);
	}

	@Override
	public ConcatString as(String outColName) {
		m_outColName = outColName;
		return this;
	}
	
	@Override
	public DataType getOutputType(DataType inputType) {
		return DataType.STRING;
	}

	@Override
	public void initialize(int pos, RecordSchema inputSchema) {
		m_inputCol = inputSchema.getColumn(m_colName);
		m_outputColIdx = pos;
	}

	@Override
	public Object getZeroValue() {
		return "";
	}

	@Override
	public void collect(RecordLite accum, RecordLite input) {
		String coll = accum.getString(m_outputColIdx);
		String str = input.get(m_inputCol.ordinal()).toString();
		
		if ( coll.length() > 0 ) {
			coll = coll + m_delim + str;
		}
		else {
			coll = str;
		}
		
		accum.set(m_outputColIdx, coll);
	}

	@Override
	public void combine(RecordLite accum1, RecordLite accum2) {
		String coll1 = accum1.getString(m_outputColIdx);
		String coll2 = accum2.getString(m_outputColIdx);
		
		if ( coll1.length() > 0 && coll2.length() > 0 ) {
			coll1 = coll1 + m_delim + coll2;
		}
		else if ( coll2.length() > 0 ) {
			coll1 
			= coll2;
		}
		accum1.set(m_outputColIdx, coll1);
	}

	@Override
	public void toFinalValue(RecordLite accum) { }
}
