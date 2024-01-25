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
public class Count implements ValueAggregator<Count> {
	private static final long serialVersionUID = 1L;
	private static final String DEFAULT_OUT_COLUMN = "count";
	
	private final String m_colName;
	private String m_outColName;
	
	// set when initialized
	private int m_outputColIdx = -1;
	
	public Count(String outColName) {
		m_colName = "";
		m_outColName = outColName;
	}
	
	public Count() {
		this(DEFAULT_OUT_COLUMN);
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
		return new Column(m_outColName, DataType.LONG);
	}

	@Override
	public Count as(String outColName) {
		m_outColName = outColName;
		return this;
	}
	
	@Override
	public DataType getOutputType(DataType inputType) {
		return DataType.LONG;
	}

	@Override
	public void initialize(int pos, RecordSchema inputSchema) {
		m_outputColIdx = pos;
	}

	@Override
	public Object getZeroValue() {
		return 0L;
	}

	@Override
	public void collect(RecordLite accum, RecordLite input) {
		long count = (Long)accum.get(m_outputColIdx);
		accum.set(m_outputColIdx, count+1);
	}

	@Override
	public void combine(RecordLite accum1, RecordLite accum2) {
		long v1 = (Long)accum1.get(m_outputColIdx);
		long v2 = (Long)accum2.get(m_outputColIdx);
		accum1.set(m_outputColIdx, v1 + v2);
	}

	@Override
	public void toFinalValue(RecordLite accum) {
	}
}
