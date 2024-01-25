package marmot.spark.optor.reducer;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import marmot.Column;
import marmot.RecordSchema;
import marmot.optor.AggregateType;
import marmot.spark.RecordLite;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CollectEnvelope implements ValueAggregator<CollectEnvelope> {
	private static final long serialVersionUID = 1L;
	private static final String DEFAULT_OUT_COLUMN = "mbr";
	
	private final String m_colName;
	private String m_outColName;
	
	// set when initialized
	private Column m_inputCol;
	private int m_outputColIdx = -1;
	
	public CollectEnvelope(String colName, String outColName) {
		m_colName = colName;
		m_outColName = outColName;
	}
	
	public CollectEnvelope(String colName) {
		this(colName, DEFAULT_OUT_COLUMN);
	}

	@Override
	public AggregateType getAggregateType() {
		return AggregateType.ENVELOPE;
	}
	
	@Override
	public String getInputColumnName() {
		return m_colName;
	}

	@Override
	public Column getOutputColumn() {
		return new Column(m_outColName, DataType.ENVELOPE);
	}

	@Override
	public CollectEnvelope as(String outColName) {
		m_outColName = outColName;
		return this;
	}
	
	@Override
	public DataType getOutputType(DataType inputType) {
		return DataType.ENVELOPE;
	}

	@Override
	public void initialize(int pos, RecordSchema inputSchema) {
		m_inputCol = inputSchema.getColumn(m_colName);
		m_outputColIdx = pos;
	}

	@Override
	public Object getZeroValue() {
		return new Envelope();
	}

	@Override
	public void collect(RecordLite accum, RecordLite input) {
		Envelope collected = (Envelope)accum.get(m_outputColIdx);
		
		Geometry geom = (Geometry)input.get(m_inputCol.ordinal());
		if ( geom != null ) {
			collected.expandToInclude(geom.getEnvelopeInternal());
			accum.set(m_outputColIdx, collected);
		}
	}

	@Override
	public void combine(RecordLite accum1, RecordLite accum2) {
		Envelope v1 = (Envelope)accum1.get(m_outputColIdx);
		Envelope v2 = (Envelope)accum2.get(m_outputColIdx);
		
		v1.expandToInclude(v2);
		accum1.set(m_outputColIdx, v1);
	}

	@Override
	public void toFinalValue(RecordLite accum) { }
}
