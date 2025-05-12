package marmot.spark.optor.reducer;

import java.util.List;

import marmot.Column;
import marmot.RecordSchema;
import marmot.optor.AggregateType;
import marmot.spark.RecordLite;
import marmot.support.DataUtils;
import marmot.type.DataType;
import utils.CSV;
import utils.Tuple;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Avg implements ValueAggregator<Avg> {
	private static final long serialVersionUID = 1L;
	private static final String DEFAULT_OUT_COLUMN = "avg";
	
	private final String m_colName;
	private String m_outColName;
	
	// set when initialized
	private Column m_inputCol;
	private DataType m_sumType;
	private int m_outputColIdx = -1;
	
	public Avg(String colName, String outColName) {
		m_colName = colName;
		m_outColName = outColName;
	}
	
	public Avg(String colName) {
		this(colName, DEFAULT_OUT_COLUMN);
	}

	@Override
	public AggregateType getAggregateType() {
		return AggregateType.AVG;
	}
	
	@Override
	public String getInputColumnName() {
		return m_colName;
	}

	@Override
	public Column getOutputColumn() {
		return new Column(m_outColName, DataType.DOUBLE);
	}

	@Override
	public Avg as(String outColName) {
		m_outColName = outColName;
		return this;
	}
	
	@Override
	public DataType getOutputType(DataType inputType) {
		return DataType.DOUBLE;
	}

	@Override
	public void initialize(int pos, RecordSchema inputSchema) {
		m_inputCol = inputSchema.getColumn(m_colName);
		m_sumType = computeSumType(m_inputCol.type());
		m_outputColIdx = pos;
	}

	@Override
	public Object getZeroValue() {
		return "0:0";
	}

	@Override
	public void collect(RecordLite accum, RecordLite input) {
		Tuple<Object,Long> sum = decode(accum.get(m_outputColIdx));
		Object value = input.get(m_inputCol.ordinal());
		accum.set(m_outputColIdx, encode(add(sum, value)));
	}

	@Override
	public void combine(RecordLite accum1, RecordLite accum2) {
		Tuple<Object,Long> v1 = decode(accum1.get(m_outputColIdx));
		Tuple<Object,Long> v2 = decode(accum2.get(m_outputColIdx));
		accum1.set(m_outputColIdx, encode(total(v1, v2)));
	}

	@Override
	public void toFinalValue(RecordLite accum) {
		Tuple<Object,Long> v = decode(accum.get(m_outputColIdx));
		
		switch ( m_sumType.getTypeCode() ) {
			case LONG:
				accum.set(m_outputColIdx, (Long)v._1 / v._2.doubleValue());
				break;
			case DOUBLE:
				accum.set(m_outputColIdx, (Double)v._1 / v._2.doubleValue());
				break;
			default:
				throw new AssertionError("invalid AVG aggregate type: " + m_sumType);
		}
	}
	
	private Tuple<Object,Long> decode(Object encoded) {
		List<String> parts = CSV.parseCsv((String)encoded, ':').toList();
		long count = Long.parseLong(parts.get(1));
		
		switch ( m_sumType.getTypeCode() ) {
			case LONG:
				return Tuple.of(Long.parseLong(parts.get(0)), count);
			case DOUBLE:
				return Tuple.of(Double.parseDouble(parts.get(0)), count);
			default:
				throw new AssertionError("invalid AVG aggregate type: " + m_sumType);
		}
	}
	
	private String encode(Tuple<Object,Long> sum) {
		return sum._1.toString() + ":" + sum._2;
	}
	
	private Tuple<Object,Long> add(Tuple<Object,Long> sum, Object value) {
		Object added;
		switch ( m_sumType.getTypeCode() ) {
			case LONG:
				added = DataUtils.asLong(value) + (Long)sum._1;
				break;
			case DOUBLE:
				added = DataUtils.asDouble(value) + (Double)sum._1;
				break;
			default:
				throw new AssertionError("invalid AVG aggregate type: " + m_sumType);
		}
		
		return Tuple.of(added, sum._2 + 1);
	}
	
	private Tuple<Object,Long> total(Tuple<Object,Long> v1, Tuple<Object,Long> v2) {
		Object total;
		switch ( m_sumType.getTypeCode() ) {
			case LONG:
				total = DataUtils.asLong(v1._1) + DataUtils.asLong(v2._1);
				break;
			case DOUBLE:
				total = DataUtils.asDouble(v1._1) + DataUtils.asDouble(v2._1);
				break;
			default:
				throw new AssertionError("invalid AVG aggregate type: " + m_sumType);
		}
		
		return Tuple.of(total, v1._2 + v2._2);
	}
	
	private DataType computeSumType(DataType inputDataType) {
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
