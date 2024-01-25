package marmot.spark.type;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ConcatStrUDAF extends UserDefinedAggregateFunction {
	private static final long serialVersionUID = 1L;

	@Override
	public StructType inputSchema() {
		return DataTypes.createStructType(new StructField[] {
			DataTypes.createStructField("str", DataTypes.StringType, true),
			DataTypes.createStructField("delim", DataTypes.StringType, true)
		});
	}

	@Override
	public StructType bufferSchema() {
		return DataTypes.createStructType(new StructField[] {
			DataTypes.createStructField("accum", DataTypes.StringType, true)
		});
	}

	@Override
	public DataType dataType() {
		return DataTypes.StringType;
	}

	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		setBuffer(buffer, "");
	}

	@Override
	public boolean deterministic() {
		return true;
	}

	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		String accum = fromBuffer(buffer);
		String str = input.getString(0);
		String delim = input.getString(1);
		setBuffer(buffer, accum + delim + str);
	}

	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		String accum1 = fromBuffer(buffer1);
		String accum2 = fromBuffer(buffer2);
		
		setBuffer(buffer1, accum1 + accum2);
	}

	@Override
	public Object evaluate(Row buffer) {
		return fromBuffer(buffer).substring(1);
	}

	private String fromBuffer(MutableAggregationBuffer buffer) {
		return buffer.isNullAt(0) ? "" : buffer.getString(0);
	}

	private String fromBuffer(Row buffer) {
		return buffer.isNullAt(0) ? "" : buffer.getString(0);
	}
	
	private void setBuffer(MutableAggregationBuffer buffer, String str) {
		if ( str == null ) {
			str = "";
		}
		
		buffer.update(0, str);
	}
}
