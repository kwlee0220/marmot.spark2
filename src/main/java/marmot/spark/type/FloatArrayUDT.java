package marmot.spark.type;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.UserDefinedType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FloatArrayUDT extends UserDefinedType<Float[]> {
	private static final long serialVersionUID = -1L;

	public static final FloatArrayUDT UDT;
	private static final DataType SQL_DATA_TYPE;
	
	static {
		SQL_DATA_TYPE = DataTypes.createArrayType(DataTypes.FloatType);
		UDT = new FloatArrayUDT();
	}

	@Override
	public DataType sqlType() {
		return SQL_DATA_TYPE;
	}

	@Override
	public Float[] deserialize(Object datum) {
		float[] values = ((UnsafeArrayData)datum).toFloatArray();
		return ArrayUtils.toObject(values);
	}

	@Override
	public Object serialize(Float[] array) {
		float[] values = ArrayUtils.toPrimitive(array);
		return new GenericArrayData(values);
	}

	@Override
	public Class<Float[]> userClass() {
		return Float[].class;
	}
}
