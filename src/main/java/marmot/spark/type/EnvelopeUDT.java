package marmot.spark.type;

import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.UserDefinedType;
import org.locationtech.jts.geom.Envelope;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class EnvelopeUDT extends UserDefinedType<Envelope> {
	private static final long serialVersionUID = 1L;

	public static final EnvelopeUDT UDT;
	public static final ArrayType SQL_DATA_TYPE;
	static {
		SQL_DATA_TYPE = DataTypes.createArrayType(DataTypes.DoubleType);
		UDT = new EnvelopeUDT();
	}

	@Override
	public DataType sqlType() {
		return SQL_DATA_TYPE;
	}

	@Override
	public Class<Envelope> userClass() {
		return Envelope.class;
	}

	@Override
	public Envelope deserialize(Object datum) {
		double[] values = ((UnsafeArrayData)datum).toDoubleArray();
		if ( Double.isInfinite(values[0]) ) {
			return new Envelope();
		}
		else {
			return new Envelope(values[0], values[1], values[2], values[3]);
		}
	}
	
	@Override
	public Object serialize(Envelope envl) {
		double[] values;
		if ( envl.isNull() ) {
			values = new double[] { Double.NEGATIVE_INFINITY };
		}
		else {
			values = new double[] { envl.getMinX(), envl.getMaxX(), envl.getMinY(), envl.getMaxY() };
		}
		
		return UnsafeArrayData.fromPrimitiveArray(values);
	}
}
