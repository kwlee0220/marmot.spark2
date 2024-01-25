package marmot.spark.type;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class EnvelopeUDAF extends UserDefinedAggregateFunction {
	private static final long serialVersionUID = 1L;

	@Override
	public StructType inputSchema() {
		return DataTypes.createStructType(new StructField[] {
			DataTypes.createStructField("geom", GeometryUDT.UDT, false)
		});
	}

	@Override
	public StructType bufferSchema() {
		return DataTypes.createStructType(new StructField[] {
			DataTypes.createStructField("x1", DataTypes.DoubleType, false),
			DataTypes.createStructField("x2", DataTypes.DoubleType, false),
			DataTypes.createStructField("y1", DataTypes.DoubleType, false),
			DataTypes.createStructField("y2", DataTypes.DoubleType, false),
		});
	}

	@Override
	public DataType dataType() {
		return EnvelopeUDT.UDT;
	}

	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		toBuffer(buffer, new Envelope());
	}

	@Override
	public boolean deterministic() {
		return true;
	}

	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		Geometry geom = (Geometry)input.get(0);
		if ( geom != null ) {
			Envelope envl = geom.getEnvelopeInternal();
			Envelope accum = fromBuffer(buffer);
			accum.expandToInclude(envl);
			toBuffer(buffer, accum);
		}
	}

	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		Envelope accum = fromBuffer(buffer1);
		Envelope envl = fromBuffer(buffer2);
		accum.expandToInclude(envl);
		toBuffer(buffer1, accum);
	}

	@Override
	public Object evaluate(Row buffer) {
		Envelope envl = fromBuffer(buffer);
		return envl;
	}

	private Envelope fromBuffer(MutableAggregationBuffer buffer) {
		if ( Double.isInfinite(buffer.getDouble(0)) ) {
			return new Envelope();
		}
		else {
			return new Envelope(buffer.getDouble(0), buffer.getDouble(1),
								buffer.getDouble(2), buffer.getDouble(3));
		}
	}

	private Envelope fromBuffer(Row buffer) {
		if ( Double.isInfinite(buffer.getDouble(0)) ) {
			return new Envelope();
		}
		else {
			return new Envelope(buffer.getDouble(0), buffer.getDouble(1),
								buffer.getDouble(2), buffer.getDouble(3));
		}
	}
	
	private void toBuffer(MutableAggregationBuffer buffer, Envelope envl) {
		if ( envl.isNull() ) {
			buffer.update(0, Double.NEGATIVE_INFINITY);
		}
		else {
			buffer.update(0, envl.getMinX());
			buffer.update(1, envl.getMaxX());
			buffer.update(2, envl.getMinY());
			buffer.update(3, envl.getMaxY());
		}
	}
}
