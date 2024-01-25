package marmot.spark.type;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.UserDefinedType;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.io.ParseException;

import marmot.geo.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiPointUDT extends UserDefinedType<MultiPoint> {
	private static final long serialVersionUID = -1L;

	public static final MultiPointUDT UDT;
	private static final DataType SQL_DATA_TYPE;
	
	static {
		SQL_DATA_TYPE = DataTypes.createStructType(new StructField[] {
			DataTypes.createStructField("wkb", DataTypes.BinaryType, false),
		});
		UDT = new MultiPointUDT();
	}

	@Override
	public DataType sqlType() {
		return SQL_DATA_TYPE;
	}

	@Override
	public InternalRow serialize(MultiPoint geom) {
		InternalRow row = new GenericInternalRow(1);
		row.update(0, GeoClientUtils.toWKB(geom));
		
		return row;
	}

	@Override
	public MultiPoint deserialize(Object datum) {
		if ( datum instanceof InternalRow ) {
			InternalRow row = (InternalRow)datum;
			try {
				byte[] wkb = row.getBinary(0);
				return (MultiPoint)GeoClientUtils.fromWKB(wkb);
			}
			catch ( ParseException e ) {
				throw new IllegalStateException("invalid WKB, cause=" + e);
			}
		}

		throw new IllegalArgumentException("datum is not Row: " + datum);
	}

	@Override
	public Class<MultiPoint> userClass() {
		return MultiPoint.class;
	}

}
