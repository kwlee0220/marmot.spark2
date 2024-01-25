package marmot.spark.optor.geo;

import org.locationtech.jts.geom.Point;

import marmot.Column;
import marmot.GRecordSchema;
import marmot.RecordSchema;
import marmot.dataset.GeometryColumnInfo;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.spark.optor.RecordLevelRDDFunction;
import marmot.support.DataUtils;
import marmot.support.GeoUtils;
import marmot.type.DataType;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ToGeometryPoint extends RecordLevelRDDFunction {
	private static final long serialVersionUID = 1L;
	
	private final String m_xColName;
	private final String m_yColName;
	private final GeometryColumnInfo m_gcInfo;
	
	private Column m_xCol;
	private Column m_yCol;
	private Column m_outCol;

	public ToGeometryPoint(String xColName, String yColName, GeometryColumnInfo gcInfo) {
		super(true);
		
		Utilities.checkNotNullArgument(xColName, "x-column is null");
		Utilities.checkNotNullArgument(yColName, "y-column is null");
		Utilities.checkNotNullArgument(gcInfo, "output GeometryColumnInfo is null");
		
		m_xColName = xColName;
		m_yColName = yColName;
		m_gcInfo = gcInfo;
	}
	
	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inGSchema) {
		RecordSchema inputSchema = inGSchema.getRecordSchema();
		m_xCol = inputSchema.findColumn(m_xColName).getOrNull();
		if ( m_xCol == null ) {
			throw new IllegalArgumentException("unknown x-column: " + m_xColName
												+ ", schema=" + inputSchema);
		}
		switch ( m_xCol.type().getTypeCode() ) {
			case DOUBLE: case FLOAT: case INT: case SHORT: case STRING:
				break;
			default:
				throw new IllegalArgumentException("invalid x-column type: name="
													+ m_xColName + ", type=" + m_xCol.type());
			
		}
		
		m_yCol = inputSchema.findColumn(m_yColName).getOrNull();
		if ( m_yCol == null ) {
			throw new IllegalArgumentException("unknown y-column: " + m_yColName + ", schema="
												+ inputSchema);
		}
		switch ( m_yCol.type().getTypeCode() ) {
			case DOUBLE: case FLOAT: case INT: case SHORT: case STRING:
				break;
			default:
				throw new IllegalArgumentException("invalid y-column type: name="
													+ m_yColName + ", type=" + m_yCol.type());
			
		}
		
		RecordSchema outputSchema = inputSchema.toBuilder()
												.addColumn(m_gcInfo.name(), DataType.POINT)
												.build();
		m_outCol = outputSchema.getColumn(m_gcInfo.name());
		return new GRecordSchema(m_gcInfo, outputSchema);
	}

	@Override
	protected void buildContext() { }

	@Override
	protected RecordLite mapRecord(RecordLite input) {
		RecordLite output = RecordLite.of(m_outputGSchema.getRecordSchema());
		input.copyTo(output);
		
		Object xObj = getValue(input, m_xCol);
		Object yObj = getValue(input, m_yCol);
		if ( xObj != null && yObj != null ) {
			try {
				double xpos = DataUtils.asDouble(xObj);
				double ypos = DataUtils.asDouble(yObj);
				Point pt = GeoUtils.toPoint(xpos, ypos);
				output.set(m_outCol.ordinal(), pt);
			}
			catch ( Exception e ) {
				output.set(m_outCol.ordinal(), null);
				throw e;
			}
		}
		else {
			output.set(m_outCol.ordinal(), null);
		}
		
		return output;
	}
	
	@Override
	public String toString() {
		return String.format("%s: (%s,%s)->%s", getClass().getSimpleName(),
								m_xColName, m_yColName, m_gcInfo);
	}
	
	private Object getValue(RecordLite input, Column col) {
		Object obj = input.get(col.ordinal());
		if ( obj instanceof String && ((String)obj).length() == 0 ) {
			return null;
		}
		else {
			return obj;
		}
	}
}
