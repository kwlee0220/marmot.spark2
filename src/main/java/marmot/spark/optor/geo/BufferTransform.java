package marmot.spark.optor.geo;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.buffer.BufferOp;

import com.google.common.base.Preconditions;

import marmot.Column;
import marmot.GRecordSchema;
import marmot.geo.GeoClientUtils;
import marmot.plan.GeomOpOptions;
import marmot.spark.RecordLite;
import marmot.support.DataUtils;
import marmot.type.GeometryDataType;
import utils.Utilities;
import utils.func.FOption;
import utils.func.Tuple;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BufferTransform extends SpatialRDDFunction {
	private static final long serialVersionUID = 1L;

	private GeometryDataType m_resultType;
	private double m_distance = -1;
	private String m_distanceCol = null;
	private FOption<Integer> m_segmentCount = FOption.empty();
	
	private int m_distColIdx = -1;
	
	public BufferTransform(double distance, GeomOpOptions opts) {
		super(opts, false);
		Preconditions.checkArgument(Double.compare(distance, 0d) > 0,
									"invalid buffer distance: dist=" + distance);

		m_distance = distance;
	}
	
	private BufferTransform(String distCol, GeomOpOptions opts) {
		super(opts, false);
		Utilities.checkNotNullArgument(distCol, "distance column is null");

		m_distanceCol = distCol;
	}

	@Override
	protected Tuple<GeometryDataType,String>
	initialize(GeometryDataType inGeomType, GRecordSchema inputSchema) {
		if ( m_distanceCol != null ) {
			Column distCol = inputSchema.findColumn(m_distanceCol).getOrNull();
			if ( distCol == null ) {
				String msg = String.format("invalid distance column: name=%s, schema=%s",
											m_distanceCol, inputSchema);
				throw new IllegalArgumentException(msg);
			}
			switch ( distCol.type().getTypeCode() ) {
				case DOUBLE: case FLOAT: case INT: case LONG: case BYTE: case SHORT:
					m_distColIdx = distCol.ordinal();
					break;
				default:
					String msg = String.format("invalid distance column: name=%s, type=%s",
							m_distanceCol, distCol.type());
					throw new IllegalArgumentException(msg);
			}
		}
		
		switch ( inGeomType.getTypeCode() ) {
			case POINT:
			case POLYGON:
			case LINESTRING:
				m_resultType = GeometryDataType.POLYGON;
				break;
			default:
				m_resultType = GeometryDataType.MULTI_POLYGON;
				break;
		}
		
		return Tuple.of(m_resultType, inputSchema.getSrid());
	}

	@Override
	protected Geometry transform(Geometry geom, RecordLite inputRecord) {
		double dist = m_distance;
		if ( dist < 0 ) {
			dist = DataUtils.asDouble(inputRecord.get(m_distColIdx));
		}

		Geometry buffered = m_segmentCount.isPresent()
							? BufferOp.bufferOp(geom, dist, m_segmentCount.get())
							: geom.buffer(dist);
		return GeoClientUtils.cast(buffered, m_resultType);
	}
}
