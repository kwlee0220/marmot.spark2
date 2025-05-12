package marmot.spark.optor.geo;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.Tuple;

import marmot.GRecordSchema;
import marmot.plan.GeomOpOptions;
import marmot.spark.RecordLite;
import marmot.type.GeometryDataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CentroidTransform extends SpatialRDDFunction {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(CentroidTransform.class);
	
	private final boolean m_inside;
	
	public CentroidTransform(boolean inside, GeomOpOptions opts) {
		super(opts, false);

		m_inside = inside;
		setLogger(s_logger);
	}

	@Override
	protected Tuple<GeometryDataType,String>
	initialize(GeometryDataType inGeomType, GRecordSchema inGSchema) {
		return Tuple.of(GeometryDataType.POINT, inGSchema.getSrid());
	}
	
	@Override
	protected Point transform(Geometry geom, RecordLite inputRecord) {
		if ( m_inside ) {
			return geom.getInteriorPoint();
		}
		else {
			return geom.getCentroid();
		}
	}
	
	@Override
	public String toString() {
		return String.format("centroid[%s]", getInputGeometryColumnName());
	}
}
