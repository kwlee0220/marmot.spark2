package marmot.spark.optor.geo;

import org.locationtech.jts.geom.Geometry;

import marmot.optor.support.SafeIntersection;
import marmot.type.GeometryDataType;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BinarySpatialIntersection extends BinarySpatialTransform {
	private static final long serialVersionUID = 1L;

	private static final int DEFAULT_REDUCE_FACTOR = 2;
	
	private SafeIntersection m_op;
	
	public BinarySpatialIntersection(String leftGeomCol, String rightGeomCol,
									String outGeomCol, FOption<GeometryDataType> outGeomType) {
		super(leftGeomCol, rightGeomCol, outGeomCol, outGeomType, true);
	}

	@Override
	protected void buildContext() {
		m_op = new SafeIntersection(getOutputGeometryType().toGeometries())
					.setReduceFactor(DEFAULT_REDUCE_FACTOR);
	}
	
	@Override
	protected Geometry mapGeometry(Geometry left, Geometry right) {
		return m_op.apply(left, right);
	}
	
	@Override
	public String toString() {
		return String.format("%s: (%s, %s)->%s, type=%s", getClass().getSimpleName(),
							m_leftGeomColName, m_rightGeomColName, m_outputGeomCol,
							getOutputGeometryType());
	}
}
