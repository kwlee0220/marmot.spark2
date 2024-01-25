package marmot.spark.optor.geo;

import java.util.Iterator;

import org.locationtech.jts.geom.Geometry;

import marmot.GRecordSchema;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.CoordinateTransform;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.spark.optor.AbstractRDDFunction;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TransformCrs extends AbstractRDDFunction {
	private static final long serialVersionUID = 1L;
	
	private final String m_toSrid;
	
	public TransformCrs(String toSrid) {
		super(true);
		Utilities.checkNotNullArgument(toSrid, "toSrid is null");
		
		m_toSrid = toSrid;
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inGSchema) {
		GeometryColumnInfo toGcInfo = new GeometryColumnInfo(inGSchema.getGeometryColumnName(), m_toSrid);
		return new GRecordSchema(toGcInfo, inGSchema.getRecordSchema());
	}
	
	@Override
	public String toString() {
		return String.format("%s: %s->%s", getClass().getSimpleName(),
								m_inputGSchema.assertGeometryColumnInfo(),
								m_outputGSchema.assertGeometryColumnInfo());
	}

	@Override
	protected Iterator<RecordLite> mapPartition(Iterator<RecordLite> iter) {
		int geomColIdx = m_inputGSchema.getGeometryColumnIdx();
		CoordinateTransform trans = CoordinateTransform.get(m_inputGSchema.getSrid(), m_toSrid);
		
		return FStream.from(iter)
						.map(rec -> {
							Object[] values = rec.values();
							Geometry geom = rec.getGeometry(geomColIdx);
							if ( geom != null && !geom.isEmpty() ) {
								geom = trans.transform(geom);
								values[geomColIdx] = geom;
							}
							
							return RecordLite.of(values);
						})
						.iterator();
	}
}
