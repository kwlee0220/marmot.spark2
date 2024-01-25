package marmot.spark.optor.geo;

import org.locationtech.jts.geom.Geometry;

import marmot.Column;
import marmot.GRecordSchema;
import marmot.plan.PredicateOptions;
import marmot.spark.MarmotRDD;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.spark.optor.RecordLevelRDDFilter;
import marmot.type.GeometryDataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class SpatialRecordLevelRDDFilter extends RecordLevelRDDFilter {
	private static final long serialVersionUID = 1L;
	
	private int m_geomColIdx;
	private Column m_geomCol;

	abstract protected void initialize(MarmotSpark marmot, GeometryDataType inGeomType,
										GRecordSchema inputSchema);
	abstract protected boolean testGeometry(Geometry geom, RecordLite record);
	
	protected SpatialRecordLevelRDDFilter(PredicateOptions opts) {
		super(opts);
	}
	
	public final Column getInputGeometryColumn() {
		return m_geomCol;
	}
	
	@Override
	protected void _initialize(MarmotSpark marmot, GRecordSchema inputSchema) {
		m_geomCol = inputSchema.getGeometryColumn();
		
		m_geomColIdx = m_geomCol.ordinal();
		initialize(marmot, (GeometryDataType)m_geomCol.type(), inputSchema);
	}
	
	@Override
	protected boolean testRecord(RecordLite input) {
		Geometry geom = input.getGeometry(m_geomColIdx);
		return testGeometry(geom,input);
	}
	
	protected final Geometry loadKeyGeometry(MarmotSpark marmot, String keyDsId) {
		MarmotRDD mrdd = marmot.getDataSet(keyDsId).read();
		RecordLite first = mrdd.getJavaRDD().first();
		for ( Object value: first.values() ) {
			if ( value instanceof Geometry ) {
				return (Geometry)value;
			}
		}
		throw new IllegalArgumentException("query key geometry is missing, opt=" + this);
	}
}
