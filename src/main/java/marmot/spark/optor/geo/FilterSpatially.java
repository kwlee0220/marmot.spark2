package marmot.spark.optor.geo;

import java.util.Objects;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;

import marmot.GRecordSchema;
import marmot.geo.GeoClientUtils;
import marmot.optor.geo.SpatialRelation;
import marmot.plan.PredicateOptions;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.type.GeometryDataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FilterSpatially extends SpatialRecordLevelRDDFilter {
	private final @Nullable Envelope m_keyBounds;
	private final @Nullable String m_keyDsId;
	private Geometry m_key;
	private PreparedGeometry m_pkey;
	private final SpatialRelation m_rel;
	
	public FilterSpatially(SpatialRelation rel, Envelope key, PredicateOptions opts) {
		super(opts);
		Objects.requireNonNull(rel, "SpatialRelation");
		Objects.requireNonNull(key, "Key bounds");

		m_rel = rel;
		m_keyBounds = key;
		m_keyDsId = null;
		m_key = null;
	}
	
	public FilterSpatially(SpatialRelation rel, String keyDsId, PredicateOptions opts) {
		super(opts);
		Objects.requireNonNull(rel, "SpatialRelation");
		Objects.requireNonNull(keyDsId, "Key dataset id");

		m_rel = rel;
		m_keyBounds = null;
		m_keyDsId = keyDsId;
		m_key = null;
	}
	
	public FilterSpatially(SpatialRelation rel, Geometry key, PredicateOptions opts) {
		super(opts);
		Objects.requireNonNull(rel, "SpatialRelation");
		Objects.requireNonNull(key, "Key geometry");

		m_rel = rel;
		m_keyBounds = null;
		m_keyDsId = null;
		m_key = key;
	}

	@Override
	protected void initialize(MarmotSpark marmot, GeometryDataType inGeomType,
								GRecordSchema inputSchema) {
		if ( m_keyBounds != null ) {
			m_key = GeoClientUtils.toPolygon(m_keyBounds);
		}
		else if ( m_keyDsId != null ) {
			m_key = loadKeyGeometry(marmot, m_keyDsId);
		}
		
		m_pkey = PreparedGeometryFactory.prepare(m_key);
	}

	@Override
	protected void buildContext() {
	}
	
	@Override
	protected boolean testGeometry(Geometry geom, RecordLite record) {
		switch ( m_rel.getCode() ) {
			case CODE_INTERSECTS:
				return m_pkey.intersects(geom);
			default:
				return m_rel.test(geom, m_key);
			
		}
	}
	
	@Override
	public String toString() {
		return String.format("filter_spatially[%s]", getInputGeometryColumn());
	}
}
