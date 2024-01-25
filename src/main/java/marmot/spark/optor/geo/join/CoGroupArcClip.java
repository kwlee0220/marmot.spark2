package marmot.spark.optor.geo.join;

import java.util.List;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Geometry;

import marmot.Column;
import marmot.GRecordSchema;
import marmot.Record;
import marmot.optor.support.SafeIntersection;
import marmot.optor.support.SafeUnion;
import marmot.plan.SpatialJoinOptions;
import marmot.spark.MarmotSpark;
import marmot.type.GeometryDataType;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CoGroupArcClip extends CoGroupNLSpatialJoin {
	private static final long serialVersionUID = 1L;
	
	// when initialized;
	private transient SafeIntersection m_intersection;
	private transient SafeUnion m_union;

	protected CoGroupArcClip(SpatialJoinOptions opts) {
		super(true, opts);
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema leftGSchema,
										GRecordSchema rightGSchema) {
		return leftGSchema;
	}

	@Override
	protected void buildContext() {
		Column geomCol = m_leftGSchema.getGeometryColumn();
		Geometries dstType = ((GeometryDataType)geomCol.type()).toGeometries();

		m_intersection = new SafeIntersection().setReduceFactor(0);
		m_intersection.setResultType(dstType);
		m_union = new SafeUnion(dstType);
	}

	@Override
	protected FStream<Record> combine(Record left, FStream<Record> rightStrm) {
		Record result = left.duplicate();
		
		Geometry outerGeom = getLeftGeometry(left); 	
		List<Geometry> clips = rightStrm.map(right -> getRightGeometry(right))
										.map(right -> m_intersection.apply(outerGeom, right))
										.toList();
		if ( clips.size() > 0 ) {
			result.set(m_leftGeomColIdx, m_union.apply(clips));
			return FStream.of(result);
		}
		else {
			return FStream.empty();
		}
	}
}
