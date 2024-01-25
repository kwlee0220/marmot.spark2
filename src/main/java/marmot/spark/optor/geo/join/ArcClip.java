package marmot.spark.optor.geo.join;

import java.util.List;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Geometry;

import marmot.Column;
import marmot.GRecordSchema;
import marmot.Record;
import marmot.RecordSet;
import marmot.optor.geo.join.NestedLoopMatch;
import marmot.optor.support.SafeIntersection;
import marmot.optor.support.SafeUnion;
import marmot.plan.SpatialJoinOptions;
import marmot.spark.MarmotSpark;
import marmot.spark.dataset.SparkDataSet;
import marmot.type.GeometryDataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ArcClip extends NestedLoopSpatialJoin {
	private static final long serialVersionUID = 1L;
	
	private transient SafeIntersection m_intersection;
	private transient SafeUnion m_union;
	private transient RecordSet m_emptyResult;
	
	public ArcClip(SparkDataSet innerDs) {
		super(innerDs, false, SpatialJoinOptions.DEFAULT, true);
	}

	@Override
	protected GRecordSchema initialize(MarmotSpark marmot, GRecordSchema outerGSchema,
										GRecordSchema innerGSchema) {
		return outerGSchema;
	}

	@Override
	protected void buildContext() {
		Column geomCol = m_inputGSchema.getGeometryColumn();
		Geometries dstType = ((GeometryDataType)geomCol.type()).toGeometries();

		m_intersection = new SafeIntersection().setReduceFactor(0);
		m_intersection.setResultType(dstType);
		m_union = new SafeUnion(dstType);
		m_emptyResult = RecordSet.empty(getGRecordSchema().getRecordSchema());
	}

	@Override
	protected RecordSet doInnerLoop(NestedLoopMatch match) {
		Record result = match.getOuterRecord().duplicate();
		
		Geometry outerGeom = getOuterGeometry(match.getOuterRecord()); 	
		List<Geometry> clips = match.getInnerRecords()
									.map(inner -> getInnerGeometry(inner))
									.map(inner -> m_intersection.apply(outerGeom, inner))
									.toList();
		if ( clips.size() > 0 ) {
			result.set(m_outerGeomColIdx, m_union.apply(clips));
			return RecordSet.of(result);
		}
		else {
			return m_emptyResult;
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s: %s<->%s", getClass().getSimpleName(),
							m_outerGSchema.getGeometryColumnName(), m_inner.getId());
	}
}
