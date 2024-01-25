package marmot.spark.optor.geo.join;

import java.util.Iterator;

import org.locationtech.jts.geom.Geometry;

import marmot.Column;
import marmot.GRecordSchema;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.geo.join.NestedLoopMatch;
import marmot.optor.support.SafeDifference;
import marmot.plan.SpatialJoinOptions;
import marmot.spark.MarmotSpark;
import marmot.spark.dataset.SparkDataSet;
import marmot.type.DataType;
import marmot.type.GeometryDataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialDifferenceJoin extends NestedLoopSpatialJoin {
	private static final long serialVersionUID = 1L;
	
	// when initialized
	private Column m_leftGeomCol;
	
	// when executed
	private transient SafeDifference m_difference;
	private transient RecordSet m_emptyResult;
	
	public SpatialDifferenceJoin(SparkDataSet inner, SpatialJoinOptions opts) {
		super(inner, true, opts, true);
	}

	@Override
	protected GRecordSchema initialize(MarmotSpark marmot, GRecordSchema outerGSchema,
										GRecordSchema innerGSchema) {
		m_leftGeomCol = outerGSchema.getGeometryColumn();
		GeometryDataType geomType = (GeometryDataType)m_leftGeomCol.type();
		
		RecordSchema outSchema = outerGSchema.getRecordSchema();
		if ( geomType == DataType.POLYGON ) {
			outSchema = outSchema.toBuilder()
								.addOrReplaceColumn(m_leftGeomCol.name(), DataType.MULTI_POLYGON)
								.build();
		}
		else if ( geomType == DataType.LINESTRING ) {
			outSchema = outSchema.toBuilder()
								.addOrReplaceColumn(m_leftGeomCol.name(), DataType.MULTI_LINESTRING)
								.build();
		}
		
		return outerGSchema.derive(outSchema);
	}

	@Override
	protected void buildContext() {
		GeometryDataType geomType = (GeometryDataType)m_leftGeomCol.type();
		m_difference = new SafeDifference(geomType.toGeometries()).setReduceFactor(0);
		m_emptyResult = RecordSet.empty(m_inputGSchema.getRecordSchema());
	}

	@Override
	protected RecordSet doInnerLoop(NestedLoopMatch match) {
		// outer 레이어의 각 레코드에 대해 matching되는 모든 inner record들이
		// 존재하는 경우, outer 레코드의 공간 정보에서 매칭되는 모든 inner record들의
		// 공간 정보 부분을 모두 제거한다.
		//
		boolean untouched = true;
		
		Geometry outGeom = getOuterGeometry(match.getOuterRecord());
		Iterator<Record> iter = match.getInnerRecords().iterator();
		while ( iter.hasNext() && !outGeom.isEmpty() ) {
			Geometry inGeom = getInnerGeometry(iter.next());
			outGeom = m_difference.apply(outGeom, inGeom);
			
			untouched = false;
		}
		
		if ( !outGeom.isEmpty() ) {
			if ( untouched ) {
				++m_untoucheds;
			}
			else {
				++m_shrinkeds;
			}
			
			Record result = match.getOuterRecord().duplicate();
			result.set(m_leftGeomCol.ordinal(), outGeom);
			return RecordSet.of(result);
		}
		else {
			++m_eraseds;
			return m_emptyResult;
		}
	}

	private long m_untoucheds = 0;
	private long m_shrinkeds = 0;
	private long m_eraseds = 0;
	
	@Override
	public String toString() {
		return String.format("difference_join[{%s}<->%s]",
								m_leftGeomCol.name(), getRightDataSetId());
		
	}

	@Override
	protected String toString(long outerCount, long count, long velo, int clusterLoadCount) {
		String str = String.format("%s: untoucheds=%d, shrinkeds=%d, eraseds=%d",
									this, m_untoucheds, m_shrinkeds, m_eraseds);
		if ( velo >= 0 ) {
			str = str + String.format(", velo=%d/s", velo);
		}
		str = str + String.format(", load_count=%d", clusterLoadCount);
		
		return str;
	}
}
