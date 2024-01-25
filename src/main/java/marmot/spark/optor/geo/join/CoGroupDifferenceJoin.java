package marmot.spark.optor.geo.join;

import java.util.Iterator;
import java.util.List;

import org.locationtech.jts.geom.Geometry;

import marmot.Column;
import marmot.GRecordSchema;
import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.support.SafeDifference;
import marmot.plan.SpatialJoinOptions;
import marmot.spark.MarmotRDD;
import marmot.spark.MarmotSpark;
import marmot.spark.geo.cluster.QuadSpacePartitioner;
import marmot.type.DataType;
import marmot.type.GeometryDataType;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CoGroupDifferenceJoin extends CoGroupNLSpatialJoin {
	private static final long serialVersionUID = 1L;
	
	private Column m_leftGeomCol;
	
	// when executed
	private transient SafeDifference m_difference;

	public CoGroupDifferenceJoin(SpatialJoinOptions opts) {
		super(true, opts);
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema leftGSchema,
										GRecordSchema rightGSchema) {
		m_leftGeomCol = leftGSchema.getGeometryColumn();
		GeometryDataType geomType = (GeometryDataType)m_leftGeomCol.type();
		
		RecordSchema outSchema = leftGSchema.getRecordSchema();
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
		
		return m_leftGSchema.derive(outSchema);
	}

	@Override
	protected void buildContext() {
		GeometryDataType geomType = (GeometryDataType)m_leftGeomCol.type();
		m_difference = new SafeDifference(geomType.toGeometries()).setReduceFactor(0);
	}
	
	public static MarmotRDD apply(MarmotRDD left, MarmotRDD right, List<String> quadKeys,
									SpatialJoinOptions opts) {
		CoGroupDifferenceJoin join = new CoGroupDifferenceJoin(opts);
		join.initialize(left.getMarmotSpark(), left.getGRecordSchema(), right.getGRecordSchema());
		return new MarmotRDD(left.getMarmotSpark(), join.getGRecordSchema(),
							join.apply(left, right, quadKeys));
	}
	
	public static MarmotRDD apply(MarmotRDD left, MarmotRDD right, QuadSpacePartitioner partitioner,
									SpatialJoinOptions opts) {
		CoGroupDifferenceJoin join = new CoGroupDifferenceJoin(opts);
		join.initialize(left.getMarmotSpark(), left.getGRecordSchema(), right.getGRecordSchema());
		return new MarmotRDD(left.getMarmotSpark(), join.getGRecordSchema(),
							join.apply(left, right, partitioner));
	}

	@Override
	protected FStream<Record> combine(Record left, FStream<Record> right) {
		// outer 레이어의 각 레코드에 대해 matching되는 모든 inner record들이
		// 존재하는 경우, outer 레코드의 공간 정보에서 매칭되는 모든 inner record들의
		// 공간 정보 부분을 모두 제거한다.
		//
		Geometry outGeom = getLeftGeometry(left);
		Iterator<Record> iter = right.iterator();
		while ( iter.hasNext() && !outGeom.isEmpty() ) {
			Geometry inGeom = getRightGeometry(iter.next());
			outGeom = m_difference.apply(outGeom, inGeom);
		}
		
		if ( !outGeom.isEmpty() ) {
			Record result = left.duplicate();
			result.set(m_leftGeomCol.ordinal(), outGeom);
			return FStream.of(result);
		}
		else {
			return FStream.empty();
		}
	}
}
