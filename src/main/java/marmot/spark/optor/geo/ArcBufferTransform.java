package marmot.spark.optor.geo;

import static marmot.optor.AggregateFunction.CONCAT_STR;
import static marmot.optor.AggregateFunction.UNION_GEOM;
import static marmot.plan.SpatialJoinOptions.OUTPUT;

import java.io.Serializable;
import java.util.List;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import marmot.Column;
import marmot.GRecordSchema;
import marmot.RecordSchema;
import marmot.dataset.GeometryColumnInfo;
import marmot.plan.GeomOpOptions;
import marmot.plan.Group;
import marmot.spark.MarmotRDD;
import marmot.spark.geo.cluster.QuadSpacePartitioner;
import marmot.spark.optor.RDDFunction;
import marmot.type.DataType;
import marmot.type.GeometryDataType;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ArcBufferTransform implements RDDFunction, Serializable {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(ArcBufferTransform.class);
	
	private final double m_distance;
	private final boolean m_dissolve;
	private final GeomOpOptions m_opts;
	
	// optional arguments
	@Nullable private QuadSpacePartitioner m_partitioner;
	
	public ArcBufferTransform(double distance, boolean dissolve, GeomOpOptions opts) {
		Preconditions.checkArgument(Double.compare(distance, 0d) > 0,
									"invalid buffer distance: dist=" + distance);

		m_distance = distance;
		m_dissolve = dissolve;
		m_opts = opts;
	}
	
	public ArcBufferTransform setQuadSpacePartitioner(QuadSpacePartitioner partitioner) {
		m_partitioner = partitioner;
		return this;
	}
	
	public ArcBufferTransform setQuadSpacePartitioner(List<String> quadKeys) {
		m_partitioner = QuadSpacePartitioner.from(quadKeys);
		return this;
	}

	@Override
	public MarmotRDD apply(MarmotRDD input) {
		initialize(input);
		
		MarmotRDD rdd = input.buffer(m_distance)
								.assignUid("FID");
		if ( !m_dissolve ) {
			return rdd;
		}
		
		boolean done = false;
		while ( !done ) {
			rdd = rdd.cache();
			
			long prevCount = rdd.count();
			rdd = collapse(rdd);
			long curCount = rdd.count();
			if ( s_logger.isDebugEnabled() ) {
				s_logger.debug("%s: iteration(%d -> %d)", getClass().getSimpleName(),
								prevCount, curCount);
			}
			done = (curCount == prevCount);
		}
		
		return rdd;
	}
	
	private MarmotRDD collapse(MarmotRDD rdd) {
		if ( m_partitioner == null ) {
			List<String> qkeys = rdd.estimateQuadKeys(0.1, null, UnitUtils.parseByteSize("64mb"));
			m_partitioner = QuadSpacePartitioner.from(qkeys);
		}
		
		GeometryColumnInfo gcInfo = rdd.assertGeometryColumnInfo();
		return rdd.spatialJoin(rdd, gcInfo, m_partitioner,
								OUTPUT("left.*,right.{the_geom as the_geom2, FID as FID2}"))
					.aggregateByGroup(Group.ofKeys("FID").orderBy("FID2:A"),
										UNION_GEOM("the_geom2").as("the_geom"),
										CONCAT_STR("FID2", ",").as("cover"))
					.takeByGroup(Group.ofKeys("cover"), 1)
					.project("the_geom")
					.assignUid("FID");
	}

	private GRecordSchema initialize(MarmotRDD input) {
		GRecordSchema inGSchema = input.getGRecordSchema();

		Column inGeomCol = inGSchema.getGeometryColumn();
		GeometryDataType retType = initialize(inGeomCol.type(), inGSchema);
		String outGeomCol = m_opts.outputColumn().getOrElse(inGeomCol.name());
		RecordSchema outSchema = inGSchema.getRecordSchema().toBuilder()
												.addOrReplaceColumn(outGeomCol, retType)
												.build();
		return inGSchema.derive(outSchema);
	}

	private GeometryDataType
	initialize(DataType inGeomType, GRecordSchema inGSchema) {
		switch ( inGeomType.getTypeCode() ) {
			case POINT:
			case POLYGON:
			case LINESTRING:
				return GeometryDataType.POLYGON;
			default:
				return GeometryDataType.MULTI_POLYGON;
		}
	}
}
