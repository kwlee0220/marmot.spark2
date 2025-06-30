package marmot.spark.optor.geo.cluster;

import static utils.UnitUtils.toByteSizeString;

import java.util.Iterator;

import javax.annotation.Nullable;

import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.GRecordSchema;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.geo.CoordinateTransform;
import marmot.io.RecordWritable;
import marmot.optor.geo.cluster.SampledQuadSpace;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.spark.Rows;
import marmot.spark.optor.AbstractRDDFunction;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class EstimateQuadKeys extends AbstractRDDFunction {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(EstimateQuadKeys.class);
	
	public static final String COLUMN_QUADKEY = SampledQuadSpace.COLUMN_QUADKEY;
	public static final String COLUMN_LENGTH = SampledQuadSpace.COLUMN_LENGTH;
	public static RecordSchema SCHEMA = SampledQuadSpace.SCHEMA;
	
	private final double m_sampleRatio;
	private @Nullable Envelope m_validBounds;
	private long m_clusterSize = -1;

	private long m_splitSize;
	private int m_geomColIdx = -1;
	
	public EstimateQuadKeys(double sampleRatio) {
		super(false);
		Utilities.checkArgument(sampleRatio > 0, "invalid sampleRatio");
		
		m_sampleRatio = sampleRatio;
		setLogger(s_logger);
	}
	
	public EstimateQuadKeys setValidBounds(Envelope bounds) {
		m_validBounds = bounds;
		return this;
	}
	
	public EstimateQuadKeys setClusterSize(long size) {
		m_clusterSize = size;
		return this;
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inGSchema) {
		m_geomColIdx = m_inputGSchema.getGeometryColumnIdx();
		long clusterSize = m_clusterSize;
		if ( clusterSize <= 0 ) {
			clusterSize = marmot.getDefaultClusterSize();
		}
		m_splitSize = Math.round(clusterSize * m_sampleRatio);
		
		return new GRecordSchema(SCHEMA);
	}

	@Override
	public JavaRDD<RecordLite> apply(JavaRDD<RecordLite> input) {
		return input.sample(false, m_sampleRatio)
					.mapPartitions(this::toRecordLength)
					.coalesce(1)
					.mapPartitions(this::buildQuadSpace);
	}

	@Override
	protected Iterator<RecordLite> mapPartition(Iterator<RecordLite> iter) {
		throw new AssertionError("should not be called");
	}
	
	private Iterator<RecordLite> toRecordLength(Iterator<RecordLite> iter) {
		String inSrid = m_inputGSchema.getSrid();
		CoordinateTransform trans = CoordinateTransform.getTransformToWgs84(inSrid);
		
		return FStream.from(iter)
						.map(rec -> toLengthRecord(rec, trans))
						.filter(rec -> rec != NULL)
						.iterator();
	}

	@Override
	public String toString() {
		return String.format("geom=%s, sample=%.3f%%, split_size=%s",
							m_inputGSchema.assertGeometryColumnInfo(),
							m_sampleRatio * 100, toByteSizeString(m_splitSize));
	}
	
	private static final RecordLite NULL = RecordLite.of(null, null);
	private RecordLite toLengthRecord(RecordLite rec, CoordinateTransform trans) {
		Geometry geom = rec.getGeometry(m_geomColIdx);
		if ( geom == null || geom.isEmpty() ) {
			return NULL;
		}
		
		Envelope envl = geom.getEnvelopeInternal();
		if ( m_validBounds != null ) {
			if ( !m_validBounds.intersects(envl) ) {
				return NULL;
			}
		}

		Envelope envl84 = (trans != null) ? trans.transform(envl) : envl;
		if ( envl84.getMaxY() > 85 || envl84.getMinY() < -85
			|| envl84.getMaxX() > 180 || envl84.getMinX() < -180 ) {
			return NULL;
		}
		
		RecordSchema schema = m_inputGSchema.getRecordSchema();
		int rlen = RecordWritable.from(schema, rec.values()).toBytes().length;
		return RecordLite.of(new Object[] {envl84, rlen});
	}
	
	@SuppressWarnings("resource")
	private Iterator<RecordLite> buildQuadSpace(Iterator<RecordLite> iter) {
		RecordSet input = Rows.toRecordSet(SCHEMA, iter);
		return new SampledQuadSpace(input, m_splitSize)
					.fstream()
					.map(RecordLite::from)
					.iterator();
	}
}
