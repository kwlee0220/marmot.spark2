package marmot.spark.optor.geo;

import java.util.Iterator;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Geometry;

import marmot.Column;
import marmot.GRecordSchema;
import marmot.RecordSchema;
import marmot.dataset.GeometryColumnInfo;
import marmot.plan.GeomOpOptions;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.spark.optor.AbstractRDDFunction;
import marmot.type.GeometryDataType;
import utils.Utilities;
import utils.func.FOption;
import utils.func.Tuple;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class SpatialRDDFunction extends AbstractRDDFunction {
	private static final long serialVersionUID = 1L;

	protected final GeomOpOptions m_options;

	private transient int m_inputGeomColIdx = -1;
	private transient GeometryDataType m_inputGeomType;
	private transient String m_outputGeomCol;
	private transient int m_outputGeomColIdx = -1;
	private transient int m_outputColCount;
	private boolean m_throwOpError = true;

	abstract protected Tuple<GeometryDataType,String> initialize(GeometryDataType inGeomType,
														GRecordSchema inputSchema);
	abstract protected @Nullable Geometry transform(Geometry geom, RecordLite inputRecord);
	
	protected SpatialRDDFunction(GeomOpOptions opts, boolean preservePartitions) {
		super(preservePartitions);
		Utilities.checkNotNullArgument(opts, "GeomOpOptions is null");
		
		m_options = opts;
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inputGSchema) {
		Column inGeomCol = inputGSchema.getGeometryColumn();
		
		Tuple<GeometryDataType,String> ret = initialize((GeometryDataType)inGeomCol.type(),
														inputGSchema);
		
		String outGeomCol = m_options.outputColumn().getOrElse(inGeomCol.name());
		RecordSchema outSchema = inputGSchema.getRecordSchema().toBuilder()
											.addOrReplaceColumn(outGeomCol, ret._1)
											.build();
		
		m_throwOpError = m_options.throwOpError().getOrElse(false);
		
		return new GRecordSchema(new GeometryColumnInfo(outGeomCol, ret._2), outSchema);
	}
	
	public final String getInputGeometryColumnName() {
		return getInputGRecordSchema().getGeometryColumnName();
	}

	@Override
	protected Iterator<RecordLite> mapPartition(Iterator<RecordLite> iter) {
		m_inputGeomColIdx = m_inputGSchema.getGeometryColumnIdx();
		m_outputColCount = m_outputGSchema.getColumnCount();
		m_outputGeomColIdx = m_outputGSchema.getGeometryColumnIdx();
		
		return FStream.from(iter)
						.flatMapFOption(this::mapRecord)
						.iterator();
	}
	
	private FOption<RecordLite> mapRecord(RecordLite input) {
		RecordLite output = RecordLite.ofLength(m_outputColCount);
		input.copyTo(output);
		
		try {
			Geometry geom = input.getGeometry(m_inputGeomColIdx);
			if ( geom == null || geom.isEmpty() ) {
				output.set(m_outputGeomColIdx, handleNullEmptyGeometry(geom));
			}
			else {
				Geometry transformed = transform(geom, input);
				if ( transformed == null ) {
					return FOption.empty();
				}
				output.set(m_outputGeomColIdx, transformed);
			}
			
			return FOption.of(output);
		}
		catch ( Exception e ) {
			if ( getLogger().isDebugEnabled() ) {
				getLogger().warn("fails to transform geometry: cause=" + e);
			}
			
			if ( m_throwOpError ) {
				throw e;
			}
			
			output.set(m_outputGeomColIdx, null);
			return FOption.of(output);
		}
	}
	
	protected Geometry handleNullEmptyGeometry(Geometry geom) {
		if ( geom == null ) {
			return null;
		}
		else if ( geom.isEmpty() ) {
			return m_inputGeomType.newInstance();
		}
		else {
			throw new AssertionError("Should not be called: " + getClass());
		}
	}
}
