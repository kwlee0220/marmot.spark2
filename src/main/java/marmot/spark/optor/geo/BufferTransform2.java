package marmot.spark.optor.geo;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.GRecordSchema;
import marmot.RecordSchema;
import marmot.plan.GeomOpOptions;
import marmot.spark.MarmotSpark;
import marmot.spark.optor.AbstractDataFrameFunction;
import marmot.type.GeometryDataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BufferTransform2 extends AbstractDataFrameFunction {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(BufferTransform2.class);
	
	private final double m_dist;
	private final GeomOpOptions m_opts;
	
	// set when initialized
	private String m_inColName;
	private String m_outColName;
	
	public BufferTransform2(double dist, GeomOpOptions opts) {
		m_dist = dist;
		m_opts = opts;
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inputGSchema) {
		m_inColName = inputGSchema.getGeometryColumnName();
		m_outColName = m_opts.outputColumn().getOrElse(m_inColName);
		
		GeometryDataType inDataType = (GeometryDataType)inputGSchema.getColumn(m_inColName).type();
		GeometryDataType outDataType = getOutputDataType(inDataType);
		RecordSchema outSchema = inputGSchema.getRecordSchema()
											.toBuilder()
											.addOrReplaceColumn(m_outColName, outDataType)
											.build();
		return new GRecordSchema(inputGSchema.assertGeometryColumnInfo(), outSchema);
	}

	@Override
	protected Dataset<Row> mapDataFrame(Dataset<Row> df) {
		Column inCol = functions.col(m_inColName);
		Column distCol = functions.lit(m_dist);
		df = df.withColumn(m_inColName, functions.callUDF("ST_Buffer", inCol, distCol));
		if ( !m_inColName.equals(m_outColName) ) {
			df = df.withColumnRenamed(m_inColName, m_outColName);
		}
		
		return df;
	}
	
	@Override
	public String toString() {
		return String.format("buffer: col=%s, dist=%.2m, out_col=%s",
								m_inColName, m_dist, m_outColName);
	}

	private GeometryDataType getOutputDataType(GeometryDataType inGeomType) {
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
