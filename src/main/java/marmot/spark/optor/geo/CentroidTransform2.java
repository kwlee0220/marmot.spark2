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
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CentroidTransform2 extends AbstractDataFrameFunction {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(CentroidTransform2.class);
	
	private final GeomOpOptions m_opts;
	
	// set when initialized
	private Column m_inGeomCol;
	private Column m_outGeomCol;
	
	public CentroidTransform2(GeomOpOptions opts) {
		m_opts = opts;
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inputGSchema) {
		String inGeomColName = inputGSchema.getGeometryColumnName();
		m_inGeomCol = functions.col(inGeomColName);
		String outGeomColName = m_opts.outputColumn().getOrElse(inGeomColName);
		m_outGeomCol = functions.col(outGeomColName);
		
		RecordSchema outSchema = inputGSchema.getRecordSchema()
											.toBuilder()
											.addOrReplaceColumn(outGeomColName, DataType.POINT)
											.build();
		return new GRecordSchema(inputGSchema.assertGeometryColumnInfo(), outSchema);
	}

	@Override
	protected Dataset<Row> mapDataFrame(Dataset<Row> df) {
		return df.select(m_outGeomCol, functions.callUDF("ST_Centroid", m_inGeomCol));
	}
	
	@Override
	public String toString() {
		return String.format("centroid[%s]", getInputGRecordSchema().getGeometryColumnName());
	}
}
