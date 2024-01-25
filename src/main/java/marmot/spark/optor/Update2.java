package marmot.spark.optor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.GRecordSchema;
import marmot.spark.MarmotSpark;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Update2 extends AbstractDataFrameFunction {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(Update2.class);
	
	private final String m_colName;
	private final String m_updateExpr;
	
	public Update2(String colName, String updateExpr) {
		m_colName = colName;
		m_updateExpr = updateExpr;
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inputGSchema) {
		return inputGSchema;
	}

	@Override
	protected Dataset<Row> mapDataFrame(Dataset<Row> df) {
		return df.withColumn(m_colName, functions.expr(m_updateExpr));
	}
	
	@Override
	public String toString() {
		return String.format("%s: col=%s, update_expr=%s",
								getClass().getSimpleName(), m_colName, m_updateExpr);
	}
}
