package marmot.spark.optor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.GRecordSchema;
import marmot.spark.MarmotSpark;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SqlFilter extends AbstractDataFrameFunction {
	private static final long serialVersionUID = 1L;
	static final Logger s_logger = LoggerFactory.getLogger(SqlFilter.class);

	private final String m_predicate;
	
	public SqlFilter(String predicate) {
		Utilities.checkNotNullArgument(predicate, "predicate");
		
		m_predicate = predicate;
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inputGSchema) {
		return inputGSchema;
	}

	@Override
	protected Dataset<Row> mapDataFrame(Dataset<Row> df) {
		return df.filter(m_predicate);
	}
	
	@Override
	public String toString() {
		return String.format("%s: predicate='%s'", getClass().getSimpleName(), m_predicate);
	}
}
