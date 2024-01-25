package marmot.spark.module;

import marmot.optor.AggregateFunction;
import marmot.spark.MarmotRDD;
import marmot.spark.RecordLite;
import marmot.spark.optor.RDDFunction;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Rescale implements RDDFunction {
	private final String m_colName;

	public Rescale(String colName) {
		Utilities.checkNotNullArgument(colName, "rescaling column name");
		
		m_colName = colName;
	}

	@Override
	public MarmotRDD apply(MarmotRDD input) {
		if ( input.getGRecordSchema().findColumn(m_colName).isAbsent() ) {
			throw new IllegalArgumentException("invalid rescaling column: " + m_colName);
		}
		
		String colDecls = String.format("%s:double", m_colName);
		AggregateFunction[] aggrs = new AggregateFunction[] {
			AggregateFunction.MIN(m_colName).as(m_colName + "_min"),
			AggregateFunction.MAX(m_colName).as(m_colName + "_max"),
		};
		RecordLite stats = input.expand(colDecls)
								.aggregate(aggrs)
								.takeFirst();
		double min = stats.getDouble(0);
		double range = stats.getDouble(1) - stats.getDouble(0);
		String updExpr = String.format("%s = (%s - %f)/%f", m_colName, m_colName, min, range);

		return input.expand(colDecls)
					.updateScript(updExpr);
	}
	
	@Override
	public String toString() {
		return String.format("%s: column=%s", getClass().getSimpleName(),
							m_colName);
	}
}
