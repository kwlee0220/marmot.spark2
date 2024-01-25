package marmot.spark.optor;

import java.util.Iterator;

import marmot.plan.PredicateOptions;
import marmot.spark.RecordLite;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class RecordLevelRDDFilter extends RDDFilter {
	private static final long serialVersionUID = 1L;
	
	private final PredicateOptions m_opts;
	private final boolean m_negated;

	protected abstract void buildContext();
	protected abstract boolean testRecord(RecordLite input);
	
	protected RecordLevelRDDFilter(PredicateOptions opts) {
		m_opts = opts;
		m_negated = opts.negated().getOrElse(false);
	}

	@Override
	protected Iterator<RecordLite> filterPartition(Iterator<RecordLite> input) {
		buildContext();
		
		return FStream.from(input)
						.filter(r -> m_negated ? !testRecord(r) : testRecord(r))
						.iterator();
	}
}
