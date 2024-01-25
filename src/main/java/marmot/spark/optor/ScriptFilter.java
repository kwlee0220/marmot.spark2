package marmot.spark.optor;

import java.util.Map;

import org.slf4j.LoggerFactory;

import marmot.GRecordSchema;
import marmot.Record;
import marmot.RecordScript;
import marmot.optor.support.ColumnVariableResolverFactory;
import marmot.plan.PredicateOptions;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.support.DataUtils;
import marmot.support.RecordScriptExecution;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ScriptFilter extends RecordLevelRDDFilter {
	private static final long serialVersionUID = 1L;
	
	private final RecordScript m_script;
	private transient RecordScriptExecution m_filterExec;
	private transient ColumnVariableResolverFactory m_vrFact;

	public ScriptFilter(RecordScript script) {
		super(PredicateOptions.DEFAULT);
		
		Utilities.checkNotNullArgument(script, "predicate is null");
		
		m_script = script;
		
		setLogger(LoggerFactory.getLogger(ScriptFilter.class));
	}

	@Override
	protected void _initialize(MarmotSpark marmot, GRecordSchema inputGSchema) {
		Utilities.checkNotNullArgument(marmot, "MarmotServer is null");
		Utilities.checkNotNullArgument(inputGSchema,
									() -> String.format("op: %s: input RecordSchema is null", this));
		
		
		m_filterExec.initialize(m_vrFact);
	}

	@Override
	protected void buildContext() {
		m_filterExec = RecordScriptExecution.of(m_script);
		Map<String,Object> args = m_filterExec.getArgumentAll();
		m_vrFact = new ColumnVariableResolverFactory(m_gschema.getRecordSchema(), args).readOnly(true);
	}

	@Override
	protected boolean testRecord(RecordLite input) {
		Record record = input.toRecord(m_gschema.getRecordSchema());
		
		m_vrFact.bind(record);
		try {
			return DataUtils.asBoolean(m_filterExec.execute(m_vrFact));
		}
		catch ( Throwable e ) {
			if ( getLogger().isDebugEnabled() ) {
				String msg = String.format("fails to evaluate the predicate: '%s, record=%s",
											m_filterExec, record);
				getLogger().warn(msg);
			}
			return false;
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s: expr='%s'", getClass().getSimpleName(), m_filterExec);
	}
}