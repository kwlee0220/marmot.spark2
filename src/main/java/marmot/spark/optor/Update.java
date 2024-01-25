package marmot.spark.optor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.GRecordSchema;
import marmot.Record;
import marmot.RecordScript;
import marmot.RecordSetException;
import marmot.optor.support.ColumnVariableResolverFactory;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.support.DefaultRecord;
import marmot.support.RecordScriptExecution;
import utils.Throwables;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Update extends RecordLevelRDDFunction {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(Update.class);

	private final RecordScript m_script;

	private transient RecordScriptExecution m_updateExec;
	// MVEL 스크립트 처리시 레코드의 각 컬럼 값과 기타 임시 변수 값을 기록.
	private transient ColumnVariableResolverFactory m_vrFact;
	
	public Update(String updateExpr) {
		this(RecordScript.of(updateExpr));
	}
	
	public Update(RecordScript script) {
		super(true);
		Utilities.checkNotNullArgument(script, "update script is null");
		
		m_script = script;
		setLogger(s_logger);
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inputGSchema) {
		return inputGSchema;
	}

	@Override
	protected void buildContext() {
		m_vrFact = new ColumnVariableResolverFactory(getInputRecordSchema(), m_script.getArgumentAll());
		m_updateExec = RecordScriptExecution.of(m_script);
		m_updateExec.initialize(m_vrFact);
	}

	@Override
	protected RecordLite mapRecord(RecordLite rec) {
		Record input = rec.toRecord(getInputRecordSchema());
		m_vrFact.bind(input);

		Record output = DefaultRecord.of(getRecordSchema()); 
		try {
			m_updateExec.execute(m_vrFact);
			output.set(input);

			return RecordLite.from(output);
		}
		catch ( Throwable e ) {
			e = Throwables.unwrapThrowable(e);
			Throwables.throwIfInstanceOf(e, RecordSetException.class);
			
			throw new RecordSetException("fails to run script on the record: " + input + ", cause=" + e);
		}
	}
	
	@Override
	public String toString() {
		String expr = m_script.getScript();
		if ( expr.length() > 50 ) {
			expr = expr.substring(0, 50) + "...";
		}
		
		return String.format("update['%s']", expr);
	}
}
