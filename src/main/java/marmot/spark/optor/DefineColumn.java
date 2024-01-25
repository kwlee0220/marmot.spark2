package marmot.spark.optor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Column;
import marmot.GRecordSchema;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordScript;
import marmot.RecordSetException;
import marmot.optor.support.ColumnVariableResolverFactory;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.support.DataUtils;
import marmot.support.DefaultRecord;
import marmot.support.RecordScriptExecution;
import utils.Throwables;
import utils.Utilities;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DefineColumn extends RecordLevelRDDFunction {
	private static final long serialVersionUID = 1L;

	static final Logger s_logger = LoggerFactory.getLogger(DefineColumn.class);

	private final String m_colDecl;
	private final FOption<RecordScript> m_columnInitScript;
	
	private Column m_definedCol;	// set during initialization
	private int m_ordinal = -1;
	
	// initialized when executed
	private transient FOption<RecordScriptExecution> m_columnInitExec;

	// MVEL 스크립트 처리시 레코드의 각 컬럼 값과 기타 임시 변수 값을 기록.
	private ColumnVariableResolverFactory m_vrFact;
	
	public DefineColumn(String colDecl, RecordScript initalizer) {
		super(true);
		Utilities.checkNotNullArgument(colDecl, "column declaration");
		Utilities.checkNotNullArgument(initalizer, "column initializer");
		
		m_colDecl = colDecl;
		m_columnInitScript = FOption.of(initalizer);
	}
	
	public DefineColumn(String colDecl) {
		super(true);
		Utilities.checkNotNullArgument(colDecl, "column declaration");
		
		m_colDecl = colDecl;
		m_columnInitScript = FOption.empty();
		m_columnInitExec = FOption.empty();
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inputGSchema) {
		RecordSchema schema = RecordSchema.parse(m_colDecl);
		if ( schema.getColumnCount() > 1 ) {
			throw new IllegalArgumentException("too many columns are defined: decl=" + m_colDecl);
		}
		else if ( schema.getColumnCount() == 0 ) {
			throw new IllegalArgumentException("no column is defined");
		}
		m_definedCol = schema.getColumnAt(0);
		
		RecordSchema outSchema = inputGSchema.getRecordSchema()
											.toBuilder()
											.addOrReplaceColumn(m_definedCol.name(), m_definedCol.type())
											.build();
		m_ordinal = outSchema.getColumn(m_definedCol.name()).ordinal();
		
		return inputGSchema.derive(outSchema);
	}

	@Override
	protected void buildContext() {
		m_columnInitExec = m_columnInitScript.map(RecordScriptExecution::of);
		m_columnInitExec.ifPresent(init -> {
			m_vrFact = new ColumnVariableResolverFactory(getRecordSchema(), init.getArgumentAll());
			init.initialize(m_vrFact);
			
			getLogger().debug("initialized={}", this);
		});
	}

	@Override
	protected RecordLite mapRecord(RecordLite input) {
		checkInitialized();
		
		Record output = DefaultRecord.of(getRecordSchema());
		output.setAll(input.values());
		
		if ( m_columnInitExec.isPresent() ) {
			m_vrFact.bind(output);
			
			try {
				Object initValue = m_columnInitExec.get().execute(m_vrFact);
				output.set(m_ordinal, DataUtils.cast(initValue, m_definedCol.type()));
			}
			catch ( Throwable e ) {
				e = Throwables.unwrapThrowable(e);
				Throwables.throwIfInstanceOf(e, RecordSetException.class);
				
				throw new RecordSetException("fails to run script on the record: " + input + ", cause=" + e);
			}
		}
		else if ( m_ordinal < getInputRecordSchema().getColumnCount() ) {
			// 기존 동일 컬럼의 타입이 변경되는 경우.
			Object src = input.get(m_ordinal);
			output.set(m_ordinal, DataUtils.cast(src, m_definedCol.type()));
		}
		else {
			output.set(m_ordinal, null);
		}
		
		return RecordLite.from(output);
	}
	
	@Override
	public String toString() {
		String initStr = m_columnInitScript.map(init -> String.format(" = %s", init))
											.getOrElse("");
		return String.format("%s: col_decl=%s, init=%s", getClass().getSimpleName(),
								m_colDecl, initStr);
	}
}
