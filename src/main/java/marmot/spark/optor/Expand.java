package marmot.spark.optor;

import java.util.Arrays;

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
import marmot.type.DataType;
import utils.Throwables;
import utils.Utilities;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Expand extends RecordLevelRDDFunction {
	private static final long serialVersionUID = 1L;

	static final Logger s_logger = LoggerFactory.getLogger(Expand.class);

	private final String m_columnDecls;
	private boolean[] m_expandedMap;
	private FOption<RecordScript> m_script = FOption.empty();
	
	// initialized when executed
	private transient FOption<RecordScriptExecution> m_scriptExec = FOption.empty();
	// MVEL 스크립트 처리시 레코드의 각 컬럼 값과 기타 임시 변수 값을 기록.
	private transient ColumnVariableResolverFactory m_vrFact;
	
	public Expand(String columnDecls) {
		super(true);
		Utilities.checkNotNullArgument(columnDecls, "added columns are null");
		
		m_columnDecls = columnDecls;
	}
	
	public Expand setInitializer(RecordScript initializer) {
		Utilities.checkNotNullArgument(initializer, "column initializer is null");
		
		m_script = FOption.ofNullable(initializer);
		
		return this;
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inputGSchema) {
		RecordSchema inputSchema = inputGSchema.getRecordSchema();
		RecordSchema expandedSchema = RecordSchema.parse(m_columnDecls);
		RecordSchema outSchema = expandedSchema.streamColumns()
												.fold(inputSchema.toBuilder(),
														(b,c) -> b.addOrReplaceColumnAll(c))
												.build();
		
		m_expandedMap = new boolean[outSchema.getColumnCount()];
		Arrays.fill(m_expandedMap, false); 
		expandedSchema.streamColumns()
					.forEach(col -> m_expandedMap[outSchema.getColumn(col.name()).ordinal()] = true);
		
		return inputGSchema.derive(outSchema);
	}

	@Override
	protected void buildContext() {
		m_scriptExec = m_script.map(RecordScriptExecution::of);
		m_scriptExec.ifPresent(init -> {
			m_vrFact = new ColumnVariableResolverFactory(getRecordSchema(), init.getArgumentAll());
			init.initialize(m_vrFact);
			
			getLogger().debug("initialized={}", this);
		});
	}

	@Override
	protected RecordLite mapRecord(RecordLite rec) {
		Record input = rec.toRecord(getInputRecordSchema());
		Record output = DefaultRecord.of(getRecordSchema()); 
		
		output.setAll(input.getAll());
		
		if ( m_scriptExec.isPresent() ) {
			m_vrFact.bind(output);
			
			try {
				m_scriptExec.get().execute(m_vrFact);
				
				getRecordSchema().streamColumns()
								.forEach(col -> {
									Object v = output.get(col.ordinal());
									output.set(col.ordinal(), DataUtils.cast(v, col.type()));
								});
			}
			catch ( Throwable e ) {
				e = Throwables.unwrapThrowable(e);
				Throwables.throwIfInstanceOf(e, RecordSetException.class);
				
				throw new RecordSetException("fails to run script on the record: " + input + " cause=" + e);
			}
		}
		else {
			for ( int i =0; i < m_expandedMap.length; ++i ) {
				if ( m_expandedMap[i] ) {
					Object v = output.get(i);
					DataType type = getRecordSchema().getColumnAt(i).type();
					try {
						output.set(i, DataUtils.cast(v, type));
					}
					catch ( Exception e ) {
						Column col = getInputRecordSchema().getColumnAt(i);
						String details = String.format("fails to conversion: src_col=%s, tar_type=%s, "
														+ "value=%s, cause=%s", col, type, v, e);
						throw new RecordSetException(details);
					}
				}
			}
		}
		
		return RecordLite.from(output);
	}
	
	@Override
	public String toString() {
		return String.format("expand[%s]", m_columnDecls);
	}
}
