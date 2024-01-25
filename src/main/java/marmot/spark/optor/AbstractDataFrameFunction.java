package marmot.spark.optor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.GRecordSchema;
import marmot.RecordSchema;
import marmot.spark.MarmotDataFrame;
import marmot.spark.MarmotSpark;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractDataFrameFunction implements DataFrameFunction {
	private static final long serialVersionUID = 1L;
	
	protected MarmotSpark m_marmot;
	protected GRecordSchema m_inputGSchema;
	protected GRecordSchema m_outputGSchema;
	protected Logger m_logger = LoggerFactory.getLogger(getClass());
	
	protected abstract GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inputGSchema);
	protected abstract Dataset<Row> mapDataFrame(Dataset<Row> df);
	
	/**
	 * 데이터프레임 함수를 초기화시킨다.
	 * 
	 * @param marmot	marmot 객체.
	 * @param inputSchema	입력 레코드세트의 스키마 정보.
	 */
	public void initialize(MarmotSpark marmot, GRecordSchema inputSchema) {
		m_marmot = marmot;
		m_inputGSchema = inputSchema;
		m_outputGSchema = _initialize(marmot, inputSchema);
	}
	
	public boolean isInitialized() {
		return m_marmot != null;
	}
	
	public void checkInitialized() {
		if ( m_marmot == null ) {
			throw new IllegalStateException("not initialized");
		}
	}
	
	/**
	 * 데이터프레임 함수 초기화시 설정된 입력 데이터프레임 스키마를 반환한다.
	 * 
	 * @return	입력 데이터프레임 스키마
	 */
	public final GRecordSchema getInputGRecordSchema() {
		return m_inputGSchema;
	}
	
	public final RecordSchema getInputRecordSchema() {
		return m_inputGSchema.getRecordSchema();
	}
	
	public final GRecordSchema getGRecordSchema() {
		return m_outputGSchema;
	}
	
	public final RecordSchema getRecordSchema() {
		return m_outputGSchema.getRecordSchema();
	}

	@Override
	public MarmotDataFrame apply(MarmotDataFrame input) {
		checkInitialized();
		
		initialize(input.getMarmotSpark(), input.getGRecordSchema());
		Dataset<Row> applied = mapDataFrame(input.getDataFrame());
		return new MarmotDataFrame(input.getMarmotSpark(), m_outputGSchema, applied);
	}
}