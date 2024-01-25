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
public abstract class AbstractBinaryDataFrameFunction implements BinaryDataFrameFunction {
	private static final long serialVersionUID = 1L;
	
	protected MarmotSpark m_marmot;
	protected GRecordSchema m_leftGSchema;
	protected GRecordSchema m_rightGSchema;
	protected GRecordSchema m_outputGSchema;
	protected Logger m_logger = LoggerFactory.getLogger(getClass());
	
	protected abstract GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema leftGSchema,
												GRecordSchema rightGSchema);
	protected abstract Dataset<Row> combine(Dataset<Row> left, Dataset<Row> right);
	
	/**
	 * 데이터프레임 함수를 초기화시킨다.
	 * 
	 * @param marmot	marmot 객체.
	 * @param inputSchema	입력 레코드세트의 스키마 정보.
	 */
	public void initialize(MarmotSpark marmot, GRecordSchema leftGSchema, GRecordSchema rightGSchema) {
		m_marmot = marmot;
		m_leftGSchema = leftGSchema;
		m_rightGSchema = rightGSchema;
		m_outputGSchema = _initialize(marmot, leftGSchema, rightGSchema);
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
	public final GRecordSchema getLeftGRecordSchema() {
		return m_leftGSchema;
	}
	
	public final RecordSchema getLeftRecordSchema() {
		return m_leftGSchema.getRecordSchema();
	}
	
	public final GRecordSchema getRightGRecordSchema() {
		return m_rightGSchema;
	}
	
	public final RecordSchema getRightRecordSchema() {
		return m_rightGSchema.getRecordSchema();
	}
	
	public final GRecordSchema getGRecordSchema() {
		return m_outputGSchema;
	}
	
	public final RecordSchema getRecordSchema() {
		return m_outputGSchema.getRecordSchema();
	}

	@Override
	public MarmotDataFrame apply(MarmotDataFrame left, MarmotDataFrame right) {
		checkInitialized();
		
		MarmotSpark marmot = left.getMarmotSpark();
		initialize(marmot, left.getGRecordSchema(), right.getGRecordSchema());
		Dataset<Row> output = combine(left.getDataFrame(), right.getDataFrame());
		return new MarmotDataFrame(marmot, m_outputGSchema, output);
	}
}