package marmot.spark.optor;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.GRecordSchema;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.spark.MarmotRDD;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.spark.Rows;
import utils.LoggerSettable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractRDDFunction implements RDDFunction, Serializable, LoggerSettable {
	private static final long serialVersionUID = 1L;
	
	private final boolean m_preservePartitions;

	protected MarmotSpark m_marmot;
	protected GRecordSchema m_inputGSchema;
	protected GRecordSchema m_outputGSchema;
	protected Logger m_logger = LoggerFactory.getLogger(getClass());
	
	protected abstract GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inputGSchema);
	protected abstract Iterator<RecordLite> mapPartition(Iterator<RecordLite> iter);
	
	protected AbstractRDDFunction(boolean preservePartitions) {
		m_preservePartitions = preservePartitions;
	}
	
	/**
	 * 데이터프레임 함수를 초기화시킨다.
	 * 
	 * @param marmot	marmot 객체.
	 * @param inputSchema	입력 레코드세트의 스키마 정보.
	 */
	protected void initialize(MarmotSpark marmot, GRecordSchema inputSchema) {
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
	
	/**
	 * 주어진 입력 데이터프레임을 이용하여 본 연산을 수행한 결과 데이터프레임을 반환한다.
	 * 
	 * @param input		입력 데이터프레임.
	 * @param gcInfo	공간 컬럼 등록 정보
	 * @return	연산 수행결과로 생성된 레코드세트 객체.
	 */
	public MarmotRDD apply(MarmotRDD input) {
		initialize(input.getMarmotSpark(), input.getGRecordSchema());
		JavaRDD<RecordLite> applied = apply(input.getJavaRDD());
		return new MarmotRDD(input.getMarmotSpark(), getGRecordSchema(), applied);
	}
	
	protected JavaRDD<RecordLite> apply(JavaRDD<RecordLite> input) {
		checkInitialized();
		
		return input.mapPartitionsWithIndex(this::mapPartitionWithIndex, m_preservePartitions);
	}
	protected Iterator<RecordLite> mapPartitionWithIndex(int partIdx, Iterator<RecordLite> iter) {
		checkInitialized();
		
		return mapPartition(iter);
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
	
	protected RecordSet toRecordSet(Iterator<RecordLite> iter) {
		return Rows.toRecordSet(getInputGRecordSchema().getRecordSchema(), iter);
	}
}
