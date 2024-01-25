package marmot.spark.optor;

import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.GRecordSchema;
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
public abstract class RDDConsumer implements RDDOperator, LoggerSettable {
	private static final long serialVersionUID = 1L;

	protected MarmotSpark m_marmot;
	protected GRecordSchema m_inputGSchema;
	protected Logger m_logger = LoggerFactory.getLogger(getClass());
	
	protected abstract void _initialize(MarmotSpark marmot, GRecordSchema inputGSchema);
	protected abstract void consumePartition(Iterator<RecordLite> iter);
	
	/**
	 * 데이터프레임 함수를 초기화시킨다.
	 * 
	 * @param marmot	marmot 객체.
	 * @param inputGSchema	입력 레코드세트의 스키마 정보.
	 */
	public void initialize(MarmotSpark marmot, GRecordSchema inputGSchema) {
		m_marmot = marmot;
		m_inputGSchema = inputGSchema;
		
		_initialize(marmot, inputGSchema);
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
	
	public void consume(MarmotRDD input) {
		initialize(input.getMarmotSpark(), input.getGRecordSchema());
		consume(input.getJavaRDD());
	}
	
	protected void consume(JavaRDD<RecordLite> input) {
		checkInitialized();

		input.foreachPartition(this::consumePartition);
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
