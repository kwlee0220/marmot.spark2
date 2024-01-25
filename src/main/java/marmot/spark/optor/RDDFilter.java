package marmot.spark.optor;

import java.io.Serializable;
import java.util.Iterator;
import java.util.function.Function;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.GRecordSchema;
import marmot.RecordSchema;
import marmot.spark.MarmotRDD;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import utils.LoggerSettable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class RDDFilter implements Function<MarmotRDD,MarmotRDD>, Serializable,
											LoggerSettable {
	private static final long serialVersionUID = 1L;

	protected MarmotSpark m_marmot;
	protected GRecordSchema m_gschema;
	protected Logger m_logger = LoggerFactory.getLogger(getClass());
	
	protected abstract void _initialize(MarmotSpark marmot, GRecordSchema inputGSchema);
	protected abstract Iterator<RecordLite> filterPartition(Iterator<RecordLite> iter);
	
	/**
	 * 데이터프레임 함수를 초기화시킨다.
	 * 
	 * @param marmot	marmot 객체.
	 * @param inputSchema	입력 레코드세트의 스키마 정보.
	 */
	public void initialize(MarmotSpark marmot, GRecordSchema inputSchema) {
		m_marmot = marmot;
		m_gschema = inputSchema;
	}
	
	public boolean isInitialized() {
		return m_marmot != null;
	}
	
	public void checkInitialized() {
		if ( m_marmot == null ) {
			throw new IllegalStateException("not initialized");
		}
	}
	
	public final GRecordSchema getGRecordSchema() {
		return m_gschema;
	}
	
	public final RecordSchema getRecordSchema() {
		return m_gschema.getRecordSchema();
	}
	
	@Override
	public MarmotRDD apply(MarmotRDD input) {
		initialize(input.getMarmotSpark(), input.getGRecordSchema());
		JavaRDD<RecordLite> applied = filter(input.getJavaRDD());
		return new MarmotRDD(input.getMarmotSpark(), getGRecordSchema(), applied);
	}
	
	protected JavaRDD<RecordLite> filter(JavaRDD<RecordLite> input) {
		return input.mapPartitionsWithIndex(this::filterPartitionWithIndex, true);
	}
	protected Iterator<RecordLite> filterPartitionWithIndex(int partIdx, Iterator<RecordLite> iter) {
		return filterPartition(iter);
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
}
