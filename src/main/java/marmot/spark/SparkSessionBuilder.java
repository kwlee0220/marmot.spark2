package marmot.spark;

import javax.annotation.Nullable;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MarmotServer builder 클래스.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SparkSessionBuilder {
	@SuppressWarnings("unused")
	private static final Logger s_logger = LoggerFactory.getLogger(SparkSessionBuilder.class);
	
	private @Nullable String m_applName = null;
	private @Nullable String m_execMemorySize = null;
	private @Nullable String m_driverMemorySize = null;
	private @Nullable String m_maxResultSize = null;
	private @Nullable String m_dirverHost = null;
	
	public SparkSession build()
		throws Exception {
		SparkSession.Builder builder = SparkSession.builder()
													.config(new SparkConf());
		if ( m_applName != null ) {
			builder = builder.appName(m_applName);
		}
		if ( m_execMemorySize != null ) {
			builder = builder.config("spark.executor.memory", m_execMemorySize);
		}
		if ( m_driverMemorySize != null ) {
			builder = builder.config("spark.driver.memory", m_driverMemorySize);
		}
		if ( m_maxResultSize != null ) {
			builder = builder.config("spark.driver.maxResultSize", m_maxResultSize);
		}
		if ( m_dirverHost != null ) {
			builder = builder.config("spark.driver.host", m_dirverHost);
		}
		return builder.getOrCreate();
	}
}
