package marmot.spark.command;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.text.StringSubstitutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import marmot.ConfigurationBuilder;
import marmot.spark.MarmotSpark;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;
import utils.PicocliCommand;
import utils.StopWatch;
import utils.UsageHelp;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class MarmotSparkCommand implements PicocliCommand<MarmotSpark> {
	private static final Logger s_logger = LoggerFactory.getLogger(MarmotSparkCommand.class);
	
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;
	@Mixin private ConfigurationBuilder m_confBuilder = new ConfigurationBuilder();
	
	@Option(names={"-v"}, description={"verbose"})
	private boolean m_verbose = false;
	
	private MarmotSpark m_marmot;
	private String[] m_applArgs;
	
	abstract protected void run(MarmotSpark marmot) throws Exception;
	
	@Override
	public MarmotSpark getInitialContext() {
		return m_marmot;
	}
	
	public String[] getApplicationArguments() {
		return m_applArgs;
	}
	
	@Override
	public void run() {
		StopWatch watch = StopWatch.start();
		
		Configuration conf = m_confBuilder.build();
		SparkSession spark = SparkSession.builder()
										.config(new SparkConf())
//										.appName("marmot_spark_server")
//										.master("yarn")
//										.config("spark.submit.deployMode", "client")
//										.config("spark.hadoop.fs.defaultFS",
//												conf.get("fs.defaultFS"))
//										.config("spark.hadoop.yarn.resourcemanager.address",
//												conf.get("yarn.resourcemanager.address"))
//										.config("spark.master", "yarn-client")
										.getOrCreate();
		m_marmot = new MarmotSpark(conf, spark);
		
		try {
			run(m_marmot);
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
			
			m_spec.commandLine().usage(System.out, Ansi.OFF);
		}
		finally {
			if ( m_verbose ) {
				System.out.printf("elapsed=%s%n", watch.stopAndGetElpasedTimeString());
			}
		}
	}

	@Override
	public void configureLog4j() throws IOException {
		String homeDir = FOption.ofNullable(System.getenv("MARMOT_SERVER_HOME"))
								.getOrElse(() -> System.getProperty("user.dir"));
		File propsFile = new File(homeDir, "log4j.properties");
		System.out.printf("use log4j.properties: file=%s%n", propsFile);
		
		Properties props = new Properties();
		try ( InputStream is = new FileInputStream(propsFile) ) {
			props.load(is);
		}
		
		Map<String,String> bindings = Maps.newHashMap();
		bindings.put("marmot.home", propsFile.getParentFile().toString());

		String rfFile = props.getProperty("log4j.appender.rfout.File");
		rfFile = StringSubstitutor.replace(rfFile, bindings);
		props.setProperty("log4j.appender.rfout.File", rfFile);
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("use log4j.properties from {}", propsFile);
		}
	}
}