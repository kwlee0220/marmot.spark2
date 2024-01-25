package marmot.spark.command;

import marmot.spark.MarmotSpark;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;
import test.TestEnergy2;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="marmot_spark_session",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="create a MarmotSparkSession")
public class TestSparkMain extends MarmotSparkCommand {
	@Override
	protected void run(MarmotSpark marmot) throws Exception {
		TestEnergy2.run(marmot);
	}

	public static final void main(String... args) throws Exception {
		TestSparkMain cmd = new TestSparkMain();
		try {
			CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF);
		}
		finally {
//			cmd.getInitialContext().shutdown();
		}
	}

//	public static final void main(String... args) throws Exception {
//		SparkConf conf = new SparkConf();
//		SparkContext sc = new SparkContext(conf);
//		
//		System.out.printf("master=%s, deploy=%s", sc.master(), sc.deployMode());
//	}
}
