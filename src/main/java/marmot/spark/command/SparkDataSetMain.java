package marmot.spark.command;

import marmot.spark.MarmotSpark;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;


/**
 * </ol>
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="sm_dataset",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="dataset-related commands",
		subcommands = {
			SparkDatasetCommands.ListDataSet.class,
//			DatasetCommands.Show.class,
//			SparkDatasetCommands.Schema.class,
//			DatasetCommands.Move.class,
//			DatasetCommands.SetGcInfo.class,
//			DatasetCommands.AttachGeometry.class,
			SparkDatasetCommands.Count.class,
//			DatasetCommands.Bind.class,
//			DatasetCommands.Delete.class,
//			DatasetCommands.Export.class,
//			DatasetCommands.Thumbnail.class,
		})
public class SparkDataSetMain extends MarmotSparkCommand {
	public static final void main(String... args) throws Exception {
		SparkDataSetMain cmd = new SparkDataSetMain();
		try {
			CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF, cmd.getApplicationArguments());
		}
		finally {
//			cmd.getInitialContext().shutdown();
		}
	}

	@Override
	protected void run(MarmotSpark marmot) throws Exception {
	}
}
