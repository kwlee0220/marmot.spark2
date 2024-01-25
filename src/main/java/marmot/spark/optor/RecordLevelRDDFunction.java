package marmot.spark.optor;

import java.util.Iterator;

import marmot.spark.RecordLite;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class RecordLevelRDDFunction extends AbstractRDDFunction {
	private static final long serialVersionUID = 1L;

	protected abstract void buildContext();
	protected abstract RecordLite mapRecord(RecordLite input);
	
	protected RecordLevelRDDFunction(boolean preservePartitions) {
		super(preservePartitions);
	}

	@Override
	protected Iterator<RecordLite> mapPartition(Iterator<RecordLite> input) {
		buildContext();
		return FStream.from(input)
						.map(this::mapRecord)
						.iterator();
	}
}
