package marmot.spark.optor;

import java.util.Iterator;

import marmot.spark.RecordLite;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class RecordLevelRDDFlatFunction extends AbstractRDDFunction {
	private static final long serialVersionUID = 1L;
	
	protected abstract FStream<RecordLite> flatMapRecord(RecordLite input);
	protected abstract void buildContext();
	
	protected RecordLevelRDDFlatFunction(boolean preservePartitions) {
		super(preservePartitions);
	}

	@Override
	protected Iterator<RecordLite> mapPartition(Iterator<RecordLite> input) {
		buildContext();
		return FStream.from(input)
						.flatMap(this::flatMapRecord)
						.iterator();
	}
}
