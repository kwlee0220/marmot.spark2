package marmot.spark.optor.geo.join;

import marmot.GRecordSchema;
import marmot.Record;
import marmot.plan.SpatialJoinOptions;
import marmot.spark.MarmotSpark;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CoGroupSpatialSemiJoin extends CoGroupNLSpatialJoin {
	private static final long serialVersionUID = 1L;

	public CoGroupSpatialSemiJoin(SpatialJoinOptions opts) {
		super(true, opts);
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema leftGSchema,
										GRecordSchema rightGSchema) {
		return leftGSchema;
	}

	@Override
	protected void buildContext() { }

	@Override
	protected FStream<Record> combine(Record left, FStream<Record> right) {
		return right.next().map(r -> FStream.of(left)).getOrElse(FStream.empty());
	}
}
