package marmot.spark.optor;

import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import marmot.GRecordSchema;
import marmot.io.DataSetPartitionInfo;
import marmot.spark.MarmotRDD;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class UpdateDataSetInfo extends RDDConsumer {
	private static final long serialVersionUID = 1L;
	
	private final String m_dsId;
	
	public UpdateDataSetInfo(String dsId) {
		m_dsId = dsId;
	}
	
	public static void update(MarmotSpark marmot, String dsId, FStream<DataSetPartitionInfo> infos) {
		DataSetPartitionInfo total = infos.reduce(DataSetPartitionInfo::plus);
		marmot.getCatalog().updateDataSetInfo(dsId, total);
	}
	
	public static void update(MarmotSpark marmot, String dsId, List<RecordLite> partInfos) {
		GRecordSchema partInfoSchema = new GRecordSchema(DataSetPartitionInfo.SCHEMA);
		JavaRDD<RecordLite> jrdd = marmot.getJavaSparkContext().parallelize(partInfos);
		MarmotRDD infoRDD = new MarmotRDD(marmot, partInfoSchema, jrdd);
		new UpdateDataSetInfo(dsId).consume(infoRDD);
	}

	@Override
	protected void _initialize(MarmotSpark marmot, GRecordSchema inputGSchema) { }

	@Override
	protected void consume(JavaRDD<RecordLite> input) {
		DataSetPartitionInfo total
			= input.map(rec -> DataSetPartitionInfo.from(rec.values()))
					.fold(DataSetPartitionInfo.EMPTY, DataSetPartitionInfo::plus);
		m_marmot.getCatalog().updateDataSetInfo(m_dsId, total);
	}

	@Override
	protected void consumePartition(Iterator<RecordLite> iter) {
		throw new AssertionError("should not be called");
	}
}
