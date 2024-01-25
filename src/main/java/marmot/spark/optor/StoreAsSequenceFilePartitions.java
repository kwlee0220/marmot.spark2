package marmot.spark.optor;

import java.util.Iterator;
import java.util.UUID;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.Iterators;

import marmot.GRecordSchema;
import marmot.RecordSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.io.DataSetPartitionInfo;
import marmot.io.HdfsPath;
import marmot.io.MarmotFileWriteOptions;
import marmot.io.MarmotSequenceFile;
import marmot.optor.StoreDataSetOptions;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreAsSequenceFilePartitions extends AbstractRDDFunction {
	private static final long serialVersionUID = 1L;
	
	private final String m_dsId;
	private final StoreDataSetOptions m_opts;
	
	// set when initialized
	private HdfsPath m_path;
	
	public StoreAsSequenceFilePartitions(String dsId, StoreDataSetOptions opts) {
		super(false);
		
		m_dsId = dsId;
		m_opts = opts;
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inputGSchema) {
		Path rootPath = marmot.getCatalog().generateFilePath(m_dsId);
		String partId = m_opts.partitionId()
							.getOrElse(() -> UUID.randomUUID().toString());
		m_path = HdfsPath.of(marmot.getHadoopFileSystem(), rootPath).child(partId);
		
		return new GRecordSchema(DataSetPartitionInfo.SCHEMA);
	}
	
	@Override
	protected Iterator<RecordLite> mapPartitionWithIndex(int partIdx, Iterator<RecordLite> iter) {
		HdfsPath partPath = m_path.child(String.format("%05d", partIdx));
		GeometryColumnInfo gcInfo = m_inputGSchema.getGeometryColumnInfo().getOrNull();
		RecordSet rset = toRecordSet(iter);
		MarmotFileWriteOptions opts = m_opts.writeOptions();
		DataSetPartitionInfo dspInfo = MarmotSequenceFile.store(partPath, rset, gcInfo, opts).call();
		
		RecordLite dspRec = RecordLite.of(dspInfo.toRecord().getAll());
		return Iterators.singletonIterator(dspRec);
	}

	@Override
	protected Iterator<RecordLite> mapPartition(Iterator<RecordLite> iter) {
		throw new AssertionError("Should not be called: " + getClass());
	}
}
