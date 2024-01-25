package marmot.spark.optor;

import java.util.Iterator;

import marmot.GRecordSchema;
import marmot.RecordSchema;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.type.DataType;
import utils.Utilities;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AssignUid extends AbstractRDDFunction {
	private static final long serialVersionUID = 1L;
	private static final long MAX_RECS_PER_SPLIT = 100_000_000L;
	
	private final String m_uidColumn;
	
	private int m_uidColIdx;
	private int m_ncols;
	private long m_startId = -1;	// just for 'toString()'
	private long m_idGen = 0;
	
	public AssignUid(String colName) {
		super(true);
		Utilities.checkNotNullArgument(colName, "id column");
		
		m_uidColumn = colName;
	}
	
	public String getUidColumn() {
		return m_uidColumn;
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inputGSchema) {
		RecordSchema outSchema = inputGSchema.getRecordSchema()
											.toBuilder()
											.addOrReplaceColumn(m_uidColumn, DataType.LONG)
											.build();
		m_ncols = outSchema.getColumnCount();
		m_uidColIdx = outSchema.getColumn(m_uidColumn).ordinal();
		return inputGSchema.derive(outSchema);
	}

	@Override
	protected Iterator<RecordLite> mapPartitionWithIndex(int partIdx, Iterator<RecordLite> iter) {
		checkInitialized();

		m_idGen = (long)MAX_RECS_PER_SPLIT * partIdx;
		return FStream.from(iter)
						.map(this::transform)
						.iterator();
	}
	
	@Override
	protected Iterator<RecordLite> mapPartition(Iterator<RecordLite> iter) {
		throw new AssertionError();
	}
	
	@Override
	public String toString() {
		return String.format("%s: col=%s, start=%d, next=%d", getClass().getSimpleName(),
							m_uidColumn, m_startId, m_idGen);
	}

	private RecordLite transform(RecordLite input) {
		RecordLite output = RecordLite.ofLength(m_ncols);
		
		output.set(input);
		output.set(m_uidColIdx, m_idGen++);
		return output;
	}
}