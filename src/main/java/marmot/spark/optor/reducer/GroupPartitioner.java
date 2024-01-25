package marmot.spark.optor.reducer;

import org.apache.spark.Partitioner;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
final class GroupPartitioner extends Partitioner {
	private static final long serialVersionUID = 1L;
	
	private final int m_npartions;
	
	GroupPartitioner(int npartions) {
		m_npartions = npartions;
	}

	@Override
	public int getPartition(Object obj) {
		return ((GroupRecordLite)obj).hashCode() % m_npartions;
	}

	@Override
	public int numPartitions() {
		return m_npartions;
	}
}
