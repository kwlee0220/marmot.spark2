package marmot.spark.optor.reducer;

import java.io.Serializable;
import java.util.Comparator;

import marmot.RecordSchema;
import marmot.io.MultiColumnKey;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GroupComparator implements Comparator<GroupRecordLite>, Serializable {
	private static final long serialVersionUID = 1L;
	
	private final KeyValueComparator m_groupColsCmptor;
	private final KeyValueComparator m_orderColsCmptor;
	
	GroupComparator(MultiColumnKey groupKeys, MultiColumnKey orderKeys, RecordSchema schema) {
		m_groupColsCmptor = new KeyValueComparator(groupKeys, schema);
		m_orderColsCmptor = new KeyValueComparator(orderKeys, schema);
	}
	
	@Override
	public int compare(GroupRecordLite rec1, GroupRecordLite rec2) {
		int ret = m_groupColsCmptor.compare(rec1.keys(), rec2.keys());
		if ( ret != 0 ) {
			return ret;
		}
		else {
			return m_orderColsCmptor.compare(rec1.orders(), rec2.orders());
		}
	}
}
