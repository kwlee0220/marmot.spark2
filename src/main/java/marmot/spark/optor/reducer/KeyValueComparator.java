package marmot.spark.optor.reducer;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

import marmot.RecordSchema;
import marmot.io.MultiColumnKey;
import marmot.optor.KeyColumn;
import marmot.optor.NullsOrder;
import marmot.optor.SortOrder;
import marmot.spark.RecordLite;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class KeyValueComparator implements Comparator<RecordLite>, Serializable {
	private static final long serialVersionUID = 1L;
	
	private final int[] m_keyColIdxes;
	private final SortOrder[] m_orders;
	private final NullsOrder[] m_nullsOrders;
	
	public KeyValueComparator(MultiColumnKey keys, RecordSchema schema) {
		m_keyColIdxes = new int[keys.length()];
		m_orders = new SortOrder[keys.length()];
		Arrays.fill(m_orders, SortOrder.ASC);
		m_nullsOrders = new NullsOrder[keys.length()];
		Arrays.fill(m_nullsOrders, NullsOrder.FIRST);
		
		for ( int i =0; i < keys.length(); ++i ) {
			KeyColumn kc = keys.getKeyColumnAt(i);
			
			m_keyColIdxes[i] = schema.getColumn(kc.name()).ordinal();
			m_orders[i] = kc.sortOrder();
			m_nullsOrders[i] = kc.nullsOrder();
		}
	}
	
	@Override
	public int compare(RecordLite r1, RecordLite r2) {
		if ( r1 == r2 ) {
			return 0;
		}
		
		for ( int i =0; i < m_keyColIdxes.length; ++i ) {
			int colIdx = m_keyColIdxes[i];
			final Object v1 = r1.get(colIdx);
			final Object v2 = r2.get(colIdx);
			SortOrder order = m_orders[i];

			if ( v1 != null && v2 != null ) {
				// GroupKeyValue에 올 수 있는 객체는 모두 Comparable 이라는 것을 가정한다.
				@SuppressWarnings({ "rawtypes", "unchecked" })
				int cmp = ((Comparable)v1).compareTo(v2);
				if ( cmp != 0 ) {
					return (order == SortOrder.ASC) ? cmp : -cmp;
				}
			}
			else if ( v1 == null && v2 == null ) { }
			else if ( v1 == null ) {
				if ( order == SortOrder.ASC ) {
					return (m_nullsOrders[i] == NullsOrder.FIRST) ? -1 : 1;
				}
				else {
					return (m_nullsOrders[i] == NullsOrder.FIRST) ? 1 : -1;
				}
			}
			else if ( v2 == null ) {
				if ( order == SortOrder.ASC ) {
					return (m_nullsOrders[i] == NullsOrder.FIRST) ? 1 : -1;
				}
				else {
					return (m_nullsOrders[i] == NullsOrder.FIRST) ? -1 : 1;
				}
			}
		}
		
		return 0;
	}
}
