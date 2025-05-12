package marmot.spark.optor.geo.join;

import java.util.List;

import utils.KeyValue;
import utils.func.FOption;
import utils.stream.KeyValueFStream;
import utils.stream.KeyedGroups;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class FindBiggestGroupWithinWindow<K,V> implements KeyValueFStream<K,List<V>> {
	private final KeyValueFStream<K, V> m_kvStrm;
	private final KeyedGroups<K, V> m_groups;
	private final int m_maxLength;
	private final int m_retainLength;
	private int m_length;	// current length of the window
	private boolean m_eos = false;
	private K m_lastSelectedGroup = null;
	
	FindBiggestGroupWithinWindow(KeyValueFStream<K, V> kvStrm, int maxLength, int retainLength) {
		m_kvStrm = kvStrm;
		m_groups = KeyedGroups.create();
		m_maxLength = maxLength;
		m_retainLength = retainLength;
		m_length = 0;
		m_lastSelectedGroup = null;
	}
	
	FindBiggestGroupWithinWindow(KeyValueFStream<K, V> kvStrm, int maxLength) {
		this(kvStrm, maxLength, maxLength);
	}

	@Override
	public void close() throws Exception {
		m_kvStrm.close();
	}

	@Override
	public FOption<KeyValue<K,List<V>>> next() {
		if ( !m_eos ) {
			fill();
		}
		
		if ( m_lastSelectedGroup != null ) {
			List<V> values = m_groups.get(m_lastSelectedGroup);
			if ( values.size() >= m_retainLength ) {
				values = m_groups.remove(m_lastSelectedGroup).get();
				m_length -= values.size();
				
				return FOption.of(KeyValue.of(m_lastSelectedGroup, values));
			}
//			else if ( values.size() > 0 ) {
//				System.out.printf("skip too small group: group size=%d, retain_size=%d%n", values.size(), m_retainLength);
//			}
//			else {
//				System.out.println("---------");
//			}
		}
		
		return m_groups.fstream()
						.takeTopK(1, (g1,g2) -> Integer.compare(g2.value().size(), g1.value().size()))
						.findFirst()
						.ifPresent(kv -> {
							m_lastSelectedGroup = kv.key();
							m_groups.remove(kv.key());
							m_length -= kv.value().size();
						});
	}

	// 'kvStrm'에서 window의 크기가 'm_maxLength'가 되도록 데이터를 채운다.
	private void fill() {
		FOption<KeyValue<K,V>> okv = null;
		while ( m_length < m_maxLength && (okv = m_kvStrm.next()).isPresent() ) {
			KeyValue<K,V> kv = okv.get();
			
			m_groups.add(kv.key(), kv.value());
			++m_length;
		}
		
		m_eos = okv != null && okv.isAbsent();
	}
}
