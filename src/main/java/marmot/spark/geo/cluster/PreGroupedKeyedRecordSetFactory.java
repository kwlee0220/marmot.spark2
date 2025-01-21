package marmot.spark.geo.cluster;

import java.util.Iterator;

import utils.KeyValue;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;
import utils.stream.FStreams.AbstractFStream;
import utils.stream.KVFStream;

import marmot.spark.RecordLite;

import scala.Tuple2;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PreGroupedKeyedRecordSetFactory<K> implements KVFStream<K,FStream<RecordLite>> {
	private final Iterator<Tuple2<K,RecordLite>> m_src;

	private Tuple2<K,RecordLite> m_next;
	
	public PreGroupedKeyedRecordSetFactory(Iterator<Tuple2<K,RecordLite>> src) {
		Utilities.checkNotNullArgument(src, "RecordSet should not be null");
		
		m_src = src;
		
		if ( src.hasNext() ) {
			m_next = m_src.next();
		}
		else {
			m_next = null;
		}
	}

	@Override
	public void close() throws Exception { }

	@Override
	public FOption<KeyValue<K, FStream<RecordLite>>> next() {
		if ( m_next == null ) {
			return FOption.empty();
		}
		
		return FOption.of(KeyValue.of(m_next._1, new GroupedRecordSet(m_next)));
	}

	private class GroupedRecordSet extends AbstractFStream<RecordLite> {
		private final K m_key;
		private final RecordLite m_first;
		
		private boolean m_isFirst = true;
		private boolean m_eos = false;
		
		GroupedRecordSet(Tuple2<K,RecordLite> first) {
			m_key = first._1;
			m_first = first._2;
		}
	
		@Override
		protected void closeInGuard() throws Exception {
			if ( !m_eos ) {
				skipRemaing();
			}
		}

		@Override
		public FOption<RecordLite> nextInGuard() {
			checkNotClosed();
			
			if ( m_eos ) {
				return FOption.empty();
			}
			if ( m_isFirst ) {
				m_isFirst = false;
				return FOption.of(m_first);
			}
			if ( !m_src.hasNext() ) {
				m_next = null;
				m_eos = true;
				return FOption.empty();
			}
			
			Tuple2<K,RecordLite> next = m_src.next();
			if ( !m_key.equals(next._1) ) {
				m_next = next;
				m_eos = true;

				return FOption.empty();
			}
			
			return FOption.of(next._2);
		}
		
		private boolean skipRemaing() {
			while ( m_src.hasNext() ) {
				Tuple2<K,RecordLite> next = m_src.next();
				if ( !m_key.equals(next._1) ) {
					m_next = next;
					m_eos = true;
					
					return true;
				}
			}
			
			return false;
		}
	}
}
