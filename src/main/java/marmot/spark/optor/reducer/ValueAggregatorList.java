package marmot.spark.optor.reducer;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function2;

import marmot.RecordSchema;
import marmot.spark.RecordLite;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ValueAggregatorList implements Serializable {
	private final ValueAggregator[] m_aggrs;
	
	public ValueAggregatorList(ValueAggregator... aggrs) {
		m_aggrs = aggrs;
	}
	
	public void initialize(RecordSchema inputSchema) {
		for ( int i =0; i < m_aggrs.length; ++i ) {
			m_aggrs[i].initialize(i, inputSchema);
		}
	}
	
	public RecordLite getZeroValue() {
		Object[] zeros = new Object[m_aggrs.length];
		for ( int i =0; i < m_aggrs.length; ++i ) {
			zeros[i] = m_aggrs[i].getZeroValue();
		}
		
		return RecordLite.of(zeros);
	}
	
	public Function2<RecordLite,RecordLite,RecordLite> getSequencer() {
		return new Sequencer();
	}
	
	public Function2<RecordLite,RecordLite,RecordLite> getCombiner() {
		return new Combiner();
	}
	
	public RecordLite toFinalValue(RecordLite rec) {
		for ( int i =0; i < m_aggrs.length; ++i ) {
			m_aggrs[i].toFinalValue(rec);
		}
		return rec;
	}
	
	private class Sequencer implements Function2<RecordLite,RecordLite,RecordLite> {
		private static final long serialVersionUID = 1L;

		@Override
		public RecordLite call(RecordLite accum, RecordLite value) throws Exception {
			for ( int i =0; i < m_aggrs.length; ++i ) {
				m_aggrs[i].collect(accum, value);
			}
			return accum.duplicate();
		}
	}
	
	private class Combiner implements Function2<RecordLite,RecordLite,RecordLite> {
		private static final long serialVersionUID = 1L;

		@Override
		public RecordLite call(RecordLite accum1, RecordLite accum2) throws Exception {
			for ( int i =0; i < m_aggrs.length; ++i ) {
				m_aggrs[i].combine(accum1, accum2);
			}
			return accum1;
		}
	}
}
