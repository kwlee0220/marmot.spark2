package marmot.spark;

import java.util.Iterator;

import marmot.Record;
import marmot.RecordSchema;
import marmot.rset.AbstractRecordSet;
import utils.io.IOUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class LocalRecordSet extends AbstractRecordSet {
	private final RecordSchema m_schema;
	private final Iterator<RecordLite> m_recIter;
	
	LocalRecordSet(RecordSchema schema, Iterator<RecordLite> iter) {
		m_schema = schema;
		m_recIter = iter;
	}
	
	@Override
	protected void closeInGuard() {
		IOUtils.closeQuietly(m_recIter);
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	@Override
	public boolean next(Record output) {
		checkNotClosed();
		
		if ( m_recIter.hasNext() ) {
			m_recIter.next().copyTo(output);
			return true;
		}
		else {
			return false;
		}
	}
}