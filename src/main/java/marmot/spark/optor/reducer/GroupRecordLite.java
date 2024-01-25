package marmot.spark.optor.reducer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import com.google.common.base.Objects;

import utils.stream.FStream;

import marmot.spark.RecordLite;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GroupRecordLite implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private RecordLite m_keys;	// serializable 때문에 final로 하지 않음
	private RecordLite m_tags;	// serializable 때문에 final로 하지 않음
	private RecordLite m_orders;	// serializable 때문에 final로 하지 않음
	
	public GroupRecordLite(RecordLite keys, RecordLite tags, RecordLite orders) {
		m_keys = keys;
		m_tags = tags;
		m_orders = orders;
	}
	
	public GroupRecordLite(RecordLite keys, RecordLite tags) {
		this(keys, tags, RecordLite.of());
	}
	
	public GroupRecordLite(RecordLite keys) {
		this(keys, RecordLite.of(), RecordLite.of());
	}
	
	public RecordLite keys() {
		return m_keys;
	}
	
	public RecordLite tags() {
		return m_tags;
	}
	
	public RecordLite orders() {
		return m_orders;
	}
	
	public Object[] values() {
		return FStream.of(m_keys, m_tags)
						.flatMap(r -> FStream.of(r.values()))
						.toList().toArray();
	}

	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != GroupRecordLite.class ) {
			return false;
		}
		
		GroupRecordLite other = (GroupRecordLite)obj;
		return Objects.equal(m_keys, other.m_keys);
	}
	
	@Override
	public int hashCode() {
		return m_keys.hashCode();
	}
	
	@Override
	public String toString() {
		return String.format("keys=%s, tags=%s, orders=%s", m_keys, m_tags, m_orders);
	}
	
	private void writeObject(ObjectOutputStream os) throws IOException {
		os.defaultWriteObject();
		
		os.writeObject(m_keys);
		os.writeObject(m_tags);
		os.writeObject(m_orders);
	}
	
	private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
		is.defaultReadObject();
		
		m_keys = (RecordLite)is.readObject();
		m_tags = (RecordLite)is.readObject();
		m_orders = (RecordLite)is.readObject();
	}
}
