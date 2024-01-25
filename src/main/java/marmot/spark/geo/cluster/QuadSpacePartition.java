package marmot.spark.geo.cluster;

import org.apache.spark.Partition;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class QuadSpacePartition implements Partition {
	private static final long serialVersionUID = 1L;
	
	private final String m_quadKey;
	private final int m_index;
	
	public QuadSpacePartition(int index, String quadKey) {
		m_index = index;
		m_quadKey = quadKey;
	}
	
	public String getQuadKey() {
		return m_quadKey;
	}
	
	@Override
	public int index() {
		return m_index;
	}
	
	@Override
	public String toString() {
		return "" + m_index + ":" + m_quadKey;
	}
}
