package marmot.spark.geo.cluster;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Partitioner;

import utils.Utilities;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class QuadSpacePartitioner extends Partitioner {
	private static final long serialVersionUID = 1L;
	
	private final String[] m_quadKeys;
	private final QuadSpacePartition[] m_partitions;
	
	public static QuadSpacePartitioner from(Iterable<String> quadKeys) {
		QuadSpacePartition[] partitions = FStream.from(quadKeys)
												.sort((v1,v2) -> v1.compareTo(v2))
												.zipWithIndex()
												.map(t -> new QuadSpacePartition(t.index(), t.value()))
												.toArray(QuadSpacePartition.class);
		Utilities.checkArgument(partitions.length > 0, "empty quadkeys");
		
		return new QuadSpacePartitioner(partitions);
	}
	
	public QuadSpacePartitioner(QuadSpacePartition[] partitions) {
		m_partitions = partitions;
		m_quadKeys = new String[partitions.length];
		for ( int i =0; i < partitions.length; ++i ) {
			m_quadKeys[i] = partitions[i].getQuadKey();
		}
	}
	
	public QuadSpacePartition[] getPartitionAll() {
		return m_partitions;
	}
	
	public QuadSpacePartition getPartition(int idx) {
		return m_partitions[idx];
	}
	
	public List<String> getQuadKeyAll() {
		return Arrays.asList(m_quadKeys);
	}

	@Override
	public int getPartition(Object key) {
		int ret = Arrays.binarySearch(m_quadKeys, key);
		return (ret >= 0) ? ret : m_quadKeys.length;
	}

	@Override
	public int numPartitions() {
		return m_quadKeys.length + 1;
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || !(obj instanceof QuadSpacePartitioner) ) {
			return false;
		}
		
		QuadSpacePartitioner other = (QuadSpacePartitioner)obj;
		return m_quadKeys.equals(other.m_quadKeys);
	}
	
	@Override
	public int hashCode() {
		return m_quadKeys.hashCode();
	}
}
