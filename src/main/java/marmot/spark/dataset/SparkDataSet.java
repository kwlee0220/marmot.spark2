package marmot.spark.dataset;

import marmot.dataset.DataSetX;
import marmot.dataset.IndexNotFoundException;
import marmot.io.geo.cluster.CacheableQuadCluster;
import marmot.io.geo.cluster.QuadClusterFile;
import marmot.spark.MarmotRDD;
import marmot.spark.geo.cluster.QuadSpacePartitioner;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface SparkDataSet extends DataSetX<MarmotRDD> {
//	/**
//	 * {@link MarmotSpark}를 반환한다.
//	 * 
//	 * @return	MarmotSpark 객체.
//	 */
//	public MarmotSpark getMarmotSpark();
//	
//	public SparkDataSet cluster(String outDsId, ClusterSpatiallyOptions opts);
	
	public QuadSpacePartitioner getSpatialPartitioner();
	
	public QuadClusterFile<? extends CacheableQuadCluster>
	loadOrBuildQuadClusterFile() throws IndexNotFoundException;
}
