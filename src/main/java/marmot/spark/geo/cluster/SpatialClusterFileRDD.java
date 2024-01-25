package marmot.spark.geo.cluster;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.locationtech.jts.geom.Envelope;

import marmot.io.HdfsPath;
import marmot.io.geo.cluster.SpatialCluster;
import marmot.io.geo.cluster.SpatialClusterFile;
import marmot.io.geo.cluster.SpatialClusterInfo;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.spark.dataset.SpatialClusterDataSet;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;
import utils.Throwables;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialClusterFileRDD extends RDD<RecordLite> implements QuadKeyPartitioned {
	private static final long serialVersionUID = 1L;
	
	static final ClassTag<RecordLite> CLASS_TAG
									= ClassManifestFactory$.MODULE$.fromClass(RecordLite.class);
	private static final scala.collection.Iterator<RecordLite> EMPTY_ITER
		= JavaConverters.asScalaIteratorConverter(Collections.<RecordLite>emptyIterator()).asScala();

	private final SpatialClusterFile m_scFile;
	private final QuadSpacePartitioner m_partitioner;
	
	public SpatialClusterFileRDD(MarmotSpark marmot, SpatialClusterDataSet ds) {
		super(marmot.getJavaSparkContext().sc(), new ArrayBuffer<Dependency<?>>(), CLASS_TAG);

		m_scFile = ds.getSpatialClusterFile();
		m_partitioner = QuadSpacePartitioner.from(m_scFile.getClusterKeyAll());
	}

	@Override
	public scala.collection.Iterator<RecordLite> compute(Partition part, TaskContext ctxt) {
		QuadSpacePartition cluster = (QuadSpacePartition)part;
		try {
			Iterator<RecordLite> recIter = loadPartition(cluster.index());
			return JavaConverters.asScalaIteratorConverter(recIter).asScala();
		}
		catch ( IOException e ) {
			Throwables.sneakyThrow(e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public Set<Integer> matchPartitions(Envelope range84) {
		return m_scFile.queryClusterKeys(range84)
						.map(qk -> m_partitioner.getPartition(qk))
						.toSet();
	}

	private Iterator<RecordLite> loadPartition(int index) throws IOException {
		QuadSpacePartition part = m_partitioner.getPartition(index);
		
		SpatialClusterInfo scInfo = m_scFile.getClusterInfo(part.getQuadKey());
		HdfsPath path = m_scFile.getPath().child(scInfo.partitionId());
		return SpatialCluster.readNonDuplicate(path, scInfo, m_scFile.getRecordSchema())
							.map(RecordLite::from)
							.iterator();
	}

	@Override
	public Partition[] getPartitions() {
		return m_partitioner.getPartitionAll();
	}
	
	@Override
	public Option<Partitioner> partitioner() {
		return Option.apply(m_partitioner);
	}
}
