package marmot.spark.geo.spindex;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;

import marmot.io.geo.index.SpatialIndexedFile;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.spark.dataset.SequenceFileDataSet;
import marmot.spark.geo.cluster.QuadSpacePartition;
import marmot.spark.geo.cluster.QuadSpacePartitioner;
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
public class SpatialIndexedFileRDD extends RDD<RecordLite> {
	private static final long serialVersionUID = 1L;
	
	static final ClassTag<RecordLite> CLASS_TAG
									= ClassManifestFactory$.MODULE$.fromClass(RecordLite.class);
	private static final scala.collection.Iterator<RecordLite> EMPTY_ITER
		= JavaConverters.asScalaIteratorConverter(Collections.<RecordLite>emptyIterator()).asScala();

	private final SpatialIndexedFile m_idxFile;
	private final QuadSpacePartitioner m_partitioner;
	
	public SpatialIndexedFileRDD(MarmotSpark marmot, SequenceFileDataSet ds) throws IOException {
		super(marmot.getJavaSparkContext().sc(), new ArrayBuffer<Dependency<?>>(), CLASS_TAG);

		m_idxFile = ds.getSpatialIndexFile();
		m_partitioner = QuadSpacePartitioner.from(m_idxFile.getClusterKeyAll());
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

	private Iterator<RecordLite> loadPartition(int index) throws IOException {
		QuadSpacePartition part = m_partitioner.getPartition(index);
		return m_idxFile.getCluster(part.getQuadKey())
						.read(false)
						.map(etr -> RecordLite.of(etr.getRecord().getAll()))
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
