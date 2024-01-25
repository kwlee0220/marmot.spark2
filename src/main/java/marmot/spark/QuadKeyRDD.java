package marmot.spark;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;

import com.google.common.collect.Maps;

import marmot.GRecordSchema;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.DataSetType;
import marmot.dataset.GeometryColumnInfo;
import marmot.io.HdfsPath;
import marmot.io.MarmotFileWriteOptions;
import marmot.io.MarmotSequenceFile;
import marmot.io.geo.cluster.SpatialClusterFile;
import marmot.io.geo.cluster.SpatialClusterInfo;
import marmot.optor.CreateDataSetOptions;
import marmot.spark.geo.cluster.PreGroupedKeyedRecordSetFactory;
import marmot.spark.geo.cluster.QuadSpacePartitioner;
import marmot.spark.optor.UpdateDataSetInfo;
import scala.Tuple2;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class QuadKeyRDD implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final MarmotSpark m_marmot;
	private final GRecordSchema m_gschema;
	private final List<String> m_quadKeys;
	private final JavaPairRDD<String,RecordLite> m_jrdd;
	
	public QuadKeyRDD(MarmotSpark marmot, GRecordSchema geomSchema, List<String> quadKeys,
						JavaPairRDD<String,RecordLite> jrdd) {
		m_marmot = marmot;
		m_gschema = geomSchema;
		m_quadKeys = quadKeys;
		m_jrdd = jrdd;
	}
	
	public MarmotSpark getMarmotSpark() {
		return m_marmot;
	}
	
	public GRecordSchema getGRecordSchema() {
		return m_gschema;
	}
	
	public RecordSchema getRecordSchema() {
		return m_gschema.getRecordSchema();
	}
	
	public FOption<GeometryColumnInfo> getGeometryColumnInfo() {
		return m_gschema.getGeometryColumnInfo();
	}
	public GeometryColumnInfo assertGeometryColumnInfo() {
		return m_gschema.assertGeometryColumnInfo();
	}
	
	public JavaPairRDD<String,RecordLite> getRDD() {
		return m_jrdd;
	}
	
	public FOption<QuadSpacePartitioner> getPartitioner() {
		Partitioner part = m_jrdd.partitioner().orNull();
		if ( part != null ) {
			return (part instanceof QuadSpacePartitioner)
					? FOption.of((QuadSpacePartitioner)part) : FOption.empty();
		}
		else {
			return FOption.empty();
		}
	}

	public void store(String dsId, FOption<Integer> partCount, MarmotFileWriteOptions opts) {
		JavaPairRDD<String,RecordLite> taggeds = m_jrdd;
		QuadSpacePartitioner partitioner = getPartitioner().getOrNull();
		if ( partitioner == null ) {
			partitioner = QuadSpacePartitioner.from(m_quadKeys);
			taggeds = m_jrdd.partitionBy(partitioner);
		}
		
		HdfsPath path = HdfsPath.of(m_marmot.getHadoopFileSystem(),
									m_marmot.getCatalog().generateFilePath(dsId));
		
		if ( opts.force() ) {
			path.delete();
			m_marmot.getCatalog().deleteDataSetInfo(dsId);
		}
		
		RecordSchema schema = m_gschema.getRecordSchema();
		GeometryColumnInfo gcInfo = m_gschema.assertGeometryColumnInfo();
		DataSetType type = DataSetType.SPATIAL_CLUSTER;
		
		//
		int packCount = partCount.getOrElse(m_marmot.getDefaultPartitionCount());
		long blockSize = opts.blockSize().getOrElse(m_marmot.getDefaultBlockSize(type));
		List<SpatialClusterInfo> scInfos
			= taggeds.coalesce(packCount)
					.mapPartitionsWithIndex((idx,iter) -> storePack(path, idx, iter, blockSize), false)
					.collect();
		
		//
		// 'cluster.idx' 파일 생성
		//
		HdfsPath idxFilePath = path.child(SpatialClusterFile.CLUSTER_INDEX_FILE);
		Map<String,String> meta = Maps.newHashMap();
		meta.put(SpatialClusterFile.PROP_DATASET_SCHEMA, schema.toString());
		meta.put(SpatialClusterFile.PROP_GEOM_COL, gcInfo.name());
		meta.put(SpatialClusterFile.PROP_SRID, gcInfo.srid());
		MarmotFileWriteOptions writeOpts = MarmotFileWriteOptions.META_DATA(meta);
		RecordSet infoRSet = RecordSet.from(SpatialClusterInfo.SCHEMA,
											FStream.from(scInfos)
													.map(SpatialClusterInfo::toRecord));
		MarmotSequenceFile.store(idxFilePath, infoRSet, null, writeOpts).call();

		CreateDataSetOptions createOpts = CreateDataSetOptions.GEOMETRY(gcInfo)
																.type(type)
																.blockSize(opts.blockSize());
		m_marmot.createDataSet(dsId, schema, createOpts);
		
		//
		// catalog update
		//
		UpdateDataSetInfo.update(m_marmot, dsId, FStream.from(scInfos)
														.map(SpatialClusterInfo::toDataSetPartitionInfo));
	}
	
	private Iterator<SpatialClusterInfo> storePack(HdfsPath clusterPath, int partIdx,
											Iterator<Tuple2<String,RecordLite>> iter,
											long blockSize) {
		String partId = String.format("%05d", partIdx);
		HdfsPath partPath = clusterPath.child(partId);
		RecordSchema schema = m_gschema.getRecordSchema();
		GeometryColumnInfo gcInfo = m_gschema.assertGeometryColumnInfo();
		
		@SuppressWarnings("resource")
		PreGroupedKeyedRecordSetFactory<String> fact = new PreGroupedKeyedRecordSetFactory<>(iter);
		return fact.map((quadKey,group) -> {
			RecordSet rset = RecordSet.from(schema, group.map(rec -> rec.toRecord(schema)));
			return SpatialClusterFile.storeCluster(partPath, quadKey, gcInfo, rset, blockSize);
		}).iterator();
	}
}
