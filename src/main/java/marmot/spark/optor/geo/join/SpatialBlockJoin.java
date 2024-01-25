package marmot.spark.optor.geo.join;


import org.apache.spark.api.java.JavaRDD;

import marmot.GRecordSchema;
import marmot.Record;
import marmot.RecordSchema;
import marmot.dataset.GeometryColumnInfo;
import marmot.io.geo.cluster.CacheableQuadCluster;
import marmot.io.geo.cluster.QuadClusterFile;
import marmot.optor.geo.join.ClusteredNLJoinRecordSet;
import marmot.plan.SpatialJoinOptions;
import marmot.spark.MarmotRDD;
import marmot.spark.RecordLite;
import marmot.spark.dataset.SparkDataSet;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialBlockJoin {
	public static MarmotRDD apply(MarmotRDD left, SparkDataSet right, GeometryColumnInfo gcInfo,
									SpatialJoinOptions opts) {
		GeometryColumnInfo outerGcInfo = left.assertGeometryColumnInfo();
		RecordSchema outerSchema = left.getRecordSchema();

		RecordSchema schema = ClusteredNLJoinRecordSet.calcRecordSchema(outerSchema,
																	right.getRecordSchema(), opts);
		JavaRDD<RecordLite> jrdd = left.getJavaRDD().mapPartitions(recIter -> {
			FStream<Record> outers = FStream.from(recIter)
											.map(rec -> rec.toRecord(outerSchema));
			QuadClusterFile<? extends CacheableQuadCluster> idxFile = right.loadOrBuildQuadClusterFile();
			ClusteredNLJoinRecordSet rset
				= new ClusteredNLJoinRecordSet(outerGcInfo.name(), outerSchema, outers, idxFile, opts);
			return rset.fstream().map(RecordLite::from).iterator();
		});
		
		return new MarmotRDD(left.getMarmotSpark(), new GRecordSchema(gcInfo, schema), jrdd);
	}
}
