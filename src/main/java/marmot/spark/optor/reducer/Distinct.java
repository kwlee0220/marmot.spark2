package marmot.spark.optor.reducer;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import marmot.RecordSchema;
import marmot.plan.Group;
import marmot.spark.MarmotRDD;
import marmot.spark.RecordLite;
import marmot.spark.optor.RDDFunction;
import scala.Tuple2;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Distinct implements RDDFunction, Serializable {
	private static final long serialVersionUID = 1L;
	
	private final Group m_group;
	
	public Distinct(Group group) {
		m_group = group;
	}

	@Override
	public MarmotRDD apply(MarmotRDD input) {
		RecordSchema inputSchema = input.getRecordSchema();
		
		int[] keyIdxes = GroupUtils.getGroupColumIndexes(m_group, inputSchema);
		JavaPairRDD<RecordLite,RecordLite> pairs
								= input.getJavaRDD()
										.mapToPair(rec -> new Tuple2<>(rec.project(keyIdxes), rec));
		JavaRDD<RecordLite> output = m_group.workerCount()
											.map(c -> pairs.reduceByKey(this::reduce, c))
											.getOrElse(() -> pairs.reduceByKey(this::reduce))
											.map(Tuple2::_2);
		return new MarmotRDD(input.getMarmotSpark(), input.getGRecordSchema(), output);
	}
	
	private RecordLite reduce(RecordLite rec1, RecordLite rec2) {
		return rec1;
	}
}
