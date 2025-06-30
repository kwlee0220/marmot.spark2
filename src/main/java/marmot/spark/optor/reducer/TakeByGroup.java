package marmot.spark.optor.reducer;

import java.io.Serializable;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.spark.api.java.JavaRDD;

import com.google.common.collect.Lists;

import marmot.RecordSchema;
import marmot.io.MultiColumnKey;
import marmot.plan.Group;
import marmot.spark.MarmotRDD;
import marmot.spark.RecordLite;
import marmot.spark.optor.RDDFunction;
import scala.Tuple2;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TakeByGroup implements RDDFunction, Serializable {
	private static final long serialVersionUID = 1L;
	
	private final Group m_group;
	private final int m_takeCount;
	
	private @Nullable KeyValueComparator m_orderCmptor = null;
	
	public TakeByGroup(Group group, int takeCount) {
		m_group = group;
		m_takeCount = takeCount;
	}

	@Override
	public MarmotRDD apply(MarmotRDD input) {
		RecordSchema inputSchema = input.getRecordSchema();
		
		m_group.orderBy().ifPresent(cols -> {
			m_orderCmptor = new KeyValueComparator(MultiColumnKey.fromString(cols), inputSchema);
		});
		
		int[] keyIdxes = GroupUtils.getGroupColumIndexes(m_group, inputSchema);
		JavaRDD<RecordLite> output
						= input.getJavaRDD()
								.mapToPair(rec -> {
									RecordLite keys = rec.project(keyIdxes);
									return new Tuple2<>(new GroupRecordLite(keys), rec);
								})
								.aggregateByKey(Lists.newArrayList(), this::sequence, this::combine)
								.flatMap(t -> t._2.iterator());
		return new MarmotRDD(input.getMarmotSpark(), input.getGRecordSchema(), output);
	}
	
	private List<RecordLite> sequence(List<RecordLite> accum, RecordLite rec) {
		if ( m_orderCmptor != null ) {
			accum.add(rec);
			if ( accum.size() > m_takeCount ) {
				accum.sort(m_orderCmptor);
				accum = Lists.newArrayList(accum.subList(0, m_takeCount));
			}
		}
		else if ( accum.size() < m_takeCount ) {
			accum.add(rec);
		}
		
		return accum;
	}
	
	private List<RecordLite> combine(List<RecordLite> accum, List<RecordLite> accum2) {
		if ( m_orderCmptor != null ) {
			accum.addAll(accum2);
			if ( accum.size() > m_takeCount ) {
				accum.sort(m_orderCmptor);
				return Lists.newArrayList(accum.subList(0, m_takeCount));
			}
			else {
				return accum;
			}
		}
		else {
			int remains = m_takeCount - accum.size();
			accum.addAll(accum2.subList(0, remains));
			return accum;
		}
	}
}
