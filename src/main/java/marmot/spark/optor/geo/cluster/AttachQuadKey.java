package marmot.spark.optor.geo.cluster;

import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Envelope;

import com.google.common.collect.Lists;

import marmot.GRecordSchema;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.geo.cluster.AttachQuadKeyRSet;
import marmot.spark.MarmotSpark;
import marmot.spark.RecordLite;
import marmot.spark.optor.AbstractRDDFunction;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AttachQuadKey extends AbstractRDDFunction {
	private static final long serialVersionUID = 1L;
	
	public static final String COL_QUAD_KEY = AttachQuadKeyRSet.COL_QUAD_KEY;
	public static final String COL_MBR = AttachQuadKeyRSet.COL_MBR;
	
	private final List<String> m_qkeys;
	private final @Nullable Envelope m_validRange;
	private final boolean m_bindOutlier;
	private final boolean m_bindOnlyToOwner;	// 주어진 공간정보가 포함되는 모든 quad-key를 포함시킬지
												// 아니면 owner quad-key만 포함시킬 지 결정 
	
	public AttachQuadKey(Iterable<String> qkeys, @Nullable Envelope validRange,
						boolean bindOutlier, boolean bindOnlyToOwner) {
		super(true);
		
		m_qkeys = Lists.newArrayList(qkeys);
		m_validRange = validRange;
		m_bindOutlier = bindOutlier;
		m_bindOnlyToOwner = bindOnlyToOwner;
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inputGSchema) {
		RecordSchema outSchema = AttachQuadKeyRSet.calcOutputRecordSchema(inputGSchema.getRecordSchema());
		return new GRecordSchema(inputGSchema.assertGeometryColumnInfo(), outSchema);
	}

	@Override
	protected Iterator<RecordLite> mapPartition(Iterator<RecordLite> iter) {
		checkInitialized();
		
		RecordSet rset = toRecordSet(iter);
		GeometryColumnInfo gcInfo = getInputGRecordSchema().assertGeometryColumnInfo();
		
		@SuppressWarnings("resource")
		AttachQuadKeyRSet outRSet = new AttachQuadKeyRSet(rset, gcInfo, m_qkeys, m_validRange,
															m_bindOutlier, m_bindOnlyToOwner);
		return outRSet.fstream()
						.map(RecordLite::from)
						.iterator();
	}
	
	@Override
	public String toString() {
		String qkeysStr = FStream.from(m_qkeys).join(',');
		if ( qkeysStr.length() > 40 ) {
			qkeysStr = qkeysStr.substring(0, 40) + "...";
		}
		return String.format("%s: geom=%s, quadkeys=%s", getClass().getSimpleName(),
								m_inputGSchema.assertGeometryColumnInfo(), qkeysStr);
	}
}
