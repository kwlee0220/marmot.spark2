package marmot.spark.optor.geo.join;

import java.util.Iterator;
import java.util.List;

import org.locationtech.jts.geom.Envelope;

import com.google.common.collect.Lists;

import marmot.GRecordSchema;
import marmot.geo.CoordinateTransform;
import marmot.io.geo.quadtree.Pointer;
import marmot.io.geo.quadtree.PointerPartition;
import marmot.io.geo.quadtree.QuadTree;
import marmot.optor.geo.SpatialRelation;
import marmot.optor.geo.join.SpatialLookupTable;
import marmot.spark.RecordLite;
import marmot.support.EnvelopeTaggedRecord;
import marmot.type.MapTile;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class QTreeLookupTable implements SpatialLookupTable {
	private final Envelope m_quadBounds;
	private final QuadTree<Pointer,PointerPartition> m_qtree;
	private final List<EnvelopeTaggedRecord> m_records;
	
	public QTreeLookupTable(String quadKey, GRecordSchema gschema, Iterator<RecordLite> records) {
		m_quadBounds = MapTile.fromQuadKey(quadKey).getBounds();
		int geomColIdx = gschema.getGeometryColumnIdx();
		CoordinateTransform rightTrans = CoordinateTransform.getTransformToWgs84(gschema.getSrid());

		m_qtree = new QuadTree<>(quadKey, qkey->new PointerPartition());
		m_records = Lists.newArrayList();
		while ( records.hasNext() ) {
			RecordLite rrec = records.next();
			Envelope mbr84 = rrec.getGeometry(geomColIdx).getEnvelopeInternal();
			if ( rightTrans != null ) {
				mbr84 = rightTrans.transform(mbr84);
			}
			m_records.add(new EnvelopeTaggedRecord(mbr84, rrec.toRecord(gschema.getRecordSchema())));
			m_qtree.insert(new Pointer(mbr84, m_records.size()-1));
		}
	}

	@Override
	public FStream<EnvelopeTaggedRecord> query(Envelope range84, boolean dropDuplicates) {
		FStream<EnvelopeTaggedRecord> strm =  m_qtree.query(SpatialRelation.INTERSECTS, range84)
													.distinct()
													.map(ptr -> m_records.get(ptr.index()));
		if ( dropDuplicates ) {
			strm = strm.filter(etr -> isOwnerOf(etr.getEnvelope().intersection(range84)));
		}
		return strm;
	}
	
	protected boolean isOwnerOf(Envelope envl84) {
		return m_quadBounds.contains(envl84.centre());
	}
}