package marmot.spark.optor.reducer;

import java.util.List;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import com.google.common.collect.Lists;

import marmot.Column;
import marmot.RecordSchema;
import marmot.optor.AggregateType;
import marmot.optor.support.SafeUnion;
import marmot.spark.RecordLite;
import marmot.support.GeoUtils;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class UnionGeom implements ValueAggregator<UnionGeom> {
	private static final long serialVersionUID = 1L;
	private static final String DEFAULT_OUT_COLUMN = "the_geom";
	private static final int BATCH_SIZE = 128;
	
	private final String m_colName;
	private String m_outColName;
	
	// set when initialized
	private Column m_inputCol;
	private DataType m_outputType;
	private int m_outputColIdx = -1;
	
	public UnionGeom(String colName, String outColName) {
		m_colName = colName;
		m_outColName = outColName;
	}
	
	public UnionGeom(String colName) {
		this(colName, DEFAULT_OUT_COLUMN);
	}

	@Override
	public AggregateType getAggregateType() {
		return AggregateType.UNION_GEOM;
	}
	
	@Override
	public String getInputColumnName() {
		return m_colName;
	}

	@Override
	public Column getOutputColumn() {
		return new Column(m_outColName, m_outputType);
	}

	@Override
	public UnionGeom as(String outColName) {
		m_outColName = outColName;
		return this;
	}
	
	@Override
	public DataType getOutputType(DataType inputType) {
		return computeOutputType(m_inputCol.type());
	}

	@Override
	public void initialize(int pos, RecordSchema inputSchema) {
		m_inputCol = inputSchema.getColumn(m_colName);
		m_outputType = computeOutputType(m_inputCol.type());
		m_outputColIdx = pos;
	}

	@Override
	public Object getZeroValue() {
		return m_outputType.newInstance();
	}

	@Override
	public void collect(RecordLite accum, RecordLite input) {
		GeometryCollection coll = (GeometryCollection)accum.get(m_outputColIdx);
		Geometry geom = (Geometry)input.get(m_inputCol.ordinal());
		
		List<Geometry> comps = GeoUtils.streamSubComponents(coll).toList();
		comps.add(geom);
		if ( comps.size() >= BATCH_SIZE ) {
			comps = Lists.newArrayList(aggregate(comps));
		}
		
		accum.set(m_outputColIdx, GeoUtils.toGeometryCollection(comps));
	}

	@Override
	public void combine(RecordLite accum1, RecordLite accum2) {
		GeometryCollection coll1 = (GeometryCollection)accum1.get(m_outputColIdx);
		GeometryCollection coll2 = (GeometryCollection)accum2.get(m_outputColIdx);

		List<Geometry> comps1 = GeoUtils.streamSubComponents(coll1).toList();
		List<Geometry> comps2 = GeoUtils.streamSubComponents(coll2).toList();
		comps1.addAll(comps2);
		if ( comps1.size() >= BATCH_SIZE ) {
			comps1 = Lists.newArrayList(aggregate(comps1));
		}
		
		accum1.set(m_outputColIdx, GeoUtils.toGeometryCollection(comps1));
	}

	@Override
	public void toFinalValue(RecordLite accum) {
		GeometryCollection coll = (GeometryCollection)accum.getGeometry(m_outputColIdx);
		
		List<Geometry> comps = GeoUtils.streamSubComponents(coll).toList();
		Geometry result = aggregate(comps);
		
		Geometry outGeom;
		switch ( m_outputType.getTypeCode() ) {
			case MULTI_POLYGON:
				List<Polygon> polys = GeoUtils.flatten(result, Polygon.class);
				outGeom = GeoUtils.toMultiPolygon(polys);
				break;
			case MULTI_POINT:
				List<Point> pts = GeoUtils.flatten(result, Point.class);
				outGeom = GeoUtils.toMultiPoint(pts);
				break;
			case MULTI_LINESTRING:
				List<LineString> lines = GeoUtils.flatten(result, LineString.class);
				outGeom = GeoUtils.toMultiLineString(lines);
				break;
			case GEOM_COLLECTION:
				outGeom = GeoUtils.toGeometryCollection(comps);
				break;
			default:
				throw new AssertionError();
		}
		
		accum.set(m_outputColIdx, outGeom);
	}
	
	private DataType computeOutputType(DataType inputDataType) {
		switch ( inputDataType.getTypeCode() ) {
			case MULTI_POLYGON:
			case POLYGON:
				return DataType.MULTI_POLYGON;
			case LINESTRING: case MULTI_LINESTRING:
				return DataType.MULTI_LINESTRING;
			case POINT: case MULTI_POINT:
				return DataType.MULTI_POINT;
			case GEOM_COLLECTION:
				return DataType.GEOM_COLLECTION;
			default:
				throw new AssertionError("unexpected DataType: " + inputDataType);
		}
	}
	
	private Geometry aggregate(List<Geometry> comps) {
		if ( comps.size() == 1 ) {
			return comps.get(0);
		}
		
		Geometries resultType = getUnionType(Geometries.get(comps.get(0)));
		SafeUnion union = new SafeUnion(resultType);
		
		return union.apply(comps);
	}
	
	private Geometries getUnionType(Geometries srcType) {
		switch (srcType ) {
			case POLYGON:
			case MULTIPOLYGON:
				return Geometries.MULTIPOLYGON;
			case POINT:
			case MULTIPOINT:
				return Geometries.MULTIPOINT;
			case LINESTRING:
			case MULTILINESTRING:
				return Geometries.MULTILINESTRING;
			default:
				return Geometries.GEOMETRYCOLLECTION;
		}
	}
}
