package marmot.spark.optor.geo;

import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.geo.GeoClientUtils;
import marmot.geo.GeoJsonReader;
import marmot.spark.type.ConcatStrUDAF;
import marmot.spark.type.ConvexHullUDAF;
import marmot.spark.type.EnvelopeUDAF;
import marmot.spark.type.EnvelopeUDT;
import marmot.spark.type.GeometryUDT;
import marmot.spark.type.PointUDT;
import marmot.spark.type.PolygonUDT;
import marmot.spark.type.UnionGeomUDAF;
import marmot.support.DataUtils;
import marmot.support.Functions;
import marmot.support.GeoFunctions;
import marmot.support.JsonParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialUDFs {
	private static final Logger s_logger = LoggerFactory.getLogger(SpatialUDFs.class);
	
	private SpatialUDFs() {
		throw new AssertionError("Should not be called: class=" + SpatialUDFs.class);
	}
	
	public static void registerUdf(UDFRegistration registry) {
		registry.register("ST_StartsWith", UDF_ST_StartsWith, DataTypes.BooleanType);
		registry.register("ST_ArrayAvgD", UDF_ST_ArrayAvgD, DataTypes.DoubleType);
		registry.register("ST_ArrayAvgF", UDF_ST_ArrayAvgF, DataTypes.DoubleType);
		
		registry.register("ST_Area", UDF_ST_Area, DataTypes.DoubleType);
		registry.register("ST_Length", UDF_ST_Length, DataTypes.DoubleType);
		registry.register("ST_Distance", UDF_ST_Distance, DataTypes.DoubleType);
		registry.register("ST_Point", UDF_ST_Point, PointUDT.UDT);
		
		registry.register("ST_Intersects", UDF_ST_Intersects, DataTypes.BooleanType);
		registry.register("ST_EnvelopeIntersects", UDF_ST_EnvelopeIntersects, DataTypes.BooleanType);
		
		registry.register("ST_Buffer", UDF_ST_Buffer, GeometryUDT.UDT);
		registry.register("ST_Centroid", UDF_ST_Centroid, PointUDT.UDT);
		registry.register("ST_InteriorPoint", UDF_ST_InteriorPoint, PointUDT.UDT);
		registry.register("ST_ConvexHull", UDF_ST_ConvexHull, PolygonUDT.UDT);
		registry.register("ST_Intersection", UDF_ST_Intersection, GeometryUDT.UDT);
		registry.register("ST_Union", UDF_ST_Union, GeometryUDT.UDT);
		
		registry.register("ST_GeomFromText", UDF_ST_GeomFromText, GeometryUDT.UDT);
		registry.register("ST_AsText", UDF_ST_GeomFromText, DataTypes.StringType);
		registry.register("ST_GeomFromWKB", UDF_ST_GeomFromWKB, GeometryUDT.UDT);
		registry.register("ST_AsBinary", UDF_ST_AsBinary, DataTypes.StringType);
		registry.register("ST_GeomFromGeoJSON", UDF_ST_GeomFromWKB, GeometryUDT.UDT);
		registry.register("ST_AsGeoJSON", UDF_ST_AsGeoJSON, DataTypes.StringType);
		registry.register("ST_AsEnvelope", UDF_ST_AsEnvelope, EnvelopeUDT.UDT);
		
		// user-defined aggregation functions
		registry.register("envelope", new EnvelopeUDAF());
		registry.register("convex_hull", new ConvexHullUDAF());
		registry.register("union_geom", new UnionGeomUDAF());
		registry.register("concat_str", new ConcatStrUDAF());
	}
	
	private static final UDF2<String,String,Boolean> UDF_ST_StartsWith
		= (str, prefix) -> (str != null) ? str.startsWith(prefix) : false;
	private static final UDF1<Double[],Double> UDF_ST_ArrayAvgD
		= (array) -> (array != null) ? Functions.avgArray(array) : null;
	private static final UDF1<Float[],Double> UDF_ST_ArrayAvgF
		= (array) -> (array != null) ? Functions.avgArray(array) : null;
	
	private static final UDF1<Geometry,Double> UDF_ST_Area = (geom) -> {
		if ( geom == null ) {
			return null;
		}
		else {
			return geom.getArea();
		}
	};
	
	private static final UDF1<Geometry,Double> UDF_ST_Length = (geom) -> {
		return (geom != null) ? geom.getLength() : -1;
	};
	
	private static final UDF2<Geometry,Geometry,Double> UDF_ST_Distance = (geom1, geom2) -> {
		if ( geom1 == null ) {
			return -1d;
		}
		if ( geom2 == null ) {
			return -1d;
		}
		return geom1.distance(geom2);
	};
	
	private static final UDF2<Geometry,Geometry,Boolean> UDF_ST_Intersects = (geom1,geom2) -> {
		if ( geom1 != null && geom2 != null ) {
			return geom1.intersects(geom2);
		}
		else {
			return false;
		}
	};
	
	private static final UDF2<Envelope,Envelope,Boolean> UDF_ST_EnvelopeIntersects = (envl1, envl2) -> {
		if ( envl1 != null && envl2 != null ) {
			return envl1.intersects(envl2);
		}
		else {
			return false;
		}
	};
	
	private static final UDF2<Geometry,Double,Geometry> UDF_ST_Buffer = (geom, dist) -> {
		if ( geom == null || geom.isEmpty() ) {
			return geom;
		}
		else {
			return geom.buffer(dist);
		}
	};
	
	private static final UDF1<Geometry,Point> UDF_ST_Centroid = (geom) -> {
		if ( geom == null ) {
			return null;
		}
		else if ( geom.isEmpty() ) {
			return GeoFunctions.ST_EmptyPoint();
		}
		else {
			return geom.getCentroid();
		}
	};
	private static final UDF1<Geometry,Point> UDF_ST_InteriorPoint = (geom) -> {
		if ( geom == null ) {
			return null;
		}
		else if ( geom.isEmpty() ) {
			return GeoFunctions.ST_EmptyPoint();
		}
		else {
			return geom.getInteriorPoint();
		}
	};
	
	private static final UDF2<Object,Object,Point> UDF_ST_Point = (xCol, yCol) -> {
		if ( xCol != null && yCol != null ) {
			try {
				double xpos = DataUtils.asDouble(xCol);
				double ypos = DataUtils.asDouble(yCol);
				return GeoClientUtils.toPoint(xpos, ypos);
			}
			catch ( Exception e ) {
				throw e;
			}
		}
		else {
			return null;
		}
	};
	
	private static final UDF1<Geometry,Polygon> UDF_ST_ConvexHull = (geom) -> {
		if ( geom != null ) {
			return (Polygon)geom.convexHull();
		}
		else {
			return null;
		}
	};
	
	private static final UDF2<Geometry,Geometry,Geometry> UDF_ST_Intersection = (geom1, geom2) -> {
		if ( geom1 == null ) {
			return null;
		}
		if ( geom2 == null ) {
			return null;
		}
		return geom1.intersection(geom2);
	};
	
	private static final UDF2<Geometry,Geometry,Geometry> UDF_ST_Union = (geom1, geom2) -> {
		if ( geom1 == null ) {
			return null;
		}
		if ( geom2 == null ) {
			return null;
		}
		return geom1.union(geom2);
	};
	
	private static final UDF1<String,Geometry> UDF_ST_GeomFromText = (wktStr) -> {
		if ( wktStr == null ) {
			return null;
		}
		else if ( wktStr instanceof String ) {
			return GeoClientUtils.fromWKT((String)wktStr);
		}
		else {
			s_logger.error("ST_GeomFromText should take string type: " + wktStr.getClass());
			throw new IllegalArgumentException();
		}
	};
	
	private static final UDF1<Geometry,String> UDF_ST_AsText = (geom) -> {
		if ( geom == null ) {
			return null;
		}
		
		return GeoClientUtils.toWKT(geom);
	};
	
	private static final UDF1<byte[],Geometry> UDF_ST_GeomFromWKB = (bytes) -> {
		if ( bytes == null ) {
			return null;
		}
		else {
			return GeoClientUtils.fromWKB(bytes);
		}
	};
	
	private static final UDF1<Geometry,byte[]> UDF_ST_AsBinary = (geom) -> {
		if ( geom == null ) {
			return null;
		}

		return GeoClientUtils.toWKB(geom);
	};
	
	private static final UDF1<String,Geometry> UDF_ST_GeomFromGeoJSON = (json) -> {
		try {
			if ( json == null ) {
				return null;
			}
			return GeoJsonReader.read(JsonParser.parse(json));
		}
		catch ( Exception e ) {
			s_logger.error(String.format("fails to parse GeoJSON: '%s'", json), e);
			return null;
		}
	};
	
	private static final UDF1<Geometry,String> UDF_ST_AsGeoJSON = (geom) -> {
		if ( geom == null ) {
			return null;
		}

		return new GeometryJSON().toString(geom);
	};
	
	private static final UDF1<Geometry,Envelope> UDF_ST_AsEnvelope = (geom) -> {
		if ( geom == null ) {
			return null;
		}
		else {
			return geom.getEnvelopeInternal();
		}
	};
}
