package marmot.spark;

import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import utils.stream.FStream;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.spark.type.EnvelopeUDT;
import marmot.spark.type.FloatArrayUDT;
import marmot.spark.type.GeometryCollectionUDT;
import marmot.spark.type.GeometryUDT;
import marmot.spark.type.GridCellUDT;
import marmot.spark.type.LineStringUDT;
import marmot.spark.type.MultiLineStringUDT;
import marmot.spark.type.MultiPolygonUDT;
import marmot.spark.type.PointUDT;
import marmot.spark.type.PolygonUDT;
import marmot.support.DefaultRecord;

import scala.collection.JavaConverters;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Rows {
	private Rows() {
		throw new AssertionError("should not be called: " + getClass());
	}
	
	public static List<Object> getValueList(Row row) {
		return JavaConverters.seqAsJavaListConverter(row.toSeq()).asJava();
	}
	
	public static Row toRow(RecordLite raw) {
		return toRow(raw.values());
	}
	
	public static Row toRow(Object[] values) {
		return RowFactory.create(values);
	}
	
	public static RecordSchema toRecordSchema(Dataset<Row> rows) {
		return toRecordSchema(rows.exprEnc().schema());
	}
	
	public static RecordSchema toRecordSchema(StructType struct) {
		return FStream.of(struct.fields())
						.map(Rows::toColumn)
						.fold(RecordSchema.builder(), (b,c) -> b.addColumn(c))
						.build();
	}
	
	public static RecordLite toRecordLite(Row row) {
		List<Object> values = getValueList(row);
		return RecordLite.ofLength(values.size()).setAll(values);
	}
	
	public static void toRecord(Row row, Record output) {
		output.setAll(getValueList(row));
	}
	
	public static Record toRecord(Row row, RecordSchema schema) {
		Record output = DefaultRecord.of(schema);
		toRecord(row, output);
		return output;
	}
	
	public static Column toColumn(StructType schema, String fieldName) {
		StructField[] fields = schema.fields();
		return toColumn(fields[schema.fieldIndex(fieldName)]);
	}
	
	private static Column toColumn(StructField field) {
		String name = field.name();
		DataType type = field.dataType();
		
		switch ( type.typeName() ) {
			case "string":
				return new Column(name, marmot.type.DataType.STRING);
			case "double":
				return new Column(name, marmot.type.DataType.DOUBLE);
			case "integer":
				return new Column(name, marmot.type.DataType.INT);
			case "long":
				return new Column(name, marmot.type.DataType.LONG);
			case "float":
				return new Column(name, marmot.type.DataType.FLOAT);
			case "short":
				return new Column(name, marmot.type.DataType.SHORT);
			case "byte":
				return new Column(name, marmot.type.DataType.BYTE);
			default:
				throw new AssertionError("type: " + type.typeName());
		}
	}
	
	public static StructType toSparkStructType(RecordSchema schema) {
		List<StructField> fields = schema.streamColumns()
										.map(Rows::toStructField)
										.toList();
		return DataTypes.createStructType(fields);
	}
	
	private static StructField toStructField(Column col) {
		return new StructField(col.name(), toSparkType(col.type()), true, Metadata.empty());
	}
	
//	public static Dataset<Row> toDataFrame(SQLContext ctx, RecordSchema schema, JavaRDD<RecordLite> jrdd) {
//		StructType rowType = Rows.toSparkStructType(schema);
//		JavaRDD<Row> rows = jrdd.map(rec -> Rows.toRow(rec));
//		return ctx.createDataFrame(rows, rowType);
//	}
	
	public static Dataset<Row> toDataFrame(SQLContext ctx, RecordSchema schema, JavaRDD<Row> rows) {
		StructType rowType = Rows.toSparkStructType(schema);
		return ctx.createDataFrame(rows, rowType);
	}
	
	public static JavaRDD<RecordLite> toJavaRDD(Dataset<Row> df) {
		return df.javaRDD().map(Rows::toRecordLite);
	}
	
	public static RecordSet toRecordSet(RecordSchema schema, Iterator<RecordLite> recIter) {
		return toRecordSet(schema, FStream.from(recIter));
	}
	
	public static RecordSet toRecordSet(RecordSchema schema, FStream<RecordLite> recStrm) {
		return RecordSet.from(schema, recStrm.map(rec -> rec.toRecord(schema)));
	}
	
	public static DataType toSparkType(marmot.type.DataType dtype) {
		switch ( dtype.getTypeCode() ) {
			case STRING:
				return DataTypes.StringType;
			case DOUBLE:
				return DataTypes.DoubleType;
			case INT:
				return DataTypes.IntegerType;
			case LONG:
				return DataTypes.LongType;
			case FLOAT:
				return DataTypes.FloatType;
			case BYTE:
				return DataTypes.ByteType;
			case SHORT:
				return DataTypes.ShortType;
			case ENVELOPE:
				return EnvelopeUDT.UDT;
			case POINT:
				return PointUDT.UDT;
			case POLYGON:
				return PolygonUDT.UDT;
			case GEOMETRY:
				return GeometryUDT.UDT;
			case MULTI_POLYGON:
				return MultiPolygonUDT.UDT;
			case LINESTRING:
				return LineStringUDT.UDT;
			case MULTI_LINESTRING:
				return MultiLineStringUDT.UDT;
			case MULTI_POINT:
				return MultiPolygonUDT.UDT;
			case GEOM_COLLECTION:
				return GeometryCollectionUDT.UDT;
			case DATETIME:
				return DataTypes.TimestampType;
			case DATE:
				return DataTypes.DateType;
			case GRID_CELL:
				return GridCellUDT.UDT;
			case FLOAT_ARRAY:
				return FloatArrayUDT.UDT;
			default:
				throw new AssertionError("unsupported row typeCode: " + dtype.getTypeCode());
		}
	}
}
