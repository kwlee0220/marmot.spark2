package marmot.spark.type;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.UserDefinedType;

import marmot.type.GridCell;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GridCellUDT extends UserDefinedType<GridCell> {
	private static final long serialVersionUID = -1L;

	public static final GridCellUDT UDT;
	private static final DataType SQL_DATA_TYPE;
	
	static {
		SQL_DATA_TYPE = DataTypes.createStructType(new StructField[] {
			DataTypes.createStructField("x", DataTypes.IntegerType, false),
			DataTypes.createStructField("y", DataTypes.IntegerType, false),
		});
		UDT = new GridCellUDT();
	}

	@Override
	public DataType sqlType() {
		return SQL_DATA_TYPE;
	}

	@Override
	public InternalRow serialize(GridCell cell) {
		InternalRow row = new GenericInternalRow(2);
		row.update(0, cell.getX());
		row.update(1, cell.getY());
		
		return row;
	}

	@Override
	public GridCell deserialize(Object datum) {
		if ( datum instanceof InternalRow ) {
			InternalRow row = (InternalRow)datum;
			return GridCell.fromXY(row.getInt(0), row.getInt(1));
		}

		throw new IllegalArgumentException("datum is not Row: " + datum);
	}

	@Override
	public Class<GridCell> userClass() {
		return GridCell.class;
	}

}
