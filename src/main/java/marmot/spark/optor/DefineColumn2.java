package marmot.spark.optor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Column;
import marmot.GRecordSchema;
import marmot.RecordSchema;
import marmot.spark.MarmotSpark;
import marmot.spark.Rows;
import utils.Utilities;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DefineColumn2 extends AbstractDataFrameFunction {
	private static final long serialVersionUID = 1L;
	static final Logger s_logger = LoggerFactory.getLogger(DefineColumn2.class);

	private final String m_colDecl;
	private final FOption<String> m_columnInitScript;
	
	private Column m_definedCol;	// set during initialization
	
	public DefineColumn2(String colDecl) {
		this(colDecl, null);
	}
	
	public DefineColumn2(String colDecl, String initExpr) {
		Utilities.checkNotNullArgument(colDecl, "column declaration");
		
		m_colDecl = colDecl;
		m_columnInitScript = FOption.ofNullable(initExpr);
	}

	@Override
	protected GRecordSchema _initialize(MarmotSpark marmot, GRecordSchema inputGSchema) {
		RecordSchema schema = RecordSchema.parse(m_colDecl);
		if ( schema.getColumnCount() > 1 ) {
			throw new IllegalArgumentException("too many columns are defined: decl=" + m_colDecl);
		}
		else if ( schema.getColumnCount() == 0 ) {
			throw new IllegalArgumentException("no column is defined");
		}
		m_definedCol = schema.getColumnAt(0);
		
		RecordSchema outSchema = inputGSchema.getRecordSchema()
											.toBuilder()
											.addOrReplaceColumn(m_definedCol.name(), m_definedCol.type())
											.build();
		return inputGSchema.derive(outSchema);
	}

	@Override
	protected Dataset<Row> mapDataFrame(Dataset<Row> df) {
		DataType colType = Rows.toSparkType(m_definedCol.type());
		org.apache.spark.sql.Column init = m_columnInitScript.map(functions::expr)
															.getOrElse(functions.lit(null))
															.cast(colType);
		return df.withColumn(m_definedCol.name(), init);
	}
	
	@Override
	public String toString() {
		String initStr = m_columnInitScript.map(init -> String.format(" = %s", init))
											.getOrElse("");
		return String.format("%s: col_decl=%s, init=%s", getClass().getSimpleName(),
								m_colDecl, initStr);
	}
}
