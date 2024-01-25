package marmot.spark.optor;

import java.io.Serializable;

import marmot.spark.MarmotDataFrame;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface BinaryDataFrameFunction extends Serializable {
	public MarmotDataFrame apply(MarmotDataFrame left, MarmotDataFrame right);
}
