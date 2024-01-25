package marmot.spark.optor;

import java.io.Serializable;

import marmot.spark.MarmotDataFrame;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface DataFrameFunction extends Serializable {
	public MarmotDataFrame apply(MarmotDataFrame inputFrame);
}
