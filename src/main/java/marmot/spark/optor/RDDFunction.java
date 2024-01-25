package marmot.spark.optor;

import java.util.function.Function;

import marmot.spark.MarmotRDD;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RDDFunction extends Function<MarmotRDD,MarmotRDD> {
}
