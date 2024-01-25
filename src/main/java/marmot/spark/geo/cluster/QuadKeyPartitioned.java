package marmot.spark.geo.cluster;

import java.util.Set;

import org.locationtech.jts.geom.Envelope;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface QuadKeyPartitioned {
	Set<Integer> matchPartitions(Envelope range84);
}
