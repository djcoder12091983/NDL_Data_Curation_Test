package org.iitkgp.ndl.data.correction.stitch.comparator;

import java.util.Comparator;

/**
 * NDL stitching comparator
 * @author Debasis
 */
public interface NDLStitchComparator {

	/**
	 * Comparison logic between object1 and object2
	 * @param o1 object1
	 * @param o2 object2
	 * @return returns same as {@link Comparator#compare(Object, Object)}
	 */
	public int comparator(String o1, String o2);
}