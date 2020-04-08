package org.iitkgp.ndl.data.correction.stitch.comparator;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.iitkgp.ndl.data.correction.stitch.NDLStitchHierarchyNode;

/**
 * Predefined standard stitching comparator
 * @author Debasis
 */
public class StandardNDLStitchComparator {
	
	/**
	 * Numeric ASCENDING comparator
	 */
	public static final NDLStitchComparator NUMERIC_ASCENDING = new NDLStitchComparator() {
		
		@Override
		public int comparator(String o1, String o2) {
			return Long.valueOf(o1).compareTo(Long.valueOf(o2));
		}
	};
	
	/**
	 * Numeric DESCENDING comparator
	 */
	public static final NDLStitchComparator NUMERIC_DESCENDING = new NDLStitchComparator() {
		
		@Override
		public int comparator(String o1, String o2) {
			// reverse
			return Long.valueOf(o2).compareTo(Long.valueOf(o1));
		}
	};
	
	/**
	 * Text ASCENDING comparator
	 */
	public static final NDLStitchComparator TEXT_ASCENDING = new NDLStitchComparator() {
		
		@Override
		public int comparator(String o1, String o2) {
			return o1.compareTo(o2);
		}
	};
	
	/**
	 * Text DESCENDING comparator
	 */
	public static final NDLStitchComparator TEXT_DESCENDING = new NDLStitchComparator() {
		
		@Override
		public int comparator(String o1, String o2) {
			// reverse
			return o2.compareTo(o1);
		}
	};
	
	/**
	 * Custom has-parts sorting
	 * @param hasParts given has-parts
	 * @param comparator given custom comparison logic
	 */
	public static void sortHasParts(List<NDLStitchHierarchyNode> hasParts, NDLStitchComparator comparator) {
		// sort has-part
		Collections.sort(hasParts, new Comparator<NDLStitchHierarchyNode>() {
			
			@Override
			public int compare(NDLStitchHierarchyNode data1, NDLStitchHierarchyNode data2) {
				// custom ordering
				return comparator.comparator(data1.getOrder(), data2.getOrder());
			}
		});
	}
}