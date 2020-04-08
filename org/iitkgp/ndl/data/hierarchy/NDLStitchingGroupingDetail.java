package org.iitkgp.ndl.data.hierarchy;

/**
 * NDL stitching grouping detail
 * <pre>'groupingKey' must be defined but 'groupingHandleSuffix' is optional</pre>
 * <pre>if 'groupingHandleSuffix' not defined then it defined as first/second/third
 * with initially found item id</pre>
 * @author Debasis
 */
public class NDLStitchingGroupingDetail {
	
	String groupingKey;
	String groupingHandleSuffix;

	/**
	 * Constructor
	 * @param groupingKey grouping key
	 */
	public NDLStitchingGroupingDetail(String groupingKey) {
		this.groupingKey = groupingKey;
	}
	
	/**
	 * Constructor
	 * @param groupingKey grouping key
	 * @param groupingHandleSuffix grouping handle suffix id
	 */
	public NDLStitchingGroupingDetail(String groupingKey, String groupingHandleSuffix) {
		this.groupingKey = groupingKey;
	}
	
	/**
	 * Gets grouping key
	 * @return grouping key
	 */
	public String getGroupingKey() {
		return groupingKey;
	}
	
	/**
	 * Gets grouping handle suffix ID
	 * @return returns grouping handle suffix ID
	 */
	public String getGroupingHandleSuffix() {
		return groupingHandleSuffix;
	}
}