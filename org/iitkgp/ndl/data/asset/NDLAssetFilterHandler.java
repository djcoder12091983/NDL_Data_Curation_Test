package org.iitkgp.ndl.data.asset;

import org.iitkgp.ndl.data.Filter;

/**
 * NDL asset filter processing
 * Note: It's can't be shared across multiple curations at the same time
 * @author Debasis
 */
public class NDLAssetFilterHandler {
	// single tone class
	
	Filter<NDLAssetDetail> filter;
	private static NDLAssetFilterHandler self;
	
	private NDLAssetFilterHandler() {
		// protecting constructor
	}
	
	/**
	 * Initializes container
	 * @param filter given filter
	 */
	public static synchronized void init(Filter<NDLAssetDetail> filter) {
		self = new NDLAssetFilterHandler();
		self.filter = filter;
	}
	
	/**
	 * Checks whether container initialized
	 * @return returns true if so otherwise false
	 */
	public static boolean isIntialized() {
		return self != null;
	}
	
	/**
	 * Runs filter logic against given asset detail
	 * @param data given asset detail
	 * @return returns true if filter passes otherwise false
	 */
	public static boolean filter(NDLAssetDetail data) {
		if(isIntialized()) {
			// runs filter
			return self.filter.filter(data);
		} else {
			// container not initialized yet
			throw new IllegalStateException("Container not intialized");
		}
	}
	
	/**
	 * Container destruction
	 */
	public static synchronized void destroy() {
		self = null; // destroy container
	}
}