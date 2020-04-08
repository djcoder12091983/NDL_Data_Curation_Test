package org.iitkgp.ndl.data;

/**
 * NDL asset type
 * @author Debasis
 */
public enum NDLAssetType {
	
	/**
	 * thumbnail asset
	 */
	THUMBNAIL("THUMBNAIL"),
	
	/**
	 * license asset
	 */
	LICENSE("LICENSE"),
	
	/**
	 * test asset
	 */
	FULLTEXT("TEXT"),
	
	/**
	 * original asset
	 */
	ORIGINAL("ORIGINAL");
	
	String type; // type text
	
	// constructor
	private NDLAssetType(String type) {
		this.type = type;
	}
	
	/**
	 * Gets asset type
	 * @return returns asset type
	 */
	public String getType() {
		return type;
	}
	
	/**
	 * Gets asset type sum by name
	 * @param name
	 * @return
	 */
	public static NDLAssetType getType(String name) {
		int p = name.indexOf('.');
		if(p != -1) {
			String onlyname = name.substring(0, p);
			try {
				return NDLAssetType.valueOf(onlyname);
			} catch(IllegalArgumentException ex) {
				// error
				// probably ORIGINAL
				return NDLAssetType.ORIGINAL;
			}
		} else {
			// probably ORIGINAL
			return NDLAssetType.ORIGINAL;
		}
	}
}