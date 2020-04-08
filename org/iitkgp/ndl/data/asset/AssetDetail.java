package org.iitkgp.ndl.data.asset;

import org.iitkgp.ndl.data.NDLAssetType;

/**
 * This class is similar to {@link NDLAssetDetail}
 * @author Debasis
 */
public class AssetDetail {
	
	byte[] contents;
	NDLAssetType type;
	String name;
	
	/**
	 * Constructor
	 * @param contents asset contents
	 * @param name asset name
	 * @param type asset type
	 */
	public AssetDetail(byte[] contents, String name, NDLAssetType type) {
		this.contents = contents;
		this.name = name;
		this.type = type;
	}
	
	/**
	 * Gets contents
	 * @return gets contents
	 */
	public byte[] getContents() {
		return contents;
	}
	
	/**
	 * Gets asset type
	 * @return returns asset type
	 */
	public NDLAssetType getType() {
		return type;
	}
	
	/**
	 * Gets asset name
	 * @return returns asset name
	 */
	public String getName() {
		return name;
	}
}