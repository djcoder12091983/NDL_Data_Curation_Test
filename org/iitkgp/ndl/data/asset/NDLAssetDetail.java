package org.iitkgp.ndl.data.asset;

import java.io.File;

import org.iitkgp.ndl.data.NDLAssetType;

/**
 * NDL asset detail
 * @author Debasis
 */
public class NDLAssetDetail {

	File asset;
	NDLAssetType type;
	
	/**
	 * constructor
	 * @param asset asset file
	 * @param type asset type
	 */
	public NDLAssetDetail(File asset, NDLAssetType type) {
		this.asset = asset;
		this.type = type;
	}
	
	/**
	 * gets asset file
	 * @return returns asset file
	 */
	public File getAsset() {
		return asset;
	}
	
	/**
	 * gets asset type
	 * @return returns asset type
	 */
	public NDLAssetType getType() {
		return type;
	}
}