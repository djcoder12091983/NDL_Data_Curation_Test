package org.iitkgp.ndl.data.compress;

/**
 * Compression mode detail, example: tar.gz, 7zip, zip etc.
 * @author Debasis
 *
 */
public enum CompressedFileMode {
	
	/**
	 * TAR GZ
	 */
	TARGZ(".tar.gz");
	
	String mode;
	
	/**
	 * Constructor
	 * @param mode compression mode
	 */
	private CompressedFileMode(String mode) {
		this.mode = mode;
	}
	
	/**
	 * Gets compression mode
	 * @return returns compression mode
	 */
	public String getMode() {
		return mode;
	}
}