package org.iitkgp.ndl.data;

/**
 * AIP file group detail
 * @author Debasis
 */
public class AIPFileGroup {

	String name; // file name
	String checksum; // message digest
	String checksumType; // message digest type
	String mime;
	long size;
	
	/**
	 * Constructor
	 * @param name file name
	 * @param mime MIME type
	 * @param size size
	 */
	public AIPFileGroup(String name, String mime, long size) {
		this.name = name;
		this.mime = mime;
		this.size = size;
	}
	
	/**
	 * Sets message digest detail
	 * @param checksum message digest detail
	 */
	public void setChecksum(String checksum) {
		this.checksum = checksum;
	}
	
	/**
	 * Sets message digest type
	 * @param checksumType message digest type
	 */
	public void setChecksumType(String checksumType) {
		this.checksumType = checksumType;
	}
	
	/**
	 * Gets message digest detail
	 * @return returns message digest detail
	 */
	public String getChecksum() {
		return checksum;
	}
	
	/**
	 * Gets size in bytes
	 * @return returns size in bytes
	 */
	public long getSize() {
		return size;
	}
	
	/**
	 * Gets message digest type
	 * @return returns message digest type
	 */
	public String getChecksumType() {
		return checksumType;
	}
	
	/**
	 * Gets file MIME
	 * @return returns mime
	 */
	public String getMime() {
		return mime;
	}
	
	/**
	 * Gets file name
	 * @return file name
	 */
	public String getName() {
		return name;
	}
}