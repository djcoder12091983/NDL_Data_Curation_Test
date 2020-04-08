package org.iitkgp.ndl.data.correction.stitch.context;

/**
 * Context for stitching purpose (auto-generated handle ID) 
 * @author Debasis
 */
public class NDLStitchingContext {
	
	long autogenerateHandleID = 0;
	
	/**
	 * Sets starting auto-generated handle ID index by which system to try to generate different 
	 * handle along with some other informations
	 * @param autogenerateHandleID handle ID starting index
	 */
	public void setAutogenerateHandleID(long autogenerateHandleID) {
		this.autogenerateHandleID = autogenerateHandleID;
	}
	
	/**
	 * Gets next handle ID
	 * @return returns next handle ID
	 */
	public long nextAutoGenerateHandleID() {
		return ++autogenerateHandleID;
	}
}