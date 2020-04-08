package org.iitkgp.ndl.context.custom;

/**
 * NDL context type
 * @author Debasis
 */
public enum NDLContext {
	
	/**
	 * career vertical
	 */
	CAREER_VERTICAL("Career_Vertical");
	
	String ctx; // context
	
	private NDLContext(String ctx) {
		this.ctx = ctx;
	}
	
	/**
	 * Gets context
	 * @return returns context
	 */
	public String getContext() {
		return ctx;
	}
}