package org.iitkgp.ndl.data;

/**
 * This transformer takes SIP as input and makes some changes then generates SIP back
 * (one to one mapping, see {@link Transformer})
 * @author Debasis
 */
public interface NDLSIPDataTransformer {
	
	/**
	 * Make changes and generates SIP
	 * @param input input to transform
	 */
	void transform(SIPDataItem input);
}