package org.iitkgp.ndl.data.transformer;

import java.util.Collection;

import org.iitkgp.ndl.data.Transformer;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * Adds prefix value transformer
 * @author Debasis
 */
public class PrefixAdderTransformer implements Transformer<String, String> {
	
	String prefix;
	
	/**
	 * Constructor
	 * @param prefix set prefix
	 */
	public PrefixAdderTransformer(String prefix) {
		this.prefix = prefix;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Collection<String> transform(String input) {
		return NDLDataUtils.createNewList(prefix + input);
	}
}