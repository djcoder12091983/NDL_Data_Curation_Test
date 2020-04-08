package org.iitkgp.ndl.data;

/**
 * Handle ID representation
 * @author debasis
 */
public class Handle {
	String prefix;
	String suffix;
	
	/**
	 * constructor
	 * @param prefix handle prefix
	 * @param suffix handle suffix
	 */
	public Handle(String prefix, String suffix) {
		this.prefix = prefix;
		this.suffix = suffix;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj == null) {
			return false;
		}
		if(obj == this) {
			return true;
		}
		if(obj instanceof Handle) {
			Handle h = (Handle)obj;
			return h.prefix.equals(this.prefix) && h.suffix.equals(this.suffix);
		} else {
			return false; 
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		int h = 13;
		h += h * 31 + prefix.hashCode();
		h += h * 31 + suffix.hashCode();
		return h;
	}
}