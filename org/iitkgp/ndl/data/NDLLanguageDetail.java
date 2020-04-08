package org.iitkgp.ndl.data;

import org.apache.commons.lang3.StringUtils;

/**
 * NDL language detail class
 * @author Debasis
 * 
 * @see NDLLanguageTranslate
 */
public class NDLLanguageDetail {
	
	String lang;
	String translation;
	
	/**
	 * Constructor
	 * @param lang given language
	 * @param translation translated version for given language
	 */
	public NDLLanguageDetail(String lang, String translation) {
		this.lang = lang;
		this.translation = translation;
	}
	
	/**
	 * Gets language name
	 * @return returns language name
	 */
	public String getLang() {
		return lang;
	}
	
	/**
	 * Gets translated version for given language
	 * @return returns translated version for given language
	 */
	public String getTranslation() {
		return translation;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		return lang.hashCode();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj == null) {
			return false;
		} else if(obj == this) {
			return true;
		} else if(obj instanceof NDLLanguageDetail) {
			return StringUtils.equals(lang, ((NDLLanguageDetail)obj).lang);
		} else {
			return false;
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "{Language: " + lang + ", Translated: " + translation + "}";
	}
}