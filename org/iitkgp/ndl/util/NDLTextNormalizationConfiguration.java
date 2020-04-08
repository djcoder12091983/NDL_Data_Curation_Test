package org.iitkgp.ndl.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * See normalizeText section of <a href="http://www.dataentry.ndl.iitkgp.ac.in/helper/#data-services">Services</a>
 * @author Debasis
 */
public class NDLTextNormalizationConfiguration {
	
	boolean htmlProcess = true; // HTML process flag
	boolean htmlRemove = true; // whether to remove HTML or not
	boolean jsRemove = true; // whether to remove JS or not
	boolean cssRemove = true; // whether to remove CSS or not
	boolean unicodeConversion = true; // unicode conversion considered or not
	boolean whitespaceRemove = true; // whether to remove white space or not
	boolean processAsJson = false; // treat it JSON or not
	
	/**
	 * Sets HTML process flag
	 * @param htmlProcess HTML process flag
	 */
	public void setHtmlProcess(boolean htmlProcess) {
		this.htmlProcess = htmlProcess;
	}
	
	/**
	 * Gets HTML process flag
	 * @return returns HTML process flag
	 */
	public boolean isHtmlProcess() {
		return htmlProcess;
	}
	
	/**
	 * Whether to remove HTML or not
	 * @param htmlRemove HTML remove flag
	 */
	public void setHtmlRemove(boolean htmlRemove) {
		this.htmlRemove = htmlRemove;
	}
	
	/**
	 * Returns HTML remove flag
	 * @return returns HTML remove flag
	 */
	public boolean isHtmlRemove() {
		return htmlRemove;
	}
	
	/**
	 * Sets JS remove flag
	 * @param jsRemove JS remove flag
	 */
	public void setJsRemove(boolean jsRemove) {
		this.jsRemove = jsRemove;
	}
	
	/**
	 * Returns JS remove flag
	 * @return JS remove flag
	 */
	public boolean isJsRemove() {
		return jsRemove;
	}
	
	/**
	 * Sets CSS remove flag
	 * @param cssRemove CSS remove flag
	 */
	public void setCssRemove(boolean cssRemove) {
		this.cssRemove = cssRemove;
	}
	
	/**
	 * Returns CSS remove flag
	 * @return returns CSS remove flag
	 */
	public boolean isCssRemove() {
		return cssRemove;
	}
	
	/**
	 * Sets space remove flag
	 * @param whitespaceRemove space remove flag
	 */
	public void setWhitespaceRemove(boolean whitespaceRemove) {
		this.whitespaceRemove = whitespaceRemove;
	}
	
	/**
	 * Returns space remove flag
	 * @return returns space remove flag
	 */
	public boolean isWhitespaceRemove() {
		return whitespaceRemove;
	}
	
	/**
	 * Sets unicode process flag
	 * @param unicodeConversion unicode process flag
	 */
	public void setUnicodeConversion(boolean unicodeConversion) {
		this.unicodeConversion = unicodeConversion;
	}
	
	/**
	 * Returns unicode process flag
	 * @return returns unicode process flag
	 */
	public boolean isUnicodeConversion() {
		return unicodeConversion;
	}
	
	/**
	 * Sets JSON process flag
	 * @param processAsJson JSON process flag
	 */
	public void setProcessAsJson(boolean processAsJson) {
		this.processAsJson = processAsJson;
	}
	
	/**
	 * Returns JSON process flag
	 * @return returns JSON process flag
	 */
	public boolean isProcessAsJson() {
		return processAsJson;
	}
	
	/**
	 * Loads configuration parameters into box
	 * @param params given parameters box
	 */
	public void loadParameters(Map<String, Collection<String>> params) {
		params.put("html_process", Arrays.asList(String.valueOf(htmlProcess)));
		params.put("html_remove", Arrays.asList(String.valueOf(htmlRemove)));
		params.put("js_remove", Arrays.asList(String.valueOf(jsRemove)));
		params.put("unicode_convert", Arrays.asList(String.valueOf(unicodeConversion)));
		params.put("whitespace_remove", Arrays.asList(String.valueOf(whitespaceRemove)));
		params.put("process_as_json", Arrays.asList(String.valueOf(processAsJson)));
		params.put("css_remove", Arrays.asList(String.valueOf(cssRemove)));
	}
}