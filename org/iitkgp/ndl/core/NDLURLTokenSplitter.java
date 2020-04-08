package org.iitkgp.ndl.core;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.validator.AbstractNDLDataValidator;

/**
 * Splitting logic URL which will be used to store into NDL-map
 * This class used by validation framework to detect duplicate URL(s) error
 * @see NDLFieldTokenSplittingLoader
 * @see AbstractNDLDataValidator
 * @author Debasis
 */
public class NDLURLTokenSplitter implements NDLFieldTokenSplitter<String, String[]> {
	// URL splitting logic for tokens to store in prefix-tree (ndl-map)
	/**
	 * Splits URL into tokens,
	 * for example <b>http://mysite/abc/xyz/1.html</b> splitted into <b>[mysite,abc,xyz,1.html]</b>
	 */
	@Override
	public String[] split(String input) {
		String first, second = null;
		String splits[] = input.split("\\?");
		first = splits[0].trim();
		if(splits.length == 2) {
			second = splits[1];
		}
		if(first.startsWith("http://")) {
			first = first.substring(7);
		} else if(first.startsWith("https://")) {
			first = first.substring(8);
		} else {
			return new String[]{}; // empty list
		}
		List<String> urlTokens = new LinkedList<String>();
		splits = first.split("/");
		for(String token : splits) {
			if(StringUtils.isNotBlank(token)) {
				urlTokens.add(token);
			}
		}
		if(StringUtils.isNotBlank(second)) {
			// query parameter processing
			splits = second.split("&");
			for(String token : splits) {
				if(StringUtils.isNotBlank(token)) {
					urlTokens.add(token);
				}
			}
		}
		String arrayTokens[] = new String[urlTokens.size()];
		return urlTokens.toArray(arrayTokens);
	}
}