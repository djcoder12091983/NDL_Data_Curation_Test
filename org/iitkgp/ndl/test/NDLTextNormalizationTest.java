package org.iitkgp.ndl.test;

import java.util.List;

import org.apache.commons.io.IOUtils;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.junit.Test;

/**
 * Text normalization testing
 * @author debasis
 *
 */
public class NDLTextNormalizationTest {
	
	// text normalization test
	@Test
	public void textNormalizationTest() throws Exception {
		// System.out.println("a<br />homomorphism".replaceAll("(?i)<br */>", " "));
		List<String> lines = IOUtils.readLines(NDLDataUtils.loadResource("/data/test.normalization.sample.file"), "utf-8");
		for(String line : lines) {
			System.out.println(line);
			System.out.println(NDLDataUtils.removeHTMLTagsAndFixLatex(line));
			System.out.println();
		}
	}
}