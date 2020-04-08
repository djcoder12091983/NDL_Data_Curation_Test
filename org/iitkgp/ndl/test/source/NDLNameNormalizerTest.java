package org.iitkgp.ndl.test.source;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.iitkgp.ndl.util.NDLDataUtils;

// name normalizer test
public class NDLNameNormalizerTest {

	public static void main(String[] args) throws Exception {
		String file = "/home/dspace/debasis/NDL/test/names.test";
		for(String name : FileUtils.readLines(new File(file), "utf-8")) { 
			System.out.println(name + " => " + NDLDataUtils.normalizeSimpleName(name));
		}
	}
}