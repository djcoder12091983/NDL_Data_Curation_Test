package org.iitkgp.ndl.test;

import static org.junit.Assert.assertEquals;

import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.util.NDLServiceUtils;
import org.junit.Test;

// NDLConfigurationContext testing
public class NDLConfigurationContextTest {
	
	static {
		// change configuration
		NDLConfigurationContext.addConfiguration("ndl.service.base.url", "http://dataentry.ndl.iitkgp.ac.in/services/");
	}
	
	@Test
	public void test() throws Exception {
		assertEquals(NDLServiceUtils.normalilzeLanguage("en"), "eng");
	}
}