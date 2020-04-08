package org.iitkgp.ndl.test;

import static org.junit.Assert.assertEquals;

import org.iitkgp.ndl.util.CommonUtilities;
import org.junit.Test;

/**
 * Test of {@link CommonUtilitiesTest}
 * @author Debasis
 */
public class CommonUtilitiesTest {
	
	// test duration message
	@Test
	public void testDuration() {
		assertEquals("Time taken : 2 Minutes 2 Seconds", CommonUtilities.durationMessage(122*1000));
		assertEquals("Time taken : 2 Hours 30 Minutes", CommonUtilities.durationMessage(360*25*1000));
	}

}