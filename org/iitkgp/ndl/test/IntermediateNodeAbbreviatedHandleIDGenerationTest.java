package org.iitkgp.ndl.test;

import org.iitkgp.ndl.data.correction.stitch.IntermediateNodeAbbreviatedHandleIDGeneration;
import org.iitkgp.ndl.data.correction.stitch.context.NDLStitchingContext;
import org.junit.Test;

/**
 * Testing of 'IntermediateNodeAbbreviatedHandleIDGeneration'
 * @author Debasis
 */
public class IntermediateNodeAbbreviatedHandleIDGenerationTest {
	
	// testing
	@Test
	public void test() {
		NDLStitchingContext context = new NDLStitchingContext();
		IntermediateNodeAbbreviatedHandleIDGeneration t = new IntermediateNodeAbbreviatedHandleIDGeneration(1, 2, 3);
		System.out.println(t.generate(1, "debasis jana", context));
		System.out.println(t.generate(1, "tilak mukho padhyay", context));
		System.out.println(t.generate(2, "my name is debasis jana xxxx", context));
		System.out.println(t.generate(3, "my name is xxxx yyy zzz", context));
		System.out.println(t.generate(1, "debasis jana", context));
		System.out.println(t.generate(3, "debasis jana", context));
	}

}