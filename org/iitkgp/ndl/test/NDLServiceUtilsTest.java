package org.iitkgp.ndl.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.iitkgp.ndl.data.duplicate.checker.DuplicateDocument;
import org.iitkgp.ndl.data.duplicate.checker.DuplicateDocumentsOutput;
import org.iitkgp.ndl.util.AccessRights;
import org.iitkgp.ndl.util.NDLServiceUtils;
import org.iitkgp.ndl.util.NDLURLType;
import org.junit.Test;

/**
 * Test cases of {@link NDLServiceUtils}
 * @author Debasis
 */
public class NDLServiceUtilsTest {
	
	/*static {
		System.setProperty("no_proxy", "10.72.22.155");
	}*/
	
	// access rights test
	//@Test
	public void accessRightsTest() throws Exception {
		assertEquals(NDLServiceUtils.getAccessRights("http://eprints-bangaloreuniversity.in/5342/", NDLURLType.EPRINTS),
				AccessRights.OPEN.getAccessRights());
	}
	
	//@Test
	public void normalizeDates() throws Exception {
		System.out.println(NDLServiceUtils.normalilzeDate("2009"));
		List<String> dates = new LinkedList<String>();
		dates.add("abcd");
		dates.add("2018");
		System.out.println(NDLServiceUtils.normalilzeDate(dates));
	}
	
	//@Test
	public void duplicateCheckerTest() throws Exception {
		Collection<String> values = new ArrayList<String>(4);
		values.add("abc.xyz");
		values.add("abc.123");
		values.add("10.1103/PhysRevA.75.043613");
		values.add("10.1109/ULTSYM.1983.198060");
		DuplicateDocumentsOutput duplicates = NDLServiceUtils.duplicateChecker("doi", values);
		for(DuplicateDocument duplicate : duplicates.getDocuments()) {
			System.out.println(duplicate.getValue() + " ==> " + duplicate.getNdli_id());
		}
	}
	
	@Test
	public void duplicateCheckerTest1() throws Exception {
		Collection<String> values = new ArrayList<String>();
		/*24549270v1
		20311478v1
		5448873
		5396866v1*/
		values.add("10132527v1");
		values.add("6345323v1");
		values.add("10007526v1");
		values.add("22170444v1");
		DuplicateDocumentsOutput duplicates = NDLServiceUtils.duplicateChecker("pmId", values);
		for(DuplicateDocument duplicate : duplicates.getDocuments()) {
			System.out.println(duplicate.getValue() + " ==> " + duplicate.getNdli_id());
		}
	}
}