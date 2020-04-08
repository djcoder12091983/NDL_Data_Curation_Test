package org.iitkgp.ndl.test;

import org.iitkgp.ndl.data.NDLSubjectPrerequisiteTopic;
import org.junit.Test;

/**
 * {@link NDLSubjectPrerequisiteTopic} testing
 * @author Debasis
 */
public class NDLSubjectPrerequisiteTopicTest {
	
	@Test
	public void test() {
		NDLSubjectPrerequisiteTopic t = new NDLSubjectPrerequisiteTopic();
		t.addItem("1234_abc/123456", "item1");
		t.addItem("1234_abc/123457", "item2");
		
		t.addTopic("debasis");
		t.addTopic("jana");
		
		System.out.println(t.jsonify());
	}
}