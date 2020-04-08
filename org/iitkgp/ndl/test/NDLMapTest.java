package org.iitkgp.ndl.test;

import static org.junit.Assert.assertEquals;

import java.util.LinkedList;
import java.util.List;

import org.iitkgp.ndl.core.NDLMap;
import org.iitkgp.ndl.core.NDLMapKeyProcessor;
import org.junit.Test;

/**
 * Test cases of {@link NDLMap}
 * @author Debasis
 */
public class NDLMapTest {
	
	class TestHandler implements NDLMapKeyProcessor<List<String>> {
		
		List<List<String>> duplicateValues = new LinkedList<List<String>>();
		
		@Override
		public void process(List<String> data) {
			if(data.size() > 1) {
				// multiple entries
				duplicateValues.add(data); // track
			}
		}
	}
	
	@Test
	public void test() {
		NDLMap<List<String>> titles = new NDLMap<List<String>>();
		add(titles, "debasis jana", "1");
		add(titles, "debasis jana", "2");
		add(titles, "subhra lahiri", "3");
		assertEquals(titles.containsKey("subhra lahiri".split(" +")), true);
		assertEquals(titles.containsKey("debasis jana".split(" +")), true);
		assertEquals(titles.containsKey("xxx yyy".split(" +")), false);
		
		TestHandler handler = new TestHandler();
		titles.setKeyProcessor(handler);
		titles.iterate();
		assertEquals(handler.duplicateValues.get(0).size() == 2, true);
	}
	
	void add(NDLMap<List<String>> titles, String value, String data) {
		String tokens[] = value.split(" +");
		List<String> list = titles.get(tokens);
		if(list == null) {
			// first time
			list = new LinkedList<String>();
			titles.add(tokens, list);
		}
		list.add(data);
	}
}