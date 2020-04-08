package org.iitkgp.ndl.test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.iitkgp.ndl.json.NDLJSONParser;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.junit.Test;

/**
 * Test cases of {@link NDLJSONParser}
 * @author Vishal, Debasis
 */
public class NDLJSONParserTest {
	
	/**
	 * test
	 * @throws Exception throws exception if parsing error occurs
	 */
	@Test
	public void test() throws Exception {
		String jsonFile = "/data/sample.json";
		NDLJSONParser parser = new NDLJSONParser(NDLDataUtils.loadResource(jsonFile));
		assertEquals("example glossary", parser.getText("glossary.title"));
		assertEquals("S", parser.getText("glossary.GlossDiv.title"));
		assertEquals("A meta-markup language, used to create markup languages such as DocBook.",
				parser.getText("glossary.GlossDiv.GlossList.GlossEntry.GlossDef.para"));
		assertEquals("GML",
				parser.getText("glossary.GlossDiv.GlossList.GlossEntry.GlossDef.GlossSeeAlso[0]"));
		// TODO more testing
		
		parser = new NDLJSONParser("{\"data\": [\"ndl data1\", \"ndl data2\"]}");
		assertEquals("ndl data1", parser.getText("data[0]"));
		assertEquals("ndl data2", parser.getText("data[1]"));
	}
	
	// HAL Source specific testing
	@Test
	public void halNamesTest() throws Exception {
		List<String> lines = FileUtils.readLines(new File(NDLDataUtils.getResourcePath("/data/hal.names.json")), "utf-8");
		for(String line : lines) {
			System.out.println(line);
			NDLJSONParser parser = new NDLJSONParser(line);
			System.out.println("Name1: " + parser.getText("firstname", ""));
			System.out.println("Name2: " + parser.getText("middlename", ""));
			System.out.println("Name3: " + parser.getText("surname", ""));
			
			System.out.println();
		}
	}
}