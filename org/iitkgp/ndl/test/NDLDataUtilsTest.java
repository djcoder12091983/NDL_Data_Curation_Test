package org.iitkgp.ndl.test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.iitkgp.ndl.data.NDLAssetType;
import org.iitkgp.ndl.data.NDLLanguageDetail;
import org.iitkgp.ndl.data.NDLLanguageTranslate;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.junit.Test;

/**
 * Test cases of {@link NDLDataUtils}
 * @author Debasis
 */
public class NDLDataUtilsTest {
	
	// normalizeDate test
	@Test
	public void normalizeDateTest() {
		assertEquals(NDLDataUtils.normalizeSimpleDate("September 1979", " +"), "1979-09-01");
		assertEquals(NDLDataUtils.normalizeSimpleDate("August 10 1908", " +"), "1908-08-10");
		assertEquals(NDLDataUtils.normalizeSimpleDate("08 Nov 2017", " +"), "2017-11-08");
		//System.out.println(NDLDataUtils.normalizeDate("Februray 2 1942", " +"));
	}
	
	// test of extractNumbers
	@Test
	public void extractNumberstest() {
		List<Long> numbers = NDLDataUtils.extractNumbers("vol. 9");
		assertEquals((long)numbers.get(0), 9);
		numbers = NDLDataUtils.extractNumbers("29");
		assertEquals((long)numbers.get(0), 29);
		numbers = NDLDataUtils.extractNumbers("vol. 19 issue: 10");
		assertEquals((long)numbers.get(0), 19);
		assertEquals((long)numbers.get(1), 10);
	}
	
	// test deleteAssetText
	@Test
	public void testDeleteAssetText() {
		String text = "thumb.jpg\tbundle:" + NDLAssetType.THUMBNAIL.getType() + NDLDataUtils.NEW_LINE
				+ "fulltext.txt\tbundle:" + NDLAssetType.FULLTEXT.getType();
		String modified = NDLDataUtils.deleteAssetText(text, "thumb.jpg", NDLAssetType.THUMBNAIL);
		assertEquals(modified, "fulltext.txt	bundle:TEXT");
		modified = NDLDataUtils.deleteAssetText(text, "fulltext.txt", NDLAssetType.FULLTEXT);
		assertEquals(modified, "thumb.jpg	bundle:THUMBNAIL");
	}
	
	// language translation JSON test
	@Test
	public void languageTranslationJSONTest() {
		NDLLanguageTranslate detail = new NDLLanguageTranslate("author");
		detail.addValue(new NDLLanguageDetail("eng", "debasis jana"), new NDLLanguageDetail("hin", "डेबसिस जन"));
		detail.addValue(new NDLLanguageDetail("eng", "tilak mu"), new NDLLanguageDetail("hin", ""),
				new NDLLanguageDetail("ur", "तिलक मु"));
		detail.addValue(new NDLLanguageDetail("eng", ""), new NDLLanguageDetail("hin", ""));
		String json = NDLDataUtils.serializeLanguageTranslation(detail);
		NDLLanguageTranslate obtained = NDLDataUtils.desrializeLanguageTranslation(json);
		List<List<NDLLanguageDetail>> values = obtained.getValues();
		for(List<NDLLanguageDetail> value : values) {
			System.out.println(value);
		}
	}
	
	// simple name normalization test
	@Test
	public void simpleNameNormalizationTest() {
		System.out.println(NDLDataUtils.normalizeSimpleName("MacMillan, Don"));
		System.out.println(NDLDataUtils.normalizeSimpleName("Jana, DK"));
		System.out.println(NDLDataUtils.normalizeSimpleName("A. Kruger"));
		System.out.println(NDLDataUtils.normalizeSimpleName("Peter axyz ac"));
		System.out.println(NDLDataUtils.normalizeSimpleName("Peter Ãac"));
		System.out.println(NDLDataUtils.normalizeSimpleName("Gestionnaire HAL-SU"));
		System.out.println(NDLDataUtils.normalizeSimpleName("Christophe Penkerc'h"));
		
		Set<String> wrongNameTokens = new HashSet<String>(2);
		wrongNameTokens.add("phd");
		wrongNameTokens.add("professor");
		wrongNameTokens.add("of");
		wrongNameTokens.add("nutrition");
		System.out.println(NDLDataUtils.normalizeSimpleNameByWrongNameTokens("A. Kruger PhD, Professor Of Nutrition",
				wrongNameTokens));
	}
	
	// shorthand display date
	@Test
	public void shorthandDisplayDateTest() {
		System.out.println(NDLDataUtils.getShorthandDisplayDate(1));
		System.out.println(NDLDataUtils.getShorthandDisplayDate(2));
		System.out.println(NDLDataUtils.getShorthandDisplayDate(3));
		System.out.println(NDLDataUtils.getShorthandDisplayDate(6));
		System.out.println(NDLDataUtils.getShorthandDisplayDate(11));
		System.out.println(NDLDataUtils.getShorthandDisplayDate(12));
		System.out.println(NDLDataUtils.getShorthandDisplayDate(13));
		System.out.println(NDLDataUtils.getShorthandDisplayDate(17));
		System.out.println(NDLDataUtils.getShorthandDisplayDate(21));
		System.out.println(NDLDataUtils.getShorthandDisplayDate(22));
		System.out.println(NDLDataUtils.getShorthandDisplayDate(23));
		System.out.println(NDLDataUtils.getShorthandDisplayDate(27));
		System.out.println(NDLDataUtils.getShorthandDisplayDate(30));
		System.out.println(NDLDataUtils.getShorthandDisplayDate(31));
	}
	
	// PDF age count test
	@Test
	public void pdfPageCountTest() throws Exception {
		String pdfFile = "/data/sample.pdf";
		System.out.println(
				"Page Count: " + NDLDataUtils.getPDFPageCount(new File(NDLDataUtils.getResourcePath(pdfFile))));
	}
	
	// more test
	@Test
	public void moreTest() throws Exception {
		System.out.println(NDLDataUtils.formatAsPT("10:01:11"));
		System.out.println(NDLDataUtils.formatAsPT("10:11"));
		System.out.println(NDLDataUtils.formatAsPT("12:11:10:13"));
		System.out.println(NDLDataUtils.formatAsPT("10:2:12:11:10:13"));
		
		String modified = NDLDataUtils.normalizeSpace("Louis V. Avioli");
		System.out.println(modified);
		System.out.println((int)modified.charAt(8));
		
		System.out.println(NDLDataUtils.validateHandle("__ABcx_drfty8765__"));
		System.out.println(NDLDataUtils.validateHandle("__ABcx_drfty8765.-----__"));
		System.out.println(NDLDataUtils.validateHandle("__ABcx_drfty876&^$&^$&@!$&__"));
		
		System.out.println("Date format: " + NDLDataUtils.isNDLValidDateFormat("2012-34-23"));
		System.out.println("Abbreviated: " + NDLDataUtils.splitByInitialLetter("debasis jana"));
		System.out.println("Abbreviated: " + NDLDataUtils.splitByInitialLetter("debasis jana 123456789 xyz12345ty"));
		System.out.println("Abbreviated: " + NDLDataUtils.splitByInitialLetter("NOC:Deep Learning - Part 2106106201"));
		
		System.out.println(NDLDataUtils.convertText("PeÃ±a-gomez, CleofÃ©", "ISO-8859-1", "UTF-8"));
		System.out.println(NDLDataUtils.convertText("BargallÃ³, NÃºria", "ISO-8859-1", "UTF-8"));
	}
}