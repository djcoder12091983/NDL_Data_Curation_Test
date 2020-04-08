package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.converter.NDLAIP2CSVConverter;

// AIP 2 CSV conversion test
public class AIP2CSVConverterTest {
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/Tripura_Board-215BKP-28.06.2019.tar.gz";
		String logLocation  = "/home/dspace/debasis/NDL/NDL_sources/csv_data";
		
		// NDLConfigurationContext.addConfiguration("compressed.data.process.buffer.size", "100");
		
		NDLAIP2CSVConverter converter = new NDLAIP2CSVConverter(input, logLocation, "sip.csv.test");
		converter.setMultivalueSeparator('|');
		converter.setCsvThresholdLimit(100000);
		
		//converter.addColumnSelector("dc.identifier.other:alternateContentUri", "Desc");
		//converter.addColumnSelector("dc.title", "Title");
		//converter.addColumnSelector("dc.description.abstract", "Abstract");
		/*converter.addColumnSelector("dc.publisher.date", "Date");
		converter.addColumnSelector("dc.identifier.other:doi", "DOI");
		converter.addColumnSelector("dc.identifier.other:alternateContentUri", "Link");*/
		
		/*converter.addColumnSelector("dc.identifier.other", "Identifier.Other");
		converter.addColumnSelector("dc.relation", "Relation");*/
		
		converter.convert();
		
		/*Date date1 = DateUtils.parseDate("2018-11-12", "yyyy-MM-dd");
		Date date2 = DateUtils.parseDate("2018-11-13", "yyyy-MM-dd");
		if(date1.after(date2)) {
			// date 1 is bigger
			System.out.println(date1);
		} else {
			// date 2 is bigger
			System.out.println(date2);
		}*/
		
		/*String text = "Comment: 5 pages, 4 figures";
		String tokens[] = NDLDataUtils.split(text, "(:| +|,|;)");
		String pages = null;
		boolean acceptf = false;
		int l = tokens.length;
		for(int i = 0; i < l; i++) {
			String t = tokens[i].trim();
			if(StringUtils.startsWithIgnoreCase(t, "page")) {
				pages = tokens[i - 1];
			} else if(StringUtils.startsWithIgnoreCase(t, "accept")) {
				acceptf = true;
			}
		}
		
		System.out.println(pages + " " + acceptf);*/
		
		System.out.println("Done.");
	}
}