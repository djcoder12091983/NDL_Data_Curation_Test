package org.iitkgp.ndl.test.source;

import java.io.File;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

// T&F source program test
public class TAndFProgramTest {
	
	public static void main(String[] args) throws Exception {
		String file = "/home/dspace/debasis/NDL/generated_xml_data/Taylor and Francis/conf/journals.csv";
		Map<String, String> journals = NDLDataUtils.loadKeyValue(file);
		
		String stitchFile = "/home/dspace/debasis/NDL/generated_xml_data/Taylor and Francis/backup/TnF_Dir_Struct_v2.0.csv";
		CSVReader reader = NDLDataUtils.readCSV(new File(stitchFile));
		String[] line = null;
		//Set<String> sjournals = new HashSet<String>(2);
		String outFile = "/home/dspace/debasis/NDL/generated_xml_data/Taylor and Francis/logs/suppl_journals.csv";
		CSVWriter writer = NDLDataUtils.openCSV(new File(outFile));
		writer.writeNext(new String[]{"Journal-Code", "Journal-Name", "Volume", "Issue", "URL"});
		while((line = reader.readNext()) != null) {
			String jcode = line[0];
			String issue = line[2];
			// System.out.println(jcode + "\t" + issue);
			if(StringUtils.containsIgnoreCase(issue, "supp")) {
				// System.out.println("Yahoo!!");
				// distinct journals for suppl issue
				//sjournals.add();
				writer.writeNext(new String[]{jcode, journals.get(jcode), line[1], issue, line[3]});
			}
		}
		
		reader.close();
		writer.close();
		
		//System.out.println("Done: " + sjournals);
		System.out.println("Done.");
	}
}