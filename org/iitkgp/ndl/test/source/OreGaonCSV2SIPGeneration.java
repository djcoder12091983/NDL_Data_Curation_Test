package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.RowData;
import org.iitkgp.ndl.data.generation.NDLCSVToSIPGeneration;

public class OreGaonCSV2SIPGeneration extends NDLCSVToSIPGeneration {
	
	public OreGaonCSV2SIPGeneration(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean generateTargetItem(RowData csv, RowData sip) throws Exception {
		
		copy("ndl.sourceMeta.additionalInfo", "ndl.sourceMeta.additionalInfo");
		copy("dc.relation.requires", "dc.relation.requires");
		copy("dc.date.other", "dc.date.other");
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {

		String input = "/home/dspace/debasis/NDL/generated_xml_data/oregaon/Oregon.csv";
		String logLocation = "<log location>";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/oregaon/";
		String name = "Oregon_Data";

		OreGaonCSV2SIPGeneration p = new OreGaonCSV2SIPGeneration(input, logLocation, outputLocation, name);
		//p.setMultilineLimit(100);
		p.setHandleIDPrefix("uoregon");
		p.setPrimaryIDColumn("Handle_ID");
		p.processData();
		
		/*String file = "/home/dspace/debasis/NDL/generated_xml_data/oregaon/Oregon.csv";
		String log = "/home/dspace/debasis/NDL/generated_xml_data/oregaon/";
		CSVReader reader = NDLDataUtils.readCSV(new File(file), ',', '"', 1000, 1);
		
		System.setOut(new PrintStream(new FileOutputStream(new File(log, "log.file"))));
		
		String tokens[] = null;
		int c = 0;
		while((tokens = reader.readNext()) != null) {
			System.out.print(++c + " =======> ");
			for(String t : tokens) {
				System.out.print(t + "\t");
			}
			System.out.println();
			//break;
		}
		
		reader.close();*/
		
		System.out.println("Done..");
	}

}