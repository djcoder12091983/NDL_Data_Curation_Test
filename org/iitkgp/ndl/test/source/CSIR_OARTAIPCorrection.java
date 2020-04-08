package org.iitkgp.ndl.test.source;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.correction.NDLAIPCorrectionContainer;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizer;
import org.iitkgp.ndl.data.normalizer.NDLSimpleNameNormalizer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class CSIR_OARTAIPCorrection extends NDLAIPCorrectionContainer {
	
	// constructor
	public CSIR_OARTAIPCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	String wordNormalize(String input) {
		String tokens[] = input.split(" +");
		StringBuilder out = new StringBuilder();
		for(String t : tokens) {
			if(t.equalsIgnoreCase("of")) {
				out.append("of");
			} else {
				out.append(NDLDataUtils.initCap(t));
			}
			out.append(" ");
		}
		out.deleteCharAt(out.length() - 1);
		return out.toString();
	}
	
	@Override
	protected boolean correctTargetItem(AIPDataItem target) throws Exception {
		
		normalize("dc.contributor.author", "dc.creator.researcher", "dc.date.submitted");
		
		List<String> desc = target.getValue("dc.description");
		for(String d : desc) {
			if(StringUtils.containsIgnoreCase(d, "PhD thesis")) {
				String tokens[] = d.split(" *, *");
				int l = tokens.length;
				for(int i = 0; i < l; i++) {
					if(StringUtils.containsIgnoreCase(tokens[i], "PhD thesis")) {
						String inst = tokens[i + 1];
						target.add("dc.publisher.institution", wordNormalize(inst.replaceFirst("\\.$", "")));
						if(tokens.length > i + 2) {
							String place = tokens[i + 2];
							target.add("dc.publisher.place", wordNormalize(place.split(" +")[0].replaceFirst("\\.$", "")));
						}
						break;
					}
				}
				break;
			}
		}
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/2018.Nov.26.17.03.43.CSIR_OARIT.tar.gz";
		String logLocation = "<logs>";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/out_done";
		String name = "csir.oart";
		
		CSIR_OARTAIPCorrection p = new CSIR_OARTAIPCorrection(input, logLocation, outputLocation, name);
		NDLDataNormalizer n = new NDLSimpleNameNormalizer();
		p.addNormalizer("dc.contributor.author", n);
		p.addNormalizer("dc.creator.researcher", n);
		p.correctData();
		
		System.out.println("Done.");
	}
}