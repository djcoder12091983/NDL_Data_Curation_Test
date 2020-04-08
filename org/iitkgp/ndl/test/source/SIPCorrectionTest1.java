package org.iitkgp.ndl.test.source;

import java.util.List;

import org.iitkgp.ndl.context.custom.NDLContext;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.ConfigurationData;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

public class SIPCorrectionTest1 extends NDLSIPCorrectionContainer {
	
	/*Set<String> subject;
	int limit = 5000;
	Map<String, Integer> subjectm = new HashMap<>();*/
	
	public SIPCorrectionTest1(String input, String logLocation, String outputLocation, String name) {
		// validation off
		super(input, logLocation, outputLocation, name, false);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		/*Set<String> fields = target.getAllFields();
		removeMultipleLines(fields, true);
		removeMultipleSpaces(fields, true);*/
		
		//removeMultipleLinesAndSpaceForAllFields(true);
		//removeMultipleLines("dc.description.abstract");
		//removeMultipleSpaces("dc.description.abstract");
		/*removeMultipleLinesAndSpace("dc.description.abstract");
		
		deleteIfContains("dc.description.abstract", ".No abstract provided.");
		if(target.exists("dc.description.abstract")) {
			target.updateSingleValue("dc.description.abstract",
					target.getSingleValue("dc.description.abstract").replaceFirst("\\?", "").trim());
		}*/
		
		/*target.print("dc.format.extent:startingPage");
		target.print("dc.format.extent:size_in_Bytes");
		target.print("dc.format.extent:endingPage");
		target.print("dc.format.extent:pageCount");
		target.print("dc.contributor.other:judge");
		target.print("dc.contributor.other:interviewer");*/
		
		/*String subject = target.getSingleValue("dc.subject");
		if(StringUtils.isNotBlank(subject)) {
			Integer c = subjectm.get(subject);
			if(c == null) {
				subjectm.put(subject, 1);
			} else {
				if(c.intValue() > limit) {
					// exceeds limit then skip items
					return false;
				}
				subjectm.put(subject, c.intValue() + 1);
			}
		}*/
		
		/*String id = target.getId();
		if(StringUtils.equals(id, "shodhganga/10603_101")) {
			System.out.println("B: " + target.getSingleValue("dc.publisher.department"));
		}
		
		target.replaceByRegex("dc.publisher.department", "\\\\n", "#XXXX#");
		
		if(StringUtils.equals(id, "shodhganga/10603_101")) {
			System.out.println("A: " + target.getSingleValue("dc.publisher.department"));
		}*/
		
		//System.out.println(target.getValue("dc.contributor.author"));
		/*List<String> authors = target.getValue("dc.contributor.author");
		for(String author : authors) {
			if(author.contains("ã") || author.contains("Ã")) {
				String ma = author.replace('ã', 'Ã');
				System.out.println(author + " (" + ma + ") => " + NDLDataUtils.convertText(ma, "ISO-8859-1", "UTF-8"));
			}
		}*/
		
		// ndl.subject.bisac
		List<String> bisacs = target.getValue("ndl.subject.bisac");
		for(String bisac : bisacs) {
			// remove BISAC::
			String key = "ddc." + ConfigurationData.escapeDot(bisac.substring(7));
			if(containsMappingKey(key)) {
				// add and normalize
				add("ndl.subject.ddc", getMappingKey(key), ';');
				//System.out.println("DDC: " + target.getValue("ndl.subject.ddc"));
			}
		}
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		
		//NDLConfigurationContext.addConfiguration("ndl.service.base.url", "http://10.72.22.239:65/services/");
		
		// flat SIP location or compressed SIP location
		String input = "/home/dspace/debasis/NDL/NDL_sources/Samim/RRB-GD_23_01_2020.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/Samim/temp"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/Samim/temp";
		String name = "sip.ddc.codes.test";
		
		//String file = "/home/dspace/debasis/NDL/NDL_sources/Karnatak_HC_Stitch/subject.list";
		String file = "/home/dspace/debasis/NDL/NDL_sources/Samim/DDC.csv";
		
		SIPCorrectionTest1 p = new SIPCorrectionTest1(input, logLocation, outputLocation, name);
		
		// switch context
		p.switchContext(NDLContext.CAREER_VERTICAL);
		
		//p.subject = NDLDataUtils.loadSet(file);
		//p.turnOffLoadHierarchyFlag();
		//p.turnOffControlFieldsValidationFlag();
		//p.turnOnFieldWiseDetailValidation();
		p.addMappingResource(file, "Key", "ddc");
		p.correctData();
		
		System.out.println("Done.");
	}
}