package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.AIPFileGroup;
import org.iitkgp.ndl.data.NDLAssetType;
import org.iitkgp.ndl.data.correction.NDLAIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

// JEE advanced AIP correction
public class JEEAdvancedAIPCorrection extends NDLAIPCorrectionContainer {
	
	// constructor
	public JEEAdvancedAIPCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(AIPDataItem target) throws Exception {
		
		// correction
		
		target.delete("dc.identifier.uri");
		
		target.add("dc.language.iso", "eng");
		target.add("dc.type", "text");
		target.add("dc.source","NDLI Tutor");
		target.add("dc.source.uri", "http://www.jeeadv.ac.in/");
		target.add("dc.format.mimetype", "application/pdf");
		target.add("lrmi.educationalUse", "classroom", "reading", "problemSolving");
		target.add("lrmi.interactivityType", "expositive");
		target.add("lrmi.typicalAgeRange", "13-17", "18-22");
		target.add("lrmi.educationalAlignment.educationalLevel", "career_tech");
		target.add("lrmi.educationalRole", "student", "teacher");
		target.add("dc.rights.accessRights", "open");
		
		
		
		/*// prerequisitetopic
		if(target.exists("dc.subject.prerequisitetopic")) {
			NDLSubjectPrerequisiteTopic t = new NDLSubjectPrerequisiteTopic();
			t.addTopics(target.getValue("dc.subject.prerequisitetopic"));
			// delete the field
			target.delete("dc.subject.prerequisitetopic");
			target.add("dc.subject.prerequisitetopic", t.jsonify());
		}*/
		
		transformFieldsById("title", false, "<Title,dc.title>");
		
		AIPFileGroup original = target.getAIPFileGroupDetail(NDLAssetType.ORIGINAL);
		if(original != null) {
			target.updateSingleValue("dc.description.searchVisibility",
					String.valueOf(target.originalBitstreamExists()));
			byte[] contents = target.getBitstreamContentsByName(original.getName());
			target.add("dc.format.extent:pageCount", String.valueOf(NDLDataUtils.getPDFPageCount(contents)));
			target.add("dc.format.extent:size_in_Bytes", String.valueOf(original.getSize()));
		}
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/JEE_advanced/in/JEE_ADVANCED-BKP-18.03.2019.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/JEE_advanced/logs";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/JEE_advanced/out";
		String name = "jee_advanced_v1";
		
		String titleFile = "/home/dspace/debasis/NDL/generated_xml_data/JEE_advanced/titles.update.csv";
		
		JEEAdvancedAIPCorrection t = new JEEAdvancedAIPCorrection(input, logLocation, outputLocation, name);
		t.turnOffLoadHierarchyFlag();
		t.turnOffFirstFailOnValidation();
		t.addMappingResource(titleFile, "ID", "title");
		t.correctData();
		
		System.out.println("Done.");
	}
}