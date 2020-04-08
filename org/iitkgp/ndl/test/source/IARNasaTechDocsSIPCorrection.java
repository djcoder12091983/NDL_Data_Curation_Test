package org.iitkgp.ndl.test.source;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

// IAR nasa tech docs data curation
public class IARNasaTechDocsSIPCorrection extends NDLSIPCorrectionContainer {
	
	Set<String> wrongDescriptions;
    Map<String, String> titleLRTMapping;
    Map<String, String> authorMapping = new HashMap<String, String>();
	
	// constructor
	public IARNasaTechDocsSIPCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	  // loads configuration
    void load(String wrongDescMappingFile, String titleLRTMappingFile) throws Exception {
        wrongDescriptions = NDLDataUtils.loadSet(wrongDescMappingFile);
        titleLRTMapping = NDLDataUtils.loadKeyValue(titleLRTMappingFile);
    }

    // loads author
    void loadAuthors(String authorLocation) throws Exception {
        for (File file : new File(authorLocation).listFiles()) {
            Map<String, String> authors = NDLDataUtils.loadKeyValue(file);
            authorMapping.putAll(authors);
        }
    }
	
    // correction logic
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		target.replace("dc.language.iso", "English", "eng");
        deleteIfContains("dc.contributor.other:studyGroup", "Other Sources");
        target.replace("dc.contributor.other:organization", "NASA",
                "The National Aeronautics and Space Administration (NASA)");
        target.replace("dc.contributor.other:studyGroup", "CASI",
                "The Climate Adaptation Science Investigators (CASI)");

        deleteIfContains("dc.description", wrongDescriptions);
        target.replace("dc.type", "texts", "text");
        target.move("dc.date.other:publisher", "dc.publisher.date");
        normalize("dc.publisher.date");
        add("lrmi.educationalAlignment.educationalLevel", "ug_pg", "career_tech");

        // title LRT mapping
        String title = target.getValue("dc.title").get(0); // TODO handle multiple value
        Set<String> lrts = new HashSet<String>(2);
        for (String key : titleLRTMapping.keySet()) {
            if (title.contains(key)) {
                lrts.add(titleLRTMapping.get(key));
            }
        }
        target.add("lrmi.learningResourceType", lrts);
       
        // author correction
        transformFieldByExactMatch("dc.contributor.author", authorMapping);
		
		return true;
	}
	
	// testing
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/IAR/raw_data/nasa-techdocs/in/Nasa.Techdocs.v3.11.Jul.2018.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/IAR/raw_data/nasa-techdocs/logs";
		String outputLocation = "/home/dspace/debasis/NDL/IAR/raw_data/nasa-techdocs/out";
		String name = "NASA.techdocs.v4";
		
		String wrongDescMappingFile = "/home/dspace/debasis/NDL/IAR/raw_data/nasa-techdocs/conf/wrong.desc.list";
        String titleLRTMappingFile = "/home/dspace/debasis/NDL/IAR/raw_data/nasa-techdocs/conf/title.lrt.mapping.csv";
        String authorLocation = "/home/dspace/debasis/NDL/IAR/raw_data/nasa-techdocs/conf/authors";
		
		System.out.println("Start.");
		
		IARNasaTechDocsSIPCorrection p = new IARNasaTechDocsSIPCorrection(input, logLocation, outputLocation, name);
		p.load(wrongDescMappingFile, titleLRTMappingFile);
	    p.loadAuthors(authorLocation);
		
	    p.correctData();
		
		System.out.println("End.");
	}
}