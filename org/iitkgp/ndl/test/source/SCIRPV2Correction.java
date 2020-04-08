package org.iitkgp.ndl.test.source;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.NDLAssetType;
import org.iitkgp.ndl.data.NDLCitation;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.ConfigurationData;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class SCIRPV2Correction extends NDLSIPCorrectionContainer {
	
	Map<String, String> ddcMap = new HashMap<String, String>();
	NDLDataNormalizer nameNormalizer = new NDLDataNormalizer() {

		// normalize
		@Override
		public Collection<String> transform(String input) {
			//String original = input;
			input = input.replace("&nbsp", "");
			String modified = NDLDataUtils.normalizeSimpleName(input);
			/*try {
				log("author", new String[]{getCurrentTargetItem().getId(), original, modified});
			} catch(Exception ex) {
				// suppress error
			}*/
			return NDLDataUtils.createNewList(modified);
		}
	};
	
	// construction
	public SCIRPV2Correction(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	void loadConfiguration(String ddcFile) throws Exception {
		ddcMap = NDLDataUtils.loadKeyValue(ddcFile);
	}
	
	// correction
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		delete("dc.identifier.other:part", "dc.publisher", "dc.identifier.other:accessionNo", "dc.description.uri",
				"dc.relation.requires", "dc.publisher.institution", "dc.identifier.other:alternateWebpageUri",
				"ndl.sourceMeta.additionalInfo:note", "dc.date.copyright");
		
		target.replace("dc.language.iso", "en", "eng");
		target.deleteDuplicateFieldValues("dc.title", "dc.title.alternative");
		target.deleteDuplicateFieldValues("dc.description.abstract", "dc.description",
				"lrmi.educationalAlignment.educationalFramework");
		target.deleteDuplicateFieldValues("dc.publisher.date", "dc.date.created", "dc.date.issued", "dc.date.awarded");
		transformFieldByExactMatch("lrmi.learningResourceType", "lrt");
		String title = target.getSingleValue("dc.title");
		if(StringUtils.containsIgnoreCase(title, "book review")) {
			target.updateSingleValue("lrmi.learningResourceType", "BookReview");
		}
		boolean item = target.isChildItem();
		if(item) {
			target.add("dc.rights.accessRights", "open");
			target.add("dc.type", "text");
			target.add("dc.description.searchVisibility", "true");
			target.add("dc.format.mimetype", "application/pdf");
			String uri = target.getSingleValue("dc.identifier.uri");
			uri = uri.replaceFirst("https:/", "http:/");
			target.updateSingleValue("dc.identifier.uri", uri);
		}
		if(!target.exists("dc.subject")) {
			target.move("dc.coverage.temporal", "dc.subject");
		}
		target.delete("dc.coverage.temporal");
		// DDC
		transformFieldByPartialMatch("dc.subject.ddc", "dc.coverage.spatial", ddcMap, ';');
		
		// keywords
		// merge
		mergeKeywrods(target);
		String spatial = target.getSingleValue("dc.coverage.spatial");
		if(StringUtils.isNotBlank(spatial)) {
			target.delete("dc.coverage.spatial");
			spatial = spatial.substring(spatial.indexOf('/') + 1, spatial.lastIndexOf('/'));
			target.add("dc.subject", spatial.split("/"));
		}
		split("dc.subject", ';');
		target.removeDuplicate("dc.subject");
		
		target.removeDuplicate("ndl.sourceMeta.additionalInfo:references");
		target.removeDuplicate("ndl.sourceMeta.additionalInfo:relatedContentUrl");

		normalize("dc.identifier.issn", "dc.identifier.other:eissn");
		target.delete("dc.contributor.editor");
		removeMultipleSpaces("dc.contributor.author", "dc.contributor.advisor");
		split("dc.contributor.advisor", ';');
		normalizeByOwn("dc.contributor.advisor", nameNormalizer);
		normalizeByOwn("dc.contributor.author", nameNormalizer);
		target.move("dc.contributor.advisor", "dc.contributor.author");
		
		// pages from citation
		updatePages(target);
		
		// citation
		addCitation(target);
		
		// special case
		moveIfContains("ndl.sourceMeta.additionalInfo:relatedContentUrl",
				"ndl.sourceMeta.additionalInfo:RightsStatement",
				"(c) 2015 by author(s) and Scientific Research Publishing Inc.");
		
		return true;
	}
	
	void mergeKeywrods(SIPDataItem target) {
		List<NDLDataNode> nodes = target.getNodes("dc.subject");
		List<String> t = new LinkedList<String>();
		for(NDLDataNode node : nodes) {
			String value = StringEscapeUtils.unescapeHtml4(node.getTextContent());
			if(NumberUtils.isDigits(value)) {
				node.remove();
				t.add(value);
			} else {
				// non-numeric, merge
				if(!t.isEmpty()) {
					t.add(value);
					node.setTextContent(NDLDataUtils.join(t, ','));
					t.clear(); // reset
				}
			}
		}
	}
	
	// citation
	void updatePages(SIPDataItem target) {
		String citation = target.getSingleValue("dc.identifier.citation");
		if(StringUtils.isNotBlank(citation)) {
			StringTokenizer tokens = new StringTokenizer(citation);
			String pages = null;
			while(tokens.hasMoreTokens()) {
				String t = tokens.nextToken();
				if(t.matches("^[0-9]+-[0-9]+\\.$")) {
					// latest
					pages = t;
				}
			}
			if(StringUtils.isNotBlank(pages)) {
				tokens = new StringTokenizer(pages, "-");
				target.updateSingleValue("dc.format.extent:startingPage", tokens.nextToken());
				target.updateSingleValue("dc.format.extent:endingPage", tokens.nextToken());
			}
		}
	}
	
	void addCitation(SIPDataItem target) {
		if(target.isParentItem()) {
			// skip for parent items
			return;
		}
		String uri = target.getSingleValue("dc.identifier.uri");
		NDLCitation citation = new NDLCitation("article", "article" + uri.substring(uri.lastIndexOf('=') + 1));
		citation.addDetail("title", target.getSingleValue("dc.title"));
		citation.addDetail("author", target.getValue("dc.contributor.author"));
		citation.addDetail("journal", target.getSingleValue("dc.identifier.other:journal"));
		citation.addDetail("volume", target.getSingleValue("dc.identifier.other:volume"));
		citation.addDetail("number", target.getSingleValue("dc.identifier.other:issue"));
		String s = target.getSingleValue("dc.format.extent:startingPage");
		String e = target.getSingleValue("dc.format.extent:endingPage");
		if(NDLDataUtils.allNotBlank(s, e)) {
			citation.addDetail("pages", s + '-' + e);
		}
		citation.addDetail("year", target.getSingleValue("dc.publisher.date").substring(0, 4));
		target.updateSingleValue("dc.identifier.citation", citation.getCitationText());
	}
	
	// asset id lookup
	@Override
	protected String getAssetID(SIPDataItem item) {
		boolean j = item.contains("lrmi.learningResourceType", "journal");
		if(j) {
			// journal
			return getMappingKey("journals." + ConfigurationData.escapeDot(item.getSingleValue("dc.title"))); 
		}
		String jname = item.getSingleValue("dc.identifier.other:journal");
		if(StringUtils.isNotBlank(jname) && item.contains("dc.description.searchVisibility", "true")) {
			// article level
			return getMappingKey("journals." + ConfigurationData.escapeDot(jname));
		}
		return null;
	}
	
	// correct
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/generated_xml_data/SCRIP/out/2018.Oct.31.11.41.51.SCIRP.V1/2018.Oct.31.11.41.51.SCIRP.V1.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/SCRIP/logs";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/SCRIP/out";
		String name = "SCIRP.V2";
		
		String lrtFile = "/home/dspace/debasis/NDL/generated_xml_data/SCRIP/conf/lrt.csv";
		String ddcFile = "/home/dspace/debasis/NDL/generated_xml_data/SCRIP/conf/ddc.csv";
		String thumbnailLocation = "/home/dspace/debasis/NDL/generated_xml_data/SCRIP/thumbnails_assets";
		String journalFile = "/home/dspace/debasis/NDL/generated_xml_data/SCRIP/conf/journals.csv";
		
		SCIRPV2Correction p = new SCIRPV2Correction(input, logLocation, outputLocation, name);
		//p.addCSVLogger("author", new String[]{"ID", "Old", "New"});
		p.turnOffLoadHierarchyFlag(); // no need to load hierarchy
		p.addMappingResource(lrtFile, "New", "lrt");
		p.addMappingResource(journalFile, "Full", "journals");
		p.addAssetLocation(NDLAssetType.THUMBNAIL, thumbnailLocation);
		p.loadConfiguration(ddcFile);
		p.correctData();
		
		System.out.println("Done.");
	}
}