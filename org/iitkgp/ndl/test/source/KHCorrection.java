package org.iitkgp.ndl.test.source;

import java.util.List;
import java.util.Map;

import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class KHCorrection extends NDLSIPCorrectionContainer {

	Map<String, String> subjectsmap;
	Map<String, String> namesmap;

	int c1 = 0, c2 = 0;

	public KHCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}

	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {

		if (!target.exists("dc.identifier.uri")) {
			// not having URI delete
			return false;
		}

		// missing casebench
		if (!target.exists("dc.identifier.other:lawCaseBench")) {
			target.updateSingleValue("dc.identifier.other:lawCaseBench", "Bangalore");
		}

		delete("dc.title.alternative");

		// corrections
		deleteIfContains("ndl.sourceMeta.additionalInfo", "{\"DocumentType\":\"Fresh\"}");
		target.replaceByRegex("dc.description.abstract", ",", ";");
		move("dc.description.abstract", "ndl.sourceMeta.additionalInfo:references");
		
		target.replace("dc.identifier.other:lawCaseBench", "Bangalore", "Bengaluru", true);
		target.replace("dc.identifier.other:lawCaseBench", "Bengaulru", "Bengaluru", true);
		
		target.replaceByRegex("dc.identifier.other:accessionNo", ",", ";");
		move("dc.identifier.other:accessionNo", "ndl.sourceMeta.additionalInfo:references");
		
		/*
		 * target.replaceByRegex("dc.date.other:lawCaseYear", "^0", "");
		 * target.replace("dc.date.other:lawCaseYear", "1191", "1991");
		 */
		target.replaceByRegex("dc.title", "(^(High Court of Karnataka: *))|( +\\(.+\\)$)", "");

		String id = target.getId();

		// id specific
		if (id.equals("karnatakajudiciary/judgments_handle_123456789_854323")) {
			target.updateSingleValue("dc.date.other:lawCaseYear", "2012");
			target.updateSingleValue("dc.title", "CRL.P 5020/2012");
		} else if (id.equals("karnatakajudiciary/judgments_handle_123456789_710044")) {
			target.updateSingleValue("dc.date.other:lawCaseYear", "1991");
			target.updateSingleValue("dc.title", "MFA 2201/1991");
		} else if (id.equals("karnatakajudiciary/judgments_handle_123456789_670435")) {
			target.updateSingleValue("dc.date.other:lawJudgmentDate", "2011-09-30");
		}

		// transforms
		c1 += transformFieldByExactMatch("dc.subject", subjectsmap);
		// c2 += transformFieldByExactMatch("dc.contributor.other", namesmap);
		List<NDLDataNode> onodes = target.getNodes("dc.contributor.other");
		for (NDLDataNode node : onodes) {
			String[] odata = NDLDataUtils.getJSONKeyedValue(node.getTextContent(), false);
			if (namesmap.containsKey(odata[1])) {
				node.setTextContent(NDLDataUtils.getJson(odata[0], namesmap.get(odata[1])));
				c2++;
			}
		}
		
		target.updateSingleValue("dc.description.searchVisibility", "true");
		target.updateSingleValue("dc.rights.accessRights", "open");

		return true;

	}

	// main function
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/KH/in/2019.Sep.17.10.54.52.Karnatak_Output_SIP_9.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/KH/out"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/KH/out";
		String name = "KH.v10";

		String sfile = "/home/dspace/debasis/NDL/NDL_sources/KH/conf/subjectsm.csv";
		String nfile = "/home/dspace/debasis/NDL/NDL_sources/KH/conf/names.csv";

		KHCorrection p = new KHCorrection(input, logLocation, outputLocation, name);
		p.subjectsmap = NDLDataUtils.loadKeyValue(sfile);
		p.namesmap = NDLDataUtils.loadKeyValue(nfile);
		p.processData();

		System.out.println("C1: " + p.c1 + " C2: " + p.c2);

		System.out.println("Done.");
	}
}