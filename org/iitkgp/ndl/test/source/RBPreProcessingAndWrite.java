package org.iitkgp.ndl.test.source;

import java.io.File;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.data.NDLAssetType;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVReader;

public class RBPreProcessingAndWrite extends NDLSIPCorrectionContainer {
	
	static final String HANDLE_PREFIX = "12345678_rjsthnbrd/";
	
	Map<String, String> titles;
	String assetLocation;
	String vnodesFile;
	
	int ci = 0;
	int oa = 0;
	int tu = 0;

	public RBPreProcessingAndWrite(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		// title update
		String id = NDLDataUtils.getHandleSuffixID(target.getId());
		if(titles.containsKey(id)) {
			target.updateSingleValue("dc.title", titles.get(id));
			tu++;
		}
		
		String title = target.getSingleValue("dc.title");
		if(CharUtils.isAsciiAlphaLower(title.charAt(0))) {
			target.updateSingleValue("dc.title", title.substring(0, 1).toUpperCase() + title.substring(1));
			tu++;
		}
		
		// handle assets
		String asset = asset(target, "ndl.sourceMeta.additionalInfo:relatedContentUrl", "dc.identifier.uri");
		if(StringUtils.isNotBlank(asset)) {
			// related content uri
			File oasset = new File(assetLocation, asset);
			if(oasset.exists()) {
				// asset available
				target.addAsset(asset, NDLAssetType.ORIGINAL, FileUtils.readFileToByteArray(oasset));
				oa++;
			} else {
				// cross check
				System.err.println("[" + id + "]Original Asset not found: " + oasset.getAbsolutePath());
			}
		}
		
		// hierarchy information set
		String k = "rb_child." + NDLDataUtils.getHandleSuffixID(target.getId());
		if(containsMappingKey(k)) {
			String p = getMappingKey(k + ".Parent");
			String o = getMappingKey(k + ".Order");
			target.updateSingleValue("dc.relation.ispartof", p);
			target.updateSingleValue("dc.relation", o);
			ci++;
		}
		
		return true;
	}
	
	String asset(SIPDataItem target, String f1, String f2) {
		String v1 = target.getSingleValue(f1);
		String v2 = target.getSingleValue(f2);
		if(StringUtils.isNotBlank(v1)) {
			return v1.substring(v1.lastIndexOf('/') + 1);
		} else if(StringUtils.isNotBlank(v2)) {
			return v2.substring(v2.lastIndexOf('/') + 1);
		} else {
			return null;
		}
	}
	
	@Override
	public void postProcessData() throws Exception {
		super.postProcessData(); // super call
		
		System.out.println("Virtual nodes creation.....");
		
		// create virtual nodes
		CSVReader vreader = NDLDataUtils.readCSV(new File(vnodesFile), 1);
		String tokens[];
		int c = 0;
		while((tokens = vreader.readNext()) != null) {
			SIPDataItem sip = NDLDataUtils.createBlankSIP(HANDLE_PREFIX + tokens[1]);
			sip.setFolder(tokens[5] + "/P_" + ++c);
			sip.add("dc.title", tokens[2]);
			sip.add("dc.relation.ispartof", tokens[0]);
			sip.add("dc.relation", tokens[6]);
			sip.add("lrmi.educationalAlignment.educationalLevel", tokens[3]);
			sip.add("dc.type", tokens[4]);
			writeItem(sip);
		}
		
		vreader.close();
		
		System.out.println("Virtual nodes creation done: " + c);
	}
	
	@Override
	protected void intermediateProcessHandler() {
		super.intermediateProcessHandler(); // super call
		
		System.out.println("Original assets: " + oa);
		System.out.println("Child hierarchy: " + ci);
		System.out.println("Titles update: " + tu);
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/RAJ/in/2019.Apr.29.11.46.38.curation.rjbrd.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/RAJ/logs";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/RAJ/output";
		String name = "rb.v2";
		
		String tlocation = "/home/dspace/debasis/NDL/NDL_sources/RAJ/assets/THUMBNAIL";
		String assetLocation = "/home/dspace/debasis/NDL/NDL_sources/RAJ/assets/original";
		String vnodesFile = "/home/dspace/debasis/NDL/NDL_sources/RAJ/conf/rb.virtual.nodes.csv";
		String cfile = "/home/dspace/debasis/NDL/NDL_sources/RAJ/conf/rb.child.nodes.csv";
		String ftitles = "/home/dspace/debasis/NDL/NDL_sources/RAJ/conf/titles.csv";
		
		NDLConfigurationContext.addConfiguration("compressed.data.process.buffer.size", "10");
		NDLConfigurationContext.addConfiguration("process.display.threshold.limit", "50");
		
		RBPreProcessingAndWrite p = new RBPreProcessingAndWrite(input, logLocation, outputLocation, name);
		//p.dontPreserveFolderStructure();
		p.titles = NDLDataUtils.loadKeyValue(ftitles);
		p.assetLocation = assetLocation;
		p.vnodesFile = vnodesFile;
		p.addMappingResource(cfile, "Child", "rb_child");
		p.addAssetLocation(NDLAssetType.THUMBNAIL, tlocation);
		p.turnOffLoadHierarchyFlag();
		p.processData();
		
		System.out.println("Done.");
	}
}