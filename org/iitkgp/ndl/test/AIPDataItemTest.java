package org.iitkgp.ndl.test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.AIPFileGroup;
import org.iitkgp.ndl.data.AIPPermissionDetail;
import org.iitkgp.ndl.data.NDLAssetType;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.junit.Test;
import org.xml.sax.SAXException;

/**
 * Test cases of {@link AIPDataItem}
 * @author Aurghya, Debasis
 */
public class AIPDataItemTest {
	
	String itemFile = "/data/sample-AIP-data/ITEM@123456789-44328.zip";
	
	// loads AIP item
	AIPDataItem load(String itemFile) throws IOException, SAXException {
		File file = new File(NDLDataUtils.getResourcePath(itemFile));
		AIPDataItem item = new AIPDataItem();
		Map<String, byte[]> contents = new HashMap<String, byte[]>(2);
		contents.put(file.getName(), IOUtils.toByteArray(new FileInputStream(file)));
		item.load(contents);
		return item;
	}
	
	/**
	 * Tests handle ID
	 * @throws Exception throws error if data loading error occurs
	 */
	@Test
	public void getIDTest() throws Exception {
		AIPDataItem item = load(itemFile);
		assertEquals("123456789/44328", item.getId());
		assertEquals(true, item.isItem());
	}
	
	/**
	 * Tests item existing values 
	 * @throws Exception throws error if data loading error occurs
	 */
	@Test
	public void getAllValuesTest() throws Exception {
		AIPDataItem item = load(itemFile);
		Map<String, Collection<String>> values = item.getAllValues();
		for(String key : values.keySet()) {
			Collection<String> data = values.get(key);
			System.out.println(key);
			System.out.println(data);
		}
	}
	
	/**
	 * Tests AIP file group
	 * @throws Exception throws error in case of error during testing
	 */
	@Test
	public void testFileGroups() throws Exception {
		String testFile = "/data/ITEM@12345678_rjsthnbrd-15171.zip";
		AIPDataItem item = load(testFile);
		AIPFileGroup detail = item.getAIPFileGroupDetail(NDLAssetType.ORIGINAL);
		System.out.println(detail.getSize());
	}
	
	// permission block testing
	@Test
	public void testPermission() throws Exception {
		String testFile = "/data/sreechitra.zip";
		AIPDataItem item = load(testFile);
		assertEquals(item.hasPermissionBlocks(), true);
		
		AIPPermissionDetail detail = item.getPermissionDetail().get(0);
		System.out.println(detail.getGroupName());
		System.out.println(detail.getType());
		System.out.println(detail.getMembers());
		
		assertEquals(item.removePermissionBlock(), true);
		assertEquals(item.hasPermissionBlocks(), false);
	}
	
	// PDF age count test
	@Test
	public void pdfPageCountTest() throws Exception {
		String aipfile = "/data/asset-AIP-data/ITEM@123456789-8825.zip";
		AIPDataItem aip = load(aipfile);
		
		AIPFileGroup original = aip.getAIPFileGroupDetail(NDLAssetType.ORIGINAL);
		byte[] contents = aip.getBitstreamContentsByName(original.getName());
		System.out.println("Page count: " + NDLDataUtils.getPDFPageCount(contents));
	}
}