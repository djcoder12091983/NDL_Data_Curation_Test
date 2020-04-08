package org.iitkgp.ndl.data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.asset.AssetDetail;
import org.iitkgp.ndl.data.exception.NDLMultivaluedException;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.iitkgp.ndl.util.NDLXMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * AIP data item detail encapsulation
 * @see SIPDataItem
 * @see AbstractNDLDataItem
 * @see NDLDataItem
 * @author Debasis, Aurghya, Vishal
 */
public class AIPDataItem extends AbstractNDLDataItem {
	
	static final String PERMISSION_XPATH = "//amdSec[@ID='amd_3']/techMD[@ID='techMD_5']";
	
	String fileName = null;
	Document document;
	AIPItemContents aipContents = null;
	boolean itemFlag = false;
	boolean collectionFlag = false;
	String parentID;
	List<String> childHandles = new LinkedList<String>(); // child id(s)
	long size;
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setFolder(String folder) {
		throw new UnsupportedOperationException("AIP folder can't be set");
	}
	
	// get child entries
	NodeList getChildNodes() throws XPathExpressionException {
		String childrenXPath = "//div[@TYPE='DSpace Object Contents']//mptr[@LOCTYPE='HANDLE']";
		return NDLXMLUtils.getNodeList(document, childrenXPath);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void load(Map<String, byte[]> files, boolean assetLoadingFlag) throws IOException, SAXException {
		fileName = files.keySet().iterator().next();
		String name = fileName.substring(fileName.lastIndexOf('/')+1);
		long size = 0;
		if(name.startsWith(NDLDataUtils.AIP_ITEM_FILE_PREFIX)) {
			// an item
			itemFlag = true;
		} else if (name.startsWith(NDLDataUtils.AIP_COLLECTION_FILE_PREFIX)
				|| name.startsWith(NDLDataUtils.AIP_COMMUNITY_FILE_PREFIX)) {
			// a collection
			collectionFlag = true;
		}
		byte[] data = files.get(fileName); // only one item
		size += data.length;
		try {
			aipContents = NDLDataUtils.loadDocument4AIP(data, assetLoadingFlag);
			size += aipContents.size(); // tentative size in bytes
		} catch(Exception ex) {
			// DEBUG purpose
			// TODO remove
			System.err.println("Name: " + fileName);
			throw ex;
		}
		document = aipContents.getDocument();
		if(document == null || (itemFlag && !NDLDataUtils.validateAIPItem(document))) {
			// AIP item validation
			throw new IOException("Invalid AIP item: " + fileName);
		}
		NodeList nodelist = document.getElementsByTagName("mets");
		Element e = (Element) nodelist.item(0);
		id = e.getAttribute("OBJID").substring(4); // item ID, removes `hdl:`
		try {
			if(itemFlag || collectionFlag) {
				// parent can be found
				String partOfEXP = "//*[@mdschema='dc'][@element='relation'][@qualifier='isPartOf']";
				String idText = NDLXMLUtils.getNodeList(document, partOfEXP).item(0).getTextContent();
				parentID = idText.substring(4); // parent ID
			}
			// populate children
			NodeList childNodes = getChildNodes();
			int l = childNodes.getLength();
			for(int i = 0; i < l; i++) {
				Node childNode = childNodes.item(i);
				// item ID, removes `hdl:`
				String childHandle = NDLXMLUtils.getAttributeValue(childNode, "xlink:href").substring(4);
				childHandles.add(childHandle);
			}
		} catch(XPathExpressionException ex) {
			// error
			throw new IOException(ex.getMessage(), ex.getCause());
		}
	}
	
	/**
	 * Whether an entry is item, for more details see AIP documentation
	 * @return returns true if it's an item otherwise false
	 */
	public boolean isItem() {
		return itemFlag;
	}
	
	/**
	 * Whether an entry is collection, for more details see AIP documentation
	 * @return returns true if it's a collection otherwise false
	 */
	public boolean isCollection() {
		return collectionFlag;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isParentItem() {
		return super.isParentItem() || collectionFlag;
	}
	
	/**
	 * Returns parent's handle ID, see AIP documentation for more details
	 * @return returns parent's handle ID
	 */
	public String getParentId() {
		return parentID;
	}
	
	/**
	 * Gets child handles if exists
	 * @return child handle list
	 */
	public List<String> getChildHandles() {
		return childHandles;
	}
	
	/**
	 * Returns whether permission block exists or not
	 * @return returns true if permission block exists otherwise false
	 * @throws XPathExpressionException throws error if any processing error happens
	 * @see #getPermissionDetail()
	 */
	public boolean hasPermissionBlocks() throws XPathExpressionException {
		return NDLXMLUtils.getNodeList(document, PERMISSION_XPATH).getLength() > 0;
	}
	
	/**
	 * Gets permission detail if any, otherwise NULL
	 * @return returns permission detail if found otherwise NULL
	 * @throws XPathExpressionException throws error if any processing error happens
	 */
	public List<AIPPermissionDetail> getPermissionDetail() throws XPathExpressionException {
		// permission details
		List<AIPPermissionDetail> details = new LinkedList<AIPPermissionDetail>();
		
		NodeList nodes = NDLXMLUtils.getNodeList(document, PERMISSION_XPATH);
		int l = nodes.getLength();
		if(l == 0) {
			// not found
			return details;
		}
		Node node = nodes.item(0);
		NodeList groups = NDLXMLUtils.getNodeList(node, "//Groups/Group");
		l = groups.getLength();
		for(int i = 0; i < l; i++) {
			Element group = (Element)groups.item(i);
			String name = group.getAttribute("Name");
			String type = group.getAttribute("Type");
			// add details
			AIPPermissionDetail detail = new AIPPermissionDetail(name, type);
			details.add(detail);
			
			NodeList members = NDLXMLUtils.getNodeList(group, "//Members/Member");
			int l1 = groups.getLength();
			for(int j = 0; j < l1; j++) {
				Element member = (Element)members.item(j);
				detail.addMember(member.getAttribute("Name"));
			}
		}
		
		return details;
	}
	
	/**
	 * Removes permission block if exists
	 * @return returns true if permission found and deleted successfully otherwise false
	 * @throws XPathExpressionException throws error if any processing error happens
	 */
	public boolean removePermissionBlock() throws XPathExpressionException {
		NodeList nodes = NDLXMLUtils.getNodeList(document, PERMISSION_XPATH);
		int l = nodes.getLength();
		if(l == 0) {
			// not found
			return false;
		}
		Node node = nodes.item(0);
		// remove block
		node.getParentNode().removeChild(node);
		return true; // successfully deleted
	}
	
	/**
	 * Removes child entry if exists
	 * @param childHandleSuffixes child handle suffix data to remove
	 * @return returns how many entry deleted
	 * @throws Exception throws exception in case of errors
	 */
	public int removeChildEntries(Collection<String> childHandleSuffixes) throws Exception {
		NodeList childNodes = getChildNodes();
		int l = childNodes.getLength();
		int c = 0;
		for(int i = 0; i < l; i++) {
			Node childNode = childNodes.item(i);
			String childHandle = NDLXMLUtils.getAttributeValue(childNode, "xlink:href").substring(4);
			if(childHandleSuffixes.contains(childHandle)) {
				// successfully delete
				Node delete = childNode.getParentNode();
				delete.getParentNode().removeChild(delete);
				c++;
			}
		}
		return c;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int add(String field, Collection<String> values) {
		if(values == null) {
			// blank case
			return 0;
		}
		// get entry point
		NDLField ndlField = new NDLField(field);
		String entryEXP = NDLDataUtils.getAIPEntryXPath();
		Node entry = null;
		Document document = aipContents.getDocument();
		try {
			entry = NDLXMLUtils.getSingleNode(document, entryEXP);
		} catch(Exception ex) {
			// error
			System.out.println(ex.getMessage()); // error message
			throw new IllegalStateException(ex.getMessage(), ex);
		}
		// create node with attributes
		int c = 0;
		boolean numeric = NDLDataUtils.isNumericJSONKeyedField(field);
		for(String value : values) {
			// iterate values
			if(StringUtils.isBlank(value)) {
				// skip blank value
				continue;
			}
			
			// remove control characters
			value = NDLDataUtils.removeControlCharaters(value).trim();
			
			Element newNode = document.createElement("dim:field");
			newNode.setAttribute("mdschema", ndlField.schema);
			newNode.setAttribute("element", ndlField.element);
			if (ndlField.hasQualifier()) {
				newNode.setAttribute("qualifier", ndlField.qualifier);
			}
			String modifiedValue = null;
			if(ndlField.hasJsonKey()) {
				modifiedValue = NDLDataUtils.getJson(ndlField.jsonKey, numeric ? Long.parseLong(value) : value);
			} else {
				modifiedValue = value;
			}
			newNode.setTextContent(modifiedValue); // add value
			c++;
			
			// add node
			entry.appendChild(newNode);
		}
		return c;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void addAsset(String name, NDLAssetType type, InputStream contents) {
		throw new UnsupportedOperationException("Asset Operation is unknown for AIP");
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean removeAsset(String name, NDLAssetType type) throws IOException {
		throw new UnsupportedOperationException("Asset Operation is unknown for AIP");
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<NDLDataNode> getNodes(String field) {
		NDLField ndlField = new NDLField(field);
		String xPath = NDLDataUtils.getXPath4AIP(ndlField);
		NodeList xmlnodes = null;
		try{
			xmlnodes = NDLXMLUtils.getNodeList(aipContents.getDocument(), xPath);
			if(StringUtils.isNotBlank(ndlField.jsonKey)) {
				// json-key available
				xmlnodes = getNodesByJsonKey(xmlnodes, ndlField.jsonKey);
			}
		} catch(Exception ex) {
			// error handling
			System.out.println(ex.getMessage());
			throw new IllegalStateException(ex.getMessage(), ex);
		}
		return NDLXMLUtils.getNDLNodeList(xmlnodes);
	}
	
	/**
	 * Gets other file names
	 * @return returns other file names
	 */
	public Set<String> getOtherFileNames() {
		return aipContents.getOthers().keySet();
	}

	/**
	 * Removes other file by name
	 * @param name given name
	 * @return returns true if removed successfully
	 */
	public boolean removeOtherFile(String name) {
		return aipContents.getOthers().remove(name) != null;
	}
	
	/**
	 * It returns bitstream file names
	 * @return returns bitstream file names
	 */
	public Set<String> getBitstreamFiles() {
		Set<String> names = new HashSet<String>(2);
		Set<String> keys = aipContents.getOthers().keySet();
		for(String key : keys) {
			if(key.startsWith(NDLDataUtils.AIP_BITSTREAM_FILE_PREFIX)) {
				names.add(key);
			}
		}
		return names;
	}
	
	/**
	 * Gets bitstream contents by name, typically used when multiple bitstream exists
	 * @param name given name
	 * @return returns contents if exists otherwise returns NULL
	 */
	public byte[] getBitstreamContentsByName(String name) {
		Set<String> names = getBitstreamFiles();
		if(names.contains(name)) {
			return aipContents.getOthers().get(name);
		} else {
			return null;
		}
	}
	
	/**
	 * Gets bitstream contents if single exists
	 * @return returns contents if single exists otherwise throws exception in case multiple
	 *         but returns NULL if not exists 
	 */
	public byte[] getBitstreamContents() {
		Set<String> names = getBitstreamFiles();
		if(names.isEmpty()) {
			// does not exist
			return null;
		} else if(names.size() > 1) {
			throw new NDLMultivaluedException("Bitstream exists multiple times");
		} else {
			// single contents
			return aipContents.getOthers().get(names.iterator().next());
		}
	}
	
	/**
	 * Gets bitstream contents by extension, typically used when multiple bitstream exists
	 * @param extension given extension
	 * @return returns contents if exists otherwise returns NULL
	 */
	public byte[] getBitstreamContentsByExtension(String extension) {
		Set<String> names = getBitstreamFiles();
		for(String name : names) {
			if(name.endsWith(extension)) {
				// found
				return aipContents.getOthers().get(name);
			}
		}
		return null;
	}
	
	/**
	 * Gets other contents by name
	 * @param name given name
	 * @return returns contents if exists otherwise returns NULL
	 */
	public byte[] getContents(String name) {
		return aipContents.getOthers().get(name);
	}
	
	/**
	 * Returns whether multiple bitstreams exists
	 * @return returns true if so otherwise false
	 */
	public boolean isBitstreamMultivalued() {
		return getBitstreamFiles().size() > 1;
	}
	
	/**
	 * Checks whether bitstream exists or not
	 * @return returns true if bitstream exists or not otehrwise false
	 */
	public boolean bitstreamExists() {
		return !getBitstreamFiles().isEmpty();
	}
	
	/**
	 * Checks whether original bitstream exists or not
	 * @return returns true if original bitstream exists or not otehrwise false
	 */
	public boolean originalBitstreamExists() {
		return getAIPFileGroupDetails(NDLAssetType.ORIGINAL) != null;
	}
	
	/**
	 * Gets AIP file group detail by file type
	 * @param type given file type
	 * @return returns AIP group file details if exists otherwise NULL
	 */
	public List<AIPFileGroup> getAIPFileGroupDetails(String type) {
		return aipContents.getFileGroupDetails(type);
	}
	
	/**
	 * Gets AIP file group detail by file type
	 * @param type given file type
	 * @return returns AIP group file details if exists otherwise NULL
	 */
	public List<AIPFileGroup> getAIPFileGroupDetails(NDLAssetType type) {
		return aipContents.getFileGroupDetails(type.getType());
	}
	
	/**
	 * Gets AIP file group detail by file type
	 * @param type given file type
	 * @return returns AIP group file detail if exists otherwise NULL
	 * @throws NDLMultivaluedException throws error if multiple value found
	 */
	public AIPFileGroup getAIPFileGroupDetail(String type) {
		return aipContents.getFileGroupDetail(type);
		
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public AssetDetail readAsset(NDLAssetType type, String name) throws IOException {
		throw new UnsupportedOperationException("Asset Operation is unknown for AIP");
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<AssetDetail> readAllAssets() throws IOException {
		throw new UnsupportedOperationException("Asset Operation is unknown for AIP");
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<AssetDetail> readAllAssets(boolean loadContents) throws IOException {
		throw new UnsupportedOperationException("Asset Operation is unknown for AIP");
	}
	
	/**
	 * Gets AIP file group detail by file type
	 * @param type given file type
	 * @return returns AIP group file detail if exists otherwise NULL
	 * @throws NDLMultivaluedException throws error if multiple value found
	 */
	public AIPFileGroup getAIPFileGroupDetail(NDLAssetType type) {
		return aipContents.getFileGroupDetail(type.getType());
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Map<String, byte[]> getContents() throws IOException, TransformerException {
		Map<String, byte[]> contents = new HashMap<String, byte[]>(2);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(stream);
        // mets entry
        byte[] mets = NDLDataUtils.getXMLContents(aipContents.getDocument());
        ZipEntry ze = new ZipEntry(NDLDataUtils.METS_XML_FILE);
        zos.putNextEntry(ze);
        zos.write(mets);
        zos.closeEntry();
        // others entry
        Map<String, byte[]> others = aipContents.getOthers();
        for(String entry : others.keySet()) {
            ze = new ZipEntry(entry);
            zos.putNextEntry(ze);
            zos.write(others.get(entry));
            zos.closeEntry();
        }
        // write
        zos.close(); // closes the stream
        byte[] bytes = stream.toByteArray();
        contents.put(fileName, bytes);
        return contents;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<String> getAllFields(Set<String> excludes) {
		if(!itemFlag) {
			// not an item, return empty data
			return new HashSet<String>(2);
		}
		Set<String> fields = new HashSet<String>();
		Document document = aipContents.getDocument();
		String dataXPath = "//dmdSec[@ID='dmdSec_2']/mdWrap/xmlData/*[@dspaceType='ITEM']/*";
		try {
			NodeList nodes = NDLXMLUtils.getNodeList(document, dataXPath);
			int l = nodes.getLength();
			for(int i = 0; i < l; i++) {
				Element node = (Element)nodes.item(i);
				StringBuilder key = new StringBuilder(node.getAttribute("mdschema"));
				key.append(".").append(node.getAttribute("element"));
				if(node.hasAttribute("qualifier")) {
					String q = node.getAttribute("qualifier");
					if(!StringUtils.equalsIgnoreCase(q, "none")) {
						// qualifier found
						key.append(".").append(q);
					}
				}
				String k = key.toString();
				if(excludes.contains(k)) {
					// skip
					continue;
				}
				fields.add(k);
			}
		} catch(Exception ex) {
			System.err.println("ERROR: " + ex.getMessage());
		}
		return fields;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public long size() {
		// TODO Auto-generated method stub
		return 0;
	}
}