package org.iitkgp.ndl.data;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.transform.TransformerException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.asset.AssetDetail;
import org.iitkgp.ndl.data.exception.UnknownSchemaFileException;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.iitkgp.ndl.util.NDLXMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * SIP data item detail encapsulation
 * @see AIPDataItem
 * @see AbstractNDLDataItem
 * @see NDLDataItem
 * @author Debasis, Vishal, Aurghya
 */
public class SIPDataItem extends AbstractNDLDataItem {
	
	// bypass unknown schema file flag
	private static boolean BYPASS_UNKWNON_SCHEMA = Boolean
			.valueOf(NDLConfigurationContext.getConfiguration("sip.bypass.unknown.xml.file")); 
	
	Map<String, Document> documents = new HashMap<String, Document>(2); // documents mapping by logical name
	Map<String, Document> documentsByName = new HashMap<String, Document>(2); // documents mapping by full path name
	Map<String, byte[]> otherFiles = new HashMap<String, byte[]>(2);
	String handleFileEntry; // handle file entry
	long size; // size in bytes
	
	boolean parent= false; // parent
	
	/**
	 * Sets parent flag
	 * @param parent parent flag
	 */
	public void setParent(boolean parent) {
		this.parent = parent;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isParentItem() {
		return parent;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public long size() {
		return size;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void load(Map<String, byte[]> files, boolean assetLoadingFlag) throws IOException, SAXException {
		// multiple items
		Set<String> entries = files.keySet();
		size = 0;
        for(String entry : entries) {
        	int p = entry.lastIndexOf('/');
        	String location = null;
        	if(p != -1) {
        		location = entry.substring(0, p);
        	}
        	if(folder == null) {
        		// not assigned yet
        		folder = location;
        	} else {
        		// cross check whether entries have different location
        		if(!StringUtils.equals(location, folder)) {
        			// mismatch, report ERROR
        			throw new IllegalStateException("Location mismatch for entries");
        		}
        	}
        	
        	byte contents[] = files.get(entry);
        	size += contents.length; // size calculation
            
        	if(entry.endsWith(".xml")) {
        		//System.out.println(entry);
        		String schemaf = entry.substring(entry.lastIndexOf('/')+1);
                String schema = NDLDataUtils.getSchema4File(schemaf);
                boolean f = StringUtils.isBlank(schema);
                if(f) {
                	// unknown schema file
                	System.err.println("Unknown schema file: " + schemaf);
                }
                if(!f) {
                	// valid schema to handle
	                try {
		                Document document = NDLXMLUtils.parseDocument(contents);
		                documents.put(schema, document);
		                documentsByName.put(entry , document);
	                } catch(Exception ex) {
	                	// error
	                	System.err.println("ERROR: " + entry);
	                	// pass the error
	                	throw ex;
	                }
                } else if(!BYPASS_UNKWNON_SCHEMA) {
                	// throw exception
					throw new UnknownSchemaFileException("Unknown schema file: " + schemaf
							+ ". To avoid, ON(true) the `sip.bypass.unknown.xml.file` flag using `NDLConfigurationContext`.");
                }
            } else {
            	if(entry.endsWith(NDLDataUtils.HANDLE_FILE)) {
            		handleFileEntry = entry; // save entry name
            		// ID
            		id = IOUtils.toString(new ByteArrayInputStream(contents), "UTF-8");
            		// remove newline and dot removal
            		id = id.replaceAll("\\.", "-").replaceAll("\\r?\\n", "");
            	}
            	if(assetLoadingFlag) {
	            	// other than XML document
            		// if asset loading flag is enabled
	            	otherFiles.put(entry, contents);
            	} else {
            		// exclude assets size
            		size -= contents.length;
            	}
            }
        }
        
        if(StringUtils.isBlank(id)) {
        	// handle ID missing
        	String message = "Handle ID is missing: " + getFolder();
        	System.err.println(message); // log
        	throw new IOException(message); // error
        }
        
        // missing contents handle
        handleMissingFile();
        
        // set parent flag
        if(!parent) {
        	// parent flag not set manually then reply on dc.relation.haspart
        	setParent(exists("dc.relation.haspart"));
        }
	}
	
	// handle missing file, then create blank entry for that
	void handleMissingFile() throws IOException, SAXException {
		// missing file handling
		for (String schema : NDLDataUtils.FILE2SCHEMA_SIP.values()) {
			Document doc = documents.get(schema);
			if (doc == null) {
				if(NDLDataUtils.getMissingSchemaDisplayFlag()) {
					// warning display flag
					System.err.println("Missing schema: " + schema + " for: " + folder);
				}
				// missing
				byte[] contents = ("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"no\"?><dublin_core schema=\""
						+ schema + "\"></dublin_core>").getBytes();
				Document newdoc = NDLXMLUtils.DOC_BUILDER.parse(new ByteArrayInputStream(contents));
				documents.put(schema, newdoc);
				documentsByName.put(folder + "/" + NDLDataUtils.SCHEMA2FILE_SIP.get(schema), newdoc);
			}
		}
		// missing contents file handling
		String contentsKey = getFullName(NDLDataUtils.CONTENTS_FILE);
        if(!otherFiles.containsKey(contentsKey)) {
        	otherFiles.put(contentsKey, "".getBytes());
        }
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setFolder(String folder) {
		// remove last slash
		this.folder = folder.endsWith("/") ? folder.substring(0, folder.length() - 1) : folder;
	}
	
	/**
	 * <pre>Sets handle ID</pre>
	 * <pre><b>Note: This method should be called by special care, typically handle ID should not be changed.</b></pre> 
	 * @param id handle ID
	 */
	public void setId(String id) {
		this.id = id;
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
		// make the field value unique
		List<String> existing = getValue(field);
		values.removeAll(existing); // remove already existing values
		
		boolean numeric = NDLDataUtils.isNumericJSONKeyedField(field);
		// get entry point
		String entryEXP = "dublin_core";
		Node entry = null;
		NDLField ndlField = new NDLField(field);
		Document document = documents.get(ndlField.schema);
		try {
			entry = NDLXMLUtils.getSingleNode(document, entryEXP);
		} catch(Exception ex) {
			// error
			System.out.println(ex.getMessage()); // error message
			throw new IllegalStateException(ex.getMessage(), ex);
		}
		// create node with attributes
		int c = 0;
		for(String value : values) {
			// iterate values
			if(StringUtils.isBlank(value)) {
				// skip blank value
				continue;
			}
			// leading and trailing space remove
			// remove control characters
			value = NDLDataUtils.removeControlCharaters(value).trim();
			
			Element newNode = document.createElement(NDLDataUtils.DCVALUE_TAG);
			newNode.setAttribute(NDLDataUtils.ELEMENT_TAG, ndlField.element);
			if (ndlField.hasQualifier()) {
				newNode.setAttribute(NDLDataUtils.QUALIFIER_TAG, ndlField.qualifier);
			}
			if(ndlField.hasJsonKey()) {
				value = NDLDataUtils.getJson(ndlField.jsonKey, numeric ? Long.parseLong(value) : value);
			}
			newNode.setTextContent(value); // add value
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
	public void addAsset(String name, NDLAssetType type, InputStream contents) throws IOException {
		String contentsKey = getFullName(NDLDataUtils.CONTENTS_FILE);
		String contentsFileText = new String(otherFiles.get(contentsKey)); //assumed always contents file exists
		if(!NDLDataUtils.containsAssetText(contentsFileText, name, type)) {
			// if not contains
			StringBuilder modifiedText = new StringBuilder();
			if(StringUtils.isNotBlank(contentsFileText)) {
				modifiedText.append(contentsFileText).append(NDLDataUtils.NEW_LINE);
			}
			modifiedText.append(name).append("\tbundle:").append(type.getType());
			otherFiles.put(contentsKey, modifiedText.toString().getBytes()); // sets new contents
		}
		// add new entry of asset
		String assetKey = getFullName(name);
		otherFiles.put(assetKey, IOUtils.toByteArray(contents));
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public AssetDetail readAsset(NDLAssetType type, String name) throws IOException {
		String assetKey = getFullName(name);
		if(otherFiles.containsKey(assetKey)) {
			// found
			return new AssetDetail(otherFiles.get(assetKey), name, type);
		} else {
			// not found
			return null;
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<AssetDetail> readAllAssets() throws IOException {
		return readAllAssets(true);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<AssetDetail> readAllAssets(boolean loadContents) throws IOException {
		List<AssetDetail> assets = new ArrayList<>(2);
		
		for(String assetKey : otherFiles.keySet()) {
			File file = new File(assetKey);
			String name = file.getName();
			if(!name.equals(NDLDataUtils.HANDLE_FILE) && !name.equals(NDLDataUtils.CONTENTS_FILE)) {
				// read only assets
				assets.add(new AssetDetail(loadContents ? otherFiles.get(assetKey) : null, name,
						NDLAssetType.getType(name)));
			}
		}
		
		return assets;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean removeAsset(String name, NDLAssetType type) throws IOException {
		String contentsKey = getFullName(NDLDataUtils.CONTENTS_FILE);
		String contentsFileText = new String(otherFiles.get(contentsKey)); //assumed always contents file exists
		// sets new contents
		otherFiles.put(contentsKey, NDLDataUtils.deleteAssetText(contentsFileText, name, type).getBytes());
		// remove key
		String assetKey = getFullName(name);
		return otherFiles.remove(assetKey) != null;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<NDLDataNode> getNodes(String field) {
		NDLField ndlField = new NDLField(field);
		String xPath = NDLDataUtils.getXPath4SIP(ndlField);
		NodeList xmlnodes = null;
		try{
			Document document = documents.get(ndlField.schema); 
			xmlnodes = NDLXMLUtils.getNodeList(document, xPath);
			if(StringUtils.isNotBlank(ndlField.jsonKey)) {
				// json-key available
				xmlnodes = getNodesByJsonKey(xmlnodes, ndlField.jsonKey);
			}
		} catch(Exception ex) {
			// error handling
			System.out.println("Field(" + field + ") ERROR: " + ex.getMessage());
			throw new IllegalStateException(ex.getMessage(), ex);
		}
		return NDLXMLUtils.getNDLNodeList(xmlnodes);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Map<String, byte[]> getContents() throws IOException, TransformerException {
		Map<String, byte[]> contents = new HashMap<String, byte[]>(4);
		Set<String> entries = documentsByName.keySet();
		// document byte[]
        for(String entry : entries) {
        	// take updated path
        	int p = entry.lastIndexOf('/');
        	String name = entry.substring(p + 1);
            contents.put(folder + '/' + name, NDLDataUtils.getXMLContents(documentsByName.get(entry)));
        }
        // take updated handle ID
        int p = handleFileEntry.lastIndexOf('/');
    	String handleFileName = handleFileEntry.substring(p + 1);
    	otherFiles.remove(handleFileEntry); // remove original entry
        otherFiles.put(folder + '/' + handleFileName, id.getBytes());
        
        // others with updated path
        for(String entry : otherFiles.keySet()) {
        	p = entry.lastIndexOf('/');
        	String name = entry.substring(p + 1);
        	contents.put(folder + '/' + name, otherFiles.get(entry));
        }
        
        return contents;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<String> getAllFields(Set<String> excludes) {
		Set<String> fields = new HashSet<String>(8);
		for(String schema : documents.keySet()) {
			Document document = documents.get(schema);
			NodeList nodes = document.getElementsByTagName(NDLDataUtils.DCVALUE_TAG);
			int l = nodes.getLength();
			for(int i = 0; i < l; i++) {
				Element node = (Element)nodes.item(i);
				StringBuilder key = new StringBuilder(schema);
				key.append(".").append(node.getAttribute(NDLDataUtils.ELEMENT_TAG));
				if(node.hasAttribute(NDLDataUtils.QUALIFIER_TAG)) {
					String q = node.getAttribute(NDLDataUtils.QUALIFIER_TAG);
					if(!StringUtils.equalsIgnoreCase(q, "none")) {
						// qualifier found
						key.append(".").append(q);
					}
				}
				String keytext = key.toString();
				if(excludes.contains(keytext)) {
					// skip
					continue;
				}
				fields.add(keytext);
			}
		}
		return fields;
	}
}