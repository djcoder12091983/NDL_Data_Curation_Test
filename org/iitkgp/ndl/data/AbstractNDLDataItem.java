package org.iitkgp.ndl.data;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.context.NDLDataValidationContext;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.core.NDLNodeList;
import org.iitkgp.ndl.data.asset.AssetDetail;
import org.iitkgp.ndl.data.exception.FieldIsNotJSONKeyedException;
import org.iitkgp.ndl.data.exception.NDLMultivaluedException;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Encapsulates SIP/AIP data item XML detail and other details like, handle, assets, contents
 * @see SIPDataItem
 * @see AIPDataItem
 * @author Debasis, Aurghya, Vishal
 */
public abstract class AbstractNDLDataItem implements NDLDataItem {
	
	static {
		// loads NDL configuration
		NDLConfigurationContext.init();
		NDLDataValidationContext.init();
	}
	
	// handling assets and contents remaining
	
	String folder; // associated parent entry
	String id; // handle ID
	
	boolean escapeHTMLFlag = false;
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getFolder() {
		return folder;
	}
	
	/**
	 * Turns on HTML escape flag
	 */
	public void turnOnEscapeHTMLFlag() {
		escapeHTMLFlag = true;
	}
	
	/**
	 * Loads XML detail and other details
	 * @param files files contents
	 * @throws IOException throws error when file processing related error occurs
	 * @throws SAXException throws error when XML processing related error occurs
	 */
	public final void load(Map<String, byte[]> files) throws IOException, SAXException {
		load(files, true);
	}
	
	/**
	 * Loads XML detail and other details
	 * @param files files contents
	 * @param assetLoadingFlag this flag indicates whether to load asset or not
	 * @throws IOException throws error when file processing related error occurs
	 * @throws SAXException throws error when XML processing related error occurs
	 */
	public abstract void load(Map<String, byte[]> files, boolean assetLoadingFlag) throws IOException, SAXException;
	
	/**
	 * Gets full path name for a given name
	 * @param name full path name prefixed with folder(if exists)
	 * @return returns full qualified name
	 */
	protected String getFullName(String name) {
		return (StringUtils.isNotBlank(folder) ? (folder + "/") : "") + name;
	}
	
	/**
	 * gets handle ID
	 * @return returns handle ID
	 */
	@Override
	public String getId() {
		return id;
	}
	
	/**
	 * gets folder prefix
	 * @return returns folder prefix
	 */
	public String getFolderPrefix() {
		int p = folder.lastIndexOf('/');
		if(p != -1) {
			return folder.substring(0, p);
		} else {
			return null;
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isParentItem() {
		// no has-part
		return exists("dc.relation.haspart");
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isChildItem() {
		// no has-part
		return !exists("dc.relation.haspart");
	}
	
	/**
	 * returns whether attribute exists in item
	 * @return returns true when field exists otherwise false 
	 */
	@Override
	public boolean exists(String field) {
		List<NDLDataNode> nodes = getNodes(field);
		return !nodes.isEmpty();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<String> getValue(String field) {
		return getValue(field, false);
	}
	
	// @param flushError flush error on json parsing error
	// internal usage
	protected List<String> getValue(String field, boolean flushError) {
		NDLField ndlField = new NDLField(field);
		String jsonKey = ndlField.getJsonKey();
		List<NDLDataNode> nodes = getNodes(ndlField.getField());
		List<String> values = new LinkedList<String>();
		for(NDLDataNode node : nodes) {
			if(StringUtils.isNotBlank(jsonKey)) {
				// json key
				String detail[] = NDLDataUtils.getJSONKeyedValue(node.getTextContent(), escapeHTMLFlag, true);
				if(StringUtils.equals(detail[0], jsonKey)) {
					// json key matches
					values.add(detail[1]);
				}
			} else {
				// normal value
				String v = node.getTextContent();
				if(escapeHTMLFlag) {
					v = StringEscapeUtils.unescapeHtml4(v);
				}
				values.add(v);
			}
		}
		return values;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<String> getValue(String field, Transformer<String, String> transformer) {
		List<String> values = getValue(field);
		List<String> newlist = new LinkedList<String>();
		for(String value : values) {
			newlist.addAll(transformer.transform(value));
		}
		return newlist;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getSingleValue(String field) throws NDLMultivaluedException {
		List<String> values = getValue(field);
		int s = values.size();
		if(s > 1) {
			throw new NDLMultivaluedException("Multiple value exists: [" + id + "][" + field + "][" + values + "]");
		} else if(s == 1){
			return values.get(0);
		} else {
			// value not found
			return null;
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean contains(String field, Set<String> values, boolean ignoreCase) {
		List<String> data = getValue(field);
		if(ignoreCase) {
			// ignore-case
			Set<String> modifiedValues = new HashSet<String>();
			for(String val : values) {
				modifiedValues.add(val.toLowerCase());
			}
			for(String d : data) {
				d = d.toLowerCase();
				if(modifiedValues.contains(d)) {
					// data found
					return true;
				}
			}
		} else {
			// not-ignor-case
			for(String d : data) {
				if(values.contains(d)) {
					// data found
					return true;
				}
			}
		}
		// not found yet
		return false;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean containsByRegex(String field, String ... regex) {
		if(regex.length == 0) {
			throw new IllegalArgumentException("At least one regular expression is expected.");
		}
		List<String> data = getValue(field);
		for(String d : data) {
			d = d.replaceAll("\\r?\\n", " "); // normalize the text
			for(String regx : regex) {
				if(d.matches(regx)) {
					// match found
					return true;
				}
			}
		}
		return false;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean containsByStartsWith(String field, boolean ignoreCase, String... phrases) {
		if(phrases.length == 0) {
			throw new IllegalArgumentException("At least one regular expression is expected.");
		}
		List<String> data = getValue(field);
		for(String d : data) {
			d = d.replaceAll("\\r?\\n", " "); // normalize the text
			for(String phrase : phrases) {
				if(ignoreCase) {
					if(StringUtils.startsWithIgnoreCase(d, phrase)) {
						return true;
					}
				} else {
					if(StringUtils.startsWith(d, phrase)) {
						return true;
					}
				}
			}
		}
		return false;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean containsByStartsWith(String field, String... phrases) {
		return containsByStartsWith(field, false, phrases);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean containsByEndsWith(String field, boolean ignoreCase, String... phrases) {
		if(phrases.length == 0) {
			throw new IllegalArgumentException("At least one regular expression is expected.");
		}
		List<String> data = getValue(field);
		for(String d : data) {
			d = d.replaceAll("\\r?\\n", " "); // normalize the text
			for(String phrase : phrases) {
				if(ignoreCase) {
					if(StringUtils.endsWithIgnoreCase(d, phrase)) {
						return true;
					}
				} else {
					if(StringUtils.endsWith(d, phrase)) {
						return true;
					}
				}
			}
		}
		return false;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean containsByEndsWith(String field, String... phrases) {
		return containsByEndsWith(field, false, phrases);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean contains(String field, String... values) {
		if(values.length == 0) {
			// at least one value expected
			throw new IllegalArgumentException("At least one value expected for checking");
		}
		Set<String> newvalues = new HashSet<String>(2);
		for(String value : values) {
			newvalues.add(value);
		}
		return contains(field, newvalues);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean contains(String field, Set<String> values) {
		return contains(field, values, false); // strict-case
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int delete(String field, Filter<String> filter) {
		NDLField ndlField = new NDLField(field);
		List<NDLDataNode> nodes = getNodes(ndlField.getField());
		int c = 0;
		String jsonKey = ndlField.getJsonKey();
		for(NDLDataNode node : nodes) {
			String t = node.getTextContent();
			String value = NDLDataUtils.getNDLFieldValue(t, jsonKey, escapeHTMLFlag, true);
			boolean delete = false;
			if(filter != null) {
				// apply filter
				delete = filter.filter(value);
			} else {
				// blind delete
				if(StringUtils.isNotBlank(jsonKey)) {
					delete = StringUtils.equals(jsonKey, NDLDataUtils.getJSONKeyedValue(t)[0]);
				} else {
					delete = true;
				}
			}
			if(delete) {
				// conditional delete
				node.remove();
				c++;
			}
		}
		return c;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int delete(String field) {
		return delete(field, null);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int add(String field, String... values) {
		Set<String> newvalues = new HashSet<String>(2);
		for(String value : values) {
			newvalues.add(value);
		}
		add(field, newvalues); // finally add to field
		return newvalues.size();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int addIfNotContains(String field, Collection<String> values) {
		List<String> existing = getValue(field);
		values.removeAll(existing); // remove existing values from provided list to avoid duplicate entry
		return add(field, values);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int addIfNotContains(String field, String... values) {
		List<String> existing = getValue(field);
		List<String> newvalues = new LinkedList<String>();
		for(String value : values) {
			if(!existing.contains(value)) {
				// new value found
				newvalues.add(value);
			}
		}
		return add(field, newvalues);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int move(String source, String destination, Filter<String> filter, Transformer<String, String> transformer) {
		NDLField ndlField1 = new NDLField(source);
		List<NDLDataNode> nodes1 = getNodes(ndlField1.getField());
		int c = 0;
		for(NDLDataNode node : nodes1) {
			String value = NDLDataUtils.getNDLFieldValue(node.getTextContent(), ndlField1.getJsonKey(), escapeHTMLFlag,
					true);
			if(value != null) {
				// value found
				Collection<String> values = null;
				if(transformer != null) {
					// modified value
					values = transformer.transform(value);
				} else {
					values = new HashSet<String>(2);
					values.add(value);
				}
				
				if(filter != null) {
					// apply filter
					boolean move = filter.filter(value);
					if(move) {
						// move
						c += addIfNotContains(destination, values);
						node.remove(); // remove original node
					}
				} else {
					// blind move
					c += addIfNotContains(destination, values);
					node.remove(); // remove original node
				}
			}
		}
		return c;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int moveByRegex(String source, String destination, String regex) {
		NDLField ndlField1 = new NDLField(source);
		List<NDLDataNode> nodes1 = getNodes(ndlField1.getField());
		int c = 0;
		for(NDLDataNode node : nodes1) {
			String value = NDLDataUtils.getNDLFieldValue(node.getTextContent(), ndlField1.getJsonKey(), escapeHTMLFlag,
					true);
			if(value != null) {
				value = value.replaceAll("\\r?\\n", " "); // normalize the text
				// value found
				if(value.matches(regex)) {
					// move
					c += addIfNotContains(destination, value);
					node.remove(); // remove original node
					c++;
				}
			}
		}
		
		return c;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int move(String source, String destination, Filter<String> filter) {
		return move(source, destination, filter, null);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int move(String source, String destination) {
		return move(source, destination, null, null);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int move(String source, String destination, Transformer<String, String> transformer) {
		return move(source, destination, null, transformer);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int update(String field, Transformer<String, String> transformer, Filter<String> filter) {
		boolean numeric = NDLDataUtils.isNumericJSONKeyedField(field);
		NDLField ndlField = new NDLField(field);
		String jsonKey = ndlField.getJsonKey();
		List<NDLDataNode> nodes = getNodes(ndlField.getField());
		Set<String> newvalues = new HashSet<String>();
		for(NDLDataNode node : nodes) {
			String value = NDLDataUtils.getNDLFieldValue(node.getTextContent(), jsonKey, escapeHTMLFlag, true);
			if(value != null) {
				// value found
				boolean update = true;
				if(filter != null) {
					// apply filter
					update = filter.filter(value);
				}
				// value available (NULL in case of JSON key not present)
				// modified value, always has to provide transformer
				Iterator<String> values = transformer.transform(value).iterator();
				if(values.hasNext()) {
					// remove node
					node.remove();;
					// update values
					while(values.hasNext()) {
						String newvalue = values.next();
						if(StringUtils.isNotBlank(jsonKey)) {
							newvalue = NDLDataUtils.getJson(jsonKey, numeric ? Long.parseLong(newvalue) : newvalue);
						}
						if(update) {
							// to be updated
							newvalues.add(newvalue);
						}
					}
				}
			}
		}
		// add new value if any
		return add(field, newvalues);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void inherit(NDLDataItem parent, String field) {
		Set<String> excludes = new HashSet<String>(2); // empty set
		inherit(parent, field, excludes);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void inherit(NDLDataItem parent, String field, String... excludes) {
		inherit(parent, field, NDLDataUtils.createNewSet(excludes));
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void inherit(NDLDataItem parent, String field, Set<String> excludes) {
		NDLField ndlField = new NDLField(field);
		String jsonKey = ndlField.getJsonKey();
		List<NDLDataNode> nodes = parent.getNodes(field);
		Set<String> values = new HashSet<String>();
		for(NDLDataNode node : nodes) {
			String value = NDLDataUtils.getNDLFieldValue(node.getTextContent(), jsonKey, escapeHTMLFlag, true);
			if(StringUtils.isNotBlank(value) && !excludes.contains(value)) {
				// skip values if exists in "excludes" set
				// value available (NULL in case of JSON key not present)
				values.add(value);
			}
		}
		// add values
		add(field, values);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int update(String field, Transformer<String, String> transformer) {
		return update(field, transformer, null);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int replace(String field, String oldvalue, String newvalue, boolean ignorecase) {
		NDLField ndlField = new NDLField(field);
		String jsonKey = ndlField.getJsonKey();
		List<NDLDataNode> nodes = getNodes(field);
		int c = 0;
		for(NDLDataNode node : nodes) {
			String value = NDLDataUtils.getNDLFieldValue(node.getTextContent(), jsonKey, escapeHTMLFlag, true);
			boolean match = ignorecase ? StringUtils.equalsIgnoreCase(value, oldvalue)
						: StringUtils.equals(value, oldvalue);
			if(match) {
				// match found so update
				node.setTextContent(StringUtils.isNotBlank(jsonKey)
						? NDLDataUtils.getJson(jsonKey,
								NDLDataUtils.isNumericJSONKeyedField(field) ? Long.valueOf(newvalue) : newvalue)
						: newvalue);
				c++;
			}
		}
		return c;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int replaceByRegex(String field, String regex, String replace) {
		NDLField ndlField = new NDLField(field);
		String jsonKey = ndlField.getJsonKey();
		List<NDLDataNode> nodes = getNodes(field);
		int c = 0;
		for(NDLDataNode node : nodes) {
			String value = NDLDataUtils.getNDLFieldValue(node.getTextContent(), jsonKey, escapeHTMLFlag, true);
			String newvalue = value.replaceAll(regex, replace);
			if(StringUtils.isBlank(newvalue)) {
				// blank value remove
				node.remove();
			} else if(!StringUtils.equals(value, newvalue)) {
				// changed
				node.setTextContent(StringUtils.isNotBlank(jsonKey)
						? NDLDataUtils.getJson(jsonKey,
								NDLDataUtils.isNumericJSONKeyedField(field) ? Long.valueOf(newvalue) : newvalue)
						: newvalue);
				c++;
			}
		}
		return c;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int replace(String field, String oldvalue, String newvalue) {
		return replace(field, oldvalue, newvalue, false);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean updateSingleValue(String field, String value) {
		if(StringUtils.isBlank(value)) {
			// avoids blank value
			return false;
		}
		boolean numeric = NDLDataUtils.isNumericJSONKeyedField(field);
		NDLField ndlField = new NDLField(field);
		String jsonKey = ndlField.getJsonKey();
		String newvalue = null;
		if(StringUtils.isNotBlank(jsonKey)) {
			newvalue = NDLDataUtils.getJson(jsonKey, numeric ? Long.parseLong(value) : value);
		} else {
			// leading and trailing space remove
			newvalue = value.trim();
		}
		List<NDLDataNode> nodes = getNodes(ndlField.getField());
		boolean flag = false;
		for(NDLDataNode node : nodes) {
			String nodevalue = NDLDataUtils.getNDLFieldValue(node.getTextContent(), jsonKey, escapeHTMLFlag, true);
			if(nodevalue != null) {
				// value available (NULL in case of JSON key not present)
				// blind update
				if(StringUtils.isNotBlank(newvalue)) {
					// check before update
					node.setTextContent(newvalue);
				}
				flag = true;
				break;
			}
		}
		if(!flag) {
			// not updated then add
			add(field, value);
			flag = true;
		}
		return flag;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int removeDuplicate(String field) {
		List<NDLDataNode> nodes = getNodes(field);
		Set<String> track = new HashSet<String>();
		int c = 0;
		for(NDLDataNode node : nodes) {
			String value = node.getTextContent().trim();
			// handle JSON-keyed
			if(NDLDataValidationContext.getNDLJSONKeyedFields().contains(field)) {
				// json-keyed field
				String keyvalue[] = NDLDataUtils.getJSONKeyedValue(value);
				String key = keyvalue[0];
				String keyval = keyvalue[1];
				if(!NDLDataUtils.isInvalidJsonKey(key)) {
					// valid
					value = key + ":" + keyval; // composite value
				}
			}
			value = StringEscapeUtils.escapeCsv(value);
			if(track.contains(value)) {
				// duplicate
				node.remove();
				c++;
			} else {
				track.add(value); // add to track
			}
		}
		return c;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int removeBlank(String field) {
		NDLField ndlField = new NDLField(field);
		List<NDLDataNode> nodes = getNodes(field);
		int c = 0;
		for(NDLDataNode node : nodes) {
			String value = NDLDataUtils.getNDLFieldValue(node.getTextContent(), ndlField.getJsonKey(), escapeHTMLFlag,
					true);
			if(StringUtils.isBlank(value)) {
				// blank
				node.remove();
				c++;
			}
		}
		return c;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Map<String, Collection<String>> getAllValues() {
		Set<String> excludes = new HashSet<String>(2); // empty set
		return getAllValues(excludes);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<String> getAllFields(String... excludes) {
		return getAllFields(NDLDataUtils.createNewSet(excludes));
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Map<String, Collection<String>> getAllValues(Set<String> excludes) {
		Map<String, Collection<String>> values = new HashMap<String, Collection<String>>();
		Set<String> fields = getAllFields();
		for(String field : fields) {
			if(excludes.contains(field)) {
				// exclude columns
				continue;
			}
			Collection<String> list = values.get(field);
			if(list == null) {
				list = new LinkedList<String>();
				values.put(field, list);
			}
			list.addAll(getValue(field));
		}
		return values;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Map<String, Collection<String>> getAllValues(String... excludes) {
		return getAllValues(NDLDataUtils.createNewSet(excludes));
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean removeAsset(NDLAssetType type) throws IOException {
		String name = NDLDataUtils.getAssetMappingName(type.getType()); // gets mapped name from asset type
		return removeAsset(name, type);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public AssetDetail readAsset(NDLAssetType type) throws IOException {
		String name = NDLDataUtils.getAssetMappingName(type.getType()); // gets mapped name from asset type
		return readAsset(type, name);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void addAsset(NDLAssetType type, InputStream contents) throws IOException {
		String name = NDLDataUtils.getAssetMappingName(type.getType()); // gets mapped name from asset type
		if(StringUtils.isBlank(name)) {
			// error condition
			throw new IllegalStateException("Name must be specified");
		}
		addAsset(name, type, contents);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void addAsset(NDLAssetType type, byte[] contents) throws IOException {
		addAsset(type, new ByteArrayInputStream(contents));
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void addAsset(String name, NDLAssetType type, byte[] contents) throws IOException {
		addAsset(name, type, new ByteArrayInputStream(contents));
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Map<String, Collection<String>> getFieldMap(String field) throws FieldIsNotJSONKeyedException {
		if(!NDLDataValidationContext.getNDLJSONKeyedFields().contains(field)) {
			// error
			throw new FieldIsNotJSONKeyedException("Field: " + field + " is not managed by json-key.");
		}
		Map<String, Collection<String>> map = new HashMap<String, Collection<String>>();
		List<String> values = getValue(field);
		for(String value : values) {
			// deserialize JSON
			String keyvalue[] = NDLDataUtils.getJSONKeyedValue(value);
			String key = keyvalue[0];
			if(NDLDataUtils.isInvalidJsonKey(key)) {
				// invalid
				continue;
			}
			// valid
			String keyval = keyvalue[1];
			Collection<String> list = map.get(key);
			if(list == null) {
				list = new LinkedList<String>();
				map.put(key, list);
			}
			list.add(keyval);
		}
		return map;
	}
	
	// sub-list by josn-key
	protected NodeList getNodesByJsonKey(NodeList nodes, String jsonKey) {
		// filter
		NDLNodeList newlist = new NDLNodeList();
		int l = nodes.getLength();
		for(int i = 0; i < l; i++) {
			Node node = nodes.item(i);
			String detail[] = NDLDataUtils.getJSONKeyedValue(node.getTextContent());
			if(StringUtils.equals(detail[0], jsonKey)) {
				// JSON keyt match
				newlist.addNode(node);
			}
		}
		return newlist;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getAdditionalInfoJSONKeyedSingleValue(String jsonKey) throws NDLMultivaluedException {
		String key = "ndl.sourceMeta.additionalInfo:" + jsonKey;
		List<String> values = getValue(key, false);
		int s = values.size();
		if(s > 1) {
			throw new NDLMultivaluedException(key + " is multivalued.");
		} else if(s == 1) {
			return values.get(0);
		} else {
			return null;
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<String> getAdditionalInfoJSONKeyedValue(String jsonKey) {
		return getValue("ndl.sourceMeta.additionalInfo:" + jsonKey, false);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int normalize(String field, Transformer<String, String> normalizer) {
		NDLField ndlField = new NDLField(field);
		List<NDLDataNode> nodes = getNodes(field);
		int c = 0;
		for(NDLDataNode node : nodes) {
			Collection<String> normalized = normalizer.transform(
					NDLDataUtils.getNDLFieldValue(node.getTextContent(), ndlField.getJsonKey(), escapeHTMLFlag, true));
			node.remove(); // remove and update
			c += add(field, normalized);
		}
		return c;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean deleteDuplicateFieldValues(String ... fields) {
		int l = fields.length;
		if(l < 2) {
			throw new IllegalArgumentException("Fields should be at least 2");
		}
		List<Set<String>> values = new ArrayList<Set<String>>(l);
		for(String field : fields) {
			Set<String> tvalues = new LinkedHashSet<String>(2);
			values.add(tvalues);
			tvalues.addAll(getValue(field));
			// remove
			delete(field);
		}
		Set<String> first = values.get(0);
		add(fields[0], first); // first field add
		boolean delete = false;
		for(int i = 1; i < l; i++) {
			Set<String> tvalues = values.get(i);
			tvalues.removeAll(first); // delete duplicates
			// add from scratch
			add(fields[i], tvalues);
		}
		return delete;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean retainByIndex(String field, Integer... indices) throws IllegalArgumentException {
		if(indices.length == 0) {
			throw new IllegalArgumentException(
					"At least one indexd should be provided for retain, otherwise use delete API");
		}
		List<Integer> indicesl = Arrays.asList(indices);
		List<NDLDataNode> nodes = getNodes(field);
		int l = nodes.size();
		boolean flag = false;
		List<String> values = new LinkedList<String>();
		for(int i = 0; i < l; i++) {
			if(indicesl.contains(i)) {
				// filter
				NDLDataNode node = nodes.get(i);
				values.add(node.getTextContent());
			}
		}
		delete(field); // delete and restore filter values
		add(field, values);
		return flag;
	}
	
	// gets node
	NDLDataNode getNode(String field, int index) {
		List<NDLDataNode> nodes = getNodes(field);
		int s = nodes.size();
		if(index > s) {
			throw new IllegalArgumentException("Invalid index: " + index + ", expected less than: " + s);
		}
		return nodes.get(index - 1);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getNodevalue(String field, int index) {
		// extracts
		return getNode(field, index).getTextContent();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void updateNodeValue(String field, String value, int index) {
		// update
		getNode(field, index).setTextContent(value);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void print(String field) {
		print(field, '|');
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void print(String field, char separator) {
		List<String> values = getValue(field);
		if(!values.isEmpty()) {
			System.out.println(field + " => " + NDLDataUtils.join(values, separator));
		}
	}
}