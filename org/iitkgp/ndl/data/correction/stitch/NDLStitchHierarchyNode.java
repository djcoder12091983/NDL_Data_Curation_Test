package org.iitkgp.ndl.data.correction.stitch;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.exception.NDLIncompleteDataException;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * NDL stitch hierarchy individual detail
 * @author Debasis
 */
public class NDLStitchHierarchyNode {
	
	// null node identifier
	static final String NULLNODE = "__NULL_NODE__";
	// no order
	static final String NO_ORDER = "__NO__";
	
	/**
	 * determines whether it's valid order information or not
	 * @param order given order information
	 * @return returns true if so otherwise false
	 */
	public static boolean isNoOrder(String order) {
		return StringUtils.isBlank(order) || StringUtils.equals(order, NO_ORDER);
	}
	
	/**
	 * NULL hierarchy node when a level is missing
	 */
	public static final NDLStitchHierarchyNode NULL_HIERARCHY_NODE = new NDLStitchHierarchyNode(NULLNODE, NULLNODE);
	
	String id;
	String title;
	String handle;
	Map<String, Collection<String>> metadata = new HashMap<>(2);
	Map<String, Collection<String>> additionalData = new HashMap<>(2);
	boolean create = true;
	String order = NO_ORDER; // default ordering
	String folder; // folder location if exists
	int level;
	boolean leaf = false;
	boolean existingNode = false;
	boolean rootLocation = false; // location to copy
	short size = 0;
	
	/**
	 * Constructor
	 * @param id node ID
	 * @param title node title
	 * @param useIDAsHandle use ID as handle
	 * @param create create flag
	 */
	public NDLStitchHierarchyNode(String id, String title, boolean useIDAsHandle, boolean create) {
		if(StringUtils.isBlank(id) || StringUtils.isBlank(title)) {
			throw new NDLIncompleteDataException("ID/Title is missing.");
		}
		this.id = id;
		this.title = title;
		if(useIDAsHandle) {
			this.handle = id;
			size += id.length();
		}
		size += id.length() + title.length();
		this.create = create;
		if(!create) {
			// probably an existing node
			this.existingNode = true;
		}
	}
	
	/**
	 * Constructor
	 * @param id node ID
	 * @param title node title
	 * @param handle handle ID
	 * @param create create flag
	 */
	public NDLStitchHierarchyNode(String id, String title, String handle, boolean create) {
		this.id = id;
		this.title = title;
		this.handle = handle;
		this.create = create;
		size += id.length() + title.length() + handle.length();
		if(!create) {
			// probably an existing node
			this.existingNode = true;
		}
	}
	
	/**
	 * Constructor
	 * @param id node ID
	 * @param title node title
	 * @param handle handle ID
	 */
	public NDLStitchHierarchyNode(String id, String title, String handle) {
		this(id, title, handle, true);
	}
	
	/**
	 * Constructor
	 * @param id node ID
	 * @param title node title
	 * @param useIDAsHandle use ID as handle
	 */
	public NDLStitchHierarchyNode(String id, String title, boolean useIDAsHandle) {
		this(id, title, useIDAsHandle, true);
	}
		
	/**
	 * Constructor
	 * @param id node ID
	 * @param title node title
	 */
	public NDLStitchHierarchyNode(String id, String title) {
		this(id, title, false, true);
	}
	
	/**
	 * Sets order value
	 * @param order order value
	 */
	public void setOrder(String order) {
		this.order = order;
		size += order.length();
	}
	
	/**
	 * Gets order value
	 * @return returns order value
	 */
	public String getOrder() {
		return order;
	}
	
	/**
	 * Sets create flag (whether to create node or not)
	 * @param create create flag
	 */
	public void setCreate(boolean create) {
		this.create = create;
	}
	
	/**
	 * Returns create flag
	 * @return Returns create flag
	 */
	public boolean isCreate() {
		return create;
	}
	
	/**
	 * Gets ID
	 * @return returns ID
	 */
	public String getId() {
		return id;
	}
	
	/**
	 * Gets title
	 * @return returns title
	 */
	public String getTitle() {
		return title;
	}
	
	/**
	 * Gets handle ID
	 * @return returns handle ID
	 */
	public String getHandle() {
		return handle;
	}
	
	/**
	 * Adds metadata (this metadata required for intermediate node)
	 * @param key metadata key
	 * @param value metadata value
	 */
	public void addMetadata(String key, String value) {
		if(StringUtils.isBlank(value)) {
			// skip blank value
			return;
		}
		Collection<String> values = metadata.get(key);
		if(values == null) {
			values = new HashSet<>(2);
			metadata.put(key, values);
			size += key.length();
		}
		values.add(value);
		size += value.length();
	}
	
	/**
	 * Adds additional data for intermediate node (this may require to indicate a node precisely)
	 * @param key additional key
	 * @param value corresponding value if exists otherwise NULL
	 */
	public void addAdditionalData(String key, String value) {
		if(StringUtils.isBlank(value)) {
			// skip blank value
			return;
		}
		Collection<String> values = additionalData.get(key);
		if(values == null) {
			values = new HashSet<>(2);
			additionalData.put(key, values);
			size += key.length();
		}
		values.add(value);
		size += value.length();
	}
	
	/**
	 * gets all additional data (if multiple values associated with a key then values are separated by pipe)
	 * <pre>note: but if values contain pipe then it would be a problem</pre> 
	 * @return returns all additional data
	 * @deprecated use {@link #getRawAdditionalData()}
	 */
	@Deprecated
	public Map<String, String> getAdditionalData() {
		Map<String, String> nd = new HashMap<>(2);
		for(String k : additionalData.keySet()) {
			nd.put(k, NDLDataUtils.join(additionalData.get(k), '|'));
		}
		
		return nd;
	}
	
	/**
	 * Gets raw additional data
	 * @return returns additional data details
	 */
	public Map<String, Collection<String>> getRawAdditionalData() {
		return additionalData;
	}
	
	/**
	 * gets metadata values
	 * @return returns metadata values
	 */
	public Map<String, Collection<String>> getMetadata() {
		return metadata;
	}
	
	/**
	 * gets metadata values by given metadata key
	 * @param key given metadata key
	 * @return returns metadata values
	 */
	public Collection<String> getMetadata(String key) {
		return metadata.get(key);
	}
	
	/**
	 * gets additional data by given key
	 * <pre>if multiple values associated with key then values are separated by pipe.</pre>
	 * <pre>note: but if values contain pipe then it would be a problem,
	 * rather use {@link #getAdditionalData(String, char)}</pre>
	 * @param key given key
	 * @return returns associated value if exists otherwise NULL
	 */
	public String getAdditionalData(String key) {
		if(additionalData.containsKey(key)) {
			return NDLDataUtils.join(additionalData.get(key), '|');
		} else {
			// not found
			return null;
		}
	}
	
	/**
	 * gets additional data by given key
	 * <pre>if multiple values associated with key then values are separated by given separator.</pre>
	 * @param key given key
	 * @param separator multiple values separator
	 * @return returns associated value if exists otherwise NULL
	 */
	public String getAdditionalData(String key, char separator) {
		if(additionalData.containsKey(key)) {
			return NDLDataUtils.join(additionalData.get(key), separator);	
		} else {
			// not found
			return null;
		}
	}
	
	/**
	 * Sets folder location if exists otherwise kept inside the root
	 * @param folder folder location
	 */
	public void setFolder(String folder) {
		this.folder = folder;
	}
	
	/**
	 * gets folder location
	 * @return returns folder location
	 */
	public String getFolder() {
		return folder;
	}
	
	/**
	 * Sets level, starts from 0
	 * @param level level, starts from 0
	 */
	public void setLevel(int level) {
		this.level = level;
	}
	
	/**
	 * gets level
	 * @return return level
	 */
	public int getLevel() {
		return level;
	}
	
	/**
	 * Mention whether root location to be use for store or not
	 * @param rootLocation true if so otherwise false
	 */
	public void setRootLocation(boolean rootLocation) {
		this.rootLocation = rootLocation;
	}
	
	/**
	 * Tentative size
	 * @return returns Tentative size 
	 */
	public short size() {
		return size;
	}
	
	// null node checking
	boolean isNULLNode() {
		return StringUtils.equals(id, NULLNODE);
	}
	
	/**
	 * returns string representation
	 */
	@Override
	public String toString() {
		return "{@id: " + id + ", @handle: " + handle + " @title: " + title + " @Exists: " + existingNode + "}";
	}
	
	/**
	 * determines whether order value not defined
	 * @param order order value
	 * @return returns true if so otherwise false
	 */
	public static boolean isorderUndefined(String order) {
		return StringUtils.equals(order, NO_ORDER);
	}
}