package org.iitkgp.ndl.data;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.asset.AssetDetail;
import org.iitkgp.ndl.data.exception.FieldIsNotJSONKeyedException;
import org.iitkgp.ndl.data.exception.NDLMultivaluedException;
import org.w3c.dom.NodeList;

/**
 * <pre>SIP/AIP Data item details, adding/removing/moving field etc.
 * Handles assets, contents, handle ID etc.</pre>
 * Here <b>field</b> refers to NDL field.
 * @author Debasis, Aurghya, Vishal
 */
public interface NDLDataItem extends DataItemWritable, BaseNDLDataItem {
	
	/**
	 * gets parent information
	 * @return returns parent
	 */
	String getFolder();
	
	/**
	 * Sets folder information
	 * @param folder folder information
	 */
	void setFolder(String folder);
	
	/**
	 * Returns whether an item is parent or not.
	 * It's required when data hierarchy exists otherwise all items are children.
	 * @return returns true if parent item otherwise false 
	 */
	boolean isParentItem();
	
	/**
	 * Returns whether an item is child or not.
	 * By default all items are children if no hierarchy exists.
	 * @return returns true if child item otherwise false 
	 */
	boolean isChildItem();
	
	/**
	 * Gets handle ID
	 * @return returns handle ID
	 */
	String getId();
	
	
	/**
     * Gets list of values for a given field/attribute
     * @param field field name
     * @param transformer transforms output value
     * @return a list of value corresponding to name, empty in case no data found
     */
    List<String> getValue(String field, Transformer<String, String> transformer);
    
    /**
     * Gets single value for a given field
     * @param field given field name
     * @return a single value corresponding to name, NULL if not found
     * @throws NDLMultivaluedException throws error when more than one values exist
     */
    String getSingleValue(String field) throws NDLMultivaluedException;
    
    /**
     * Checks whether a field exists or not in item
     * @param field given field name
     * @return returns true when exists otherwise false
     */
    boolean exists(String field);
    
    /**
     * Checks whether a field contains any value from a given set of values
     * @param field field name
     * @param values given set of values to check whether any value belongs to that field
     * @return whether field belongs to any values, true if exists otherwise false 
     */
    boolean contains(String field, Set<String> values);
    
    /**
     * Checks whether a field contains any value from a given patterns (regular expression)
     * @param field field name
     * @param regex given patterns (regular expression)
     * @return whether field belongs to given pattern, true if exists otherwise false
     */
    boolean containsByRegex(String field, String... regex);
    
    /**
     * Checks whether a field value starts with given phrases
     * @param field field name
     * @param ignoreCase whether checking is case insensitive
     * @param phrases given phrases
     * @return whether field belongs to given pattern, true if exists otherwise false
     */
    boolean containsByStartsWith(String field, boolean ignoreCase, String... phrases);
    
    /**
     * Checks whether a field value starts with given phrases
     * @param field field name
     * @param phrases given phrases
     * @return whether field belongs to given pattern, true if exists otherwise false
     */
    boolean containsByStartsWith(String field, String... phrases);
    
    /**
     * Checks whether a field value ends with given phrases
     * @param field field name
     * @param ignoreCase whether checking is case insensitive
     * @param phrases given phrases
     * @return whether field belongs to given pattern, true if exists otherwise false
     */
    boolean containsByEndsWith(String field, boolean ignoreCase, String... phrases);
    
    /**
     * Checks whether a field value ends with given phrases
     * @param field field name
     * @param phrases given phrases
     * @return whether field belongs to given pattern, true if exists otherwise false
     */
    boolean containsByEndsWith(String field, String... phrases);
    
    /**
     * Checks whether a field contains any value from a given set of values
     * @param field field name
     * @param values given set of values to check whether any value belongs to that field
     * @return whether field belongs to any values, true if exists otherwise false 
     */
    boolean contains(String field, String... values);
    
    /**
     * Checks whether a field contains any value from a given set of values with case sensitivity
     * @param field field name
     * @param values given set of values to check whether any value belongs to that field
     * @param ignoreCase false when strict case comparison takes place otherwise true
     * @return whether field belongs to any values, true if exists otherwise false
     * @see #contains(String, Set)
     */
    boolean contains(String field, Set<String> values, boolean ignoreCase);
    
    /**
     * Deletes a field from item with filter logic
     * @param field field name
     * @param filter filter logic determines whether a value to be deleted or not,
     * if {@link Filter#filter(Object)} returns true that value deleted otherwise retained
     * @return returns how many values deleted 
     */
    int delete(String field, Filter<String> filter);
    
    /**
     * Deletes a field from item
     * @param field field name
     * @return returns how many values deleted
     */
    int delete(String field);
    
    /**
     * <pre>Moves source field value to destination field with filter and transformation logic</pre>
     * Note: filter and transformation logic work on string text
     * @param source source field name
     * @param destination destination field name
     * @param filter filter logic determines whether a value to be deleted or not,
     * if {@link Filter#filter(Object)} returns true that value moved otherwise retained
     * @param transformer transformation logic determines while moving the value any transformation of that value needed or not,
     * @return returns how many values moved 
     * see {@link Transformer#transform(Object)} for more details
     */
    int move(String source, String destination, Filter<String> filter, Transformer<String, String> transformer);
    
    /**
     * <pre>Moves source field value to destination field with filter logic</pre>
     * Note: filter logic works on string text
     * @param source source field name
     * @param destination destination field name
     * @param filter filter logic determines whether a value to be deleted or not,
     * if {@link Filter#filter(Object)} returns true that value moved otherwise retained
     * @return returns how many values moved
     */
    int move(String source, String destination, Filter<String> filter);
    
    /**
     * <pre>Moves source field value to destination field with transformation logic</pre>
     * Note: transformation logic works on string text
     * @param source source field name
     * @param destination destination field name
     * @param transformer transformation logic determines while moving the value any transformation of that value needed or not,
     * @return returns how many values moved
     * see {@link Transformer#transform(Object)} for more details
     */
    int move(String source, String destination, Transformer<String, String> transformer);
    
    /**
     * Moves data from source to destination if source values matches with given pattern (regular expression)
     * @param source source field name
     * @param destination destination field name
     * @param regex given pattern (regular expression) of source value
     * @return returns how many values moved
     */
    int moveByRegex(String source, String destination, String regex);
    
    /**
     * Moves source filed value to destination field without filter and transformation logic
     * @param source source field name
     * @param destination destination field name
     * @return returns how many values moved
     */
    int move(String source, String destination);
    
    /**
     * <pre>Updates a field values with a transformation logic</pre>
     * Note: transformation logic works on string text
     * @param field field name
     * @param transformer transformation logic determines while moving the value any transformation of that value needed or not,
     * see {@link Transformer#transform(Object)} for more details
     * @return returns how many values updated
     */
    int update(String field, Transformer<String, String> transformer);
    
    /**
     * <pre>Updates a field values with filter and transformation logic</pre>
     * Note: filter and transformation logic work on string text
     * @param field field name
     * @param filter filter logic determines whether a value to be deleted or not,
     * if {@link Filter#filter(Object)} returns true that value moved otherwise retained
     * @param transformer transformation logic determines while moving the value any transformation of that value needed or not,
     * see {@link Transformer#transform(Object)} for more details
     * @return returns how many values updated
     */
    int update(String field, Transformer<String, String> transformer, Filter<String> filter);
    
    /**
     * Replace old value with new value for a given field
     * @param field field name
     * @param oldvalue old value to be replaced
     * @param newvalue new value to be replaced by
     * @return returns how many values replaced
     */
    int replace(String field, String oldvalue, String newvalue);
    
    /**
     * Replace a pattern (regular expression) with new value for a given field
     * @param field field name
     * @param regex pattern (regular expression)
     * @param replace new value to be replaced by
     * @return returns how many values replaced
     */
    int replaceByRegex(String field, String regex, String replace);
    
    /**
     * Replace old value with new value for a given field with case sensitivity
     * @param field field name
     * @param oldvalue old value to be replaced
     * @param newvalue new value to be replaced by
     * @param ignoreCase false when strict case comparison takes place otherwise true
     * @return returns how many values replaced
     * @see #replace(String, String, String)
     */
    int replace(String field, String oldvalue, String newvalue, boolean ignoreCase);
    
    /**
     * Adds values for a given field
     * @param field field name
     * @param values values
     * @return returns how many values added
     * @see #addIfNotContains(String, Collection)
     */
    int add(String field, Collection<String> values);
    
    /**
     * Adds values for a given field
     * @param field field name
     * @param values multiple values (array of values)
     * @return returns how many values added
     * @see #addIfNotContains(String, String...)
     */
    int add(String field, String ... values);
    
    /**
     * Adds values for a given field, if specified values does not exist
     * @param field field name
     * @param values values
     * @return returns how many values added
     */
    int addIfNotContains(String field, Collection<String> values);
    
    /**
     * Adds values for a given field, if specified values does not exist
     * @param field field name
     * @param values multiple values (array of values)
     * @return returns how many values added
     */
    int addIfNotContains(String field, String ... values);
    
    /**
     * Adds assets to data item
     * @param type asset type, see {@link NDLAssetType}
     * @param contents asset contents in byte array
     * @throws IOException throws error if adding asset I/O related error occurs
     * @see #addAsset(String, NDLAssetType, byte[])
     */
    void addAsset(NDLAssetType type, byte[] contents) throws IOException;
    
    /**
     * Adds assets to data item
     * @param name name of the asset
     * @param type asset type, see {@link NDLAssetType}
     * @param contents asset contents in byte array
     * @throws IOException throws error if adding asset I/O related error occurs
     */
    void addAsset(String name, NDLAssetType type, byte[] contents) throws IOException;
    
    /**
     * Adds assets to data item
     * @param type asset type
     * @param contents asset contents in stream
     * @throws IOException throws error if adding asset I/O related error occurs
     */
    void addAsset(NDLAssetType type, InputStream contents) throws IOException;
    
    /**
     * Adds assets to data item
     * @param name name of the asset
     * @param type asset type
     * @param contents asset contents in stream
     * @throws IOException throws error if adding asset I/O related error occurs
     * @see #addAsset(String, NDLAssetType, InputStream)
     */
    void addAsset(String name, NDLAssetType type, InputStream contents) throws IOException;
    
    /**
     * Reads asset by asset type
     * @param type asset type
     * @return returns asset detail if found otherwise null
     * @throws IOException throws exception in case of errors
     */
    AssetDetail readAsset(NDLAssetType type) throws IOException;
    
    /**
     * Reads asset by asset type and name, when same type multiple name exists
     * @param type asset type
     * @param name name for that asset type
     * @return returns asset detail if found otherwise null
     * @throws IOException throws exception in case of errors
     */
    AssetDetail readAsset(NDLAssetType type, String name) throws IOException;
    
    /**
     * read all assets for an item
     * @return returns all assets if found otherwise empty list
     * @throws IOException throws exception in case of error
     */
    List<AssetDetail> readAllAssets() throws IOException;
    
    /**
     * read all assets for an item
     * @param loadContents whether to load contents or not
     * @return returns all assets if found otherwise empty list
     * @throws IOException throws exception in case of error
     */
    List<AssetDetail> readAllAssets(boolean loadContents) throws IOException;
    
    /**
     * Removes asset by asset type
     * @param type asset type
     * @return returns true if removed successfully otherwise false
     * @throws IOException throws Exception in case of errors
     */
    boolean removeAsset(NDLAssetType type) throws IOException;
    
    /**
     * Removes asset by name and asset type
     * @param name asset name
     * @param type asset type
     * @return returns true if removed successfully otherwise false
     * @throws IOException throws Exception in case of errors
     */
    boolean removeAsset(String name, NDLAssetType type) throws IOException;
    
    /**
     * Updates an item single item with new value, if not exist then append
     * @param field field name
     * @param value new value to be updated with
     * @return returns whether value updated or not 
     */
    boolean updateSingleValue(String field, String value);
    
    /**
     * Gets node list for a given field
     * @param field field name
     * @return returns node list
     * @see NodeList
     */
    List<NDLDataNode> getNodes(String field);
    
    /**
     * Gets node value for a given field
     * @param field given field
     * @param index node index (starts from 1)
     * @return returns value if exists otherwise throws exception
     * @throws IllegalArgumentException throws exception if index does not exist
     */
    String getNodevalue(String field, int index);
    
    /**
     * Updates node value by node index for a given field
     * @param field given field
     * @param value given new value
     * @param index node index (starts from 1)
     * @throws IllegalArgumentException throws exception if index does not exist
     */
    void updateNodeValue(String field, String value, int index);
    
    /**
     * Inherits all values for a given item and field
     * @param parent parent item
     * @param field field name to be inherited
     */
    void inherit(NDLDataItem parent, String field);
    
    /**
     * Inherits all values for a given item and field
     * @param parent parent item
     * @param field field name to be inherited
     * @param excludes which values to be excluded
     * @see #inherit(NDLDataItem, String)
     */
    void inherit(NDLDataItem parent, String field, Set<String> excludes);
    
    /**
     * Inherits all values for a given item and field
     * @param parent parent item
     * @param field field name to be inherited
     * @param excludes which values to be excluded
     * @see #inherit(NDLDataItem, String, Set)
     */
    void inherit(NDLDataItem parent, String field, String ... excludes);
    
    /**
     * Gets all values for current item with excluded field names
     * @param excludes field to be excluded while generating values
     * @return returns values in <b>Map&lt;String, List&lt;String&gt;&gt;</b> form,
     * key is field name and value is list of values
     */
    Map<String, Collection<String>> getAllValues(Set<String> excludes);
    
    /**
     * Gets all values for current item with excluded field names
     * @param excludes field to be excluded while generating values
     * @return returns values in <b>Map&lt;String, List&lt;String&gt;&gt;</b> form,
     * key is field name and value is list of values
     */
    Map<String, Collection<String>> getAllValues(String ... excludes);
    
    /**
     * Gets all fields excluding given exclude field set
     * @param excludes given exclude field set
     * @return returns all fields excluding given exclude field set
     */
    Set<String> getAllFields(String ... excludes);
    
    /**
     * Gets all fields excluding given exclude field set
     * @param excludes given exclude field set
     * @return returns all fields excluding given exclude field set
     */
    Set<String> getAllFields(Set<String> excludes);
    
    /**
     * Removes duplicate entry for a given field
     * @param field field name
     * @return returns how many values removed
     */
    int removeDuplicate(String field);
    
    /**
     * Removes blank value for given field
     * @param field field name
     * @return returns how many values removed
     */
    int removeBlank(String field);
    
    /**
	 * Gets item key-value pair details for json-keyed fields for a given item
	 * @param field field name
	 * @return returns field map (key-value pair details), key is JSON-key and value is associated value/values
	 */
	Map<String, Collection<String>> getFieldMap(String field) throws FieldIsNotJSONKeyedException;
	
	/**
	 * Gets additional info json keyed-value
	 * @param jsonKey json key
	 * @return returns json keyed-value
	 * @throws NDLMultivaluedException throws error in case of multiple value found
	 */
	String getAdditionalInfoJSONKeyedSingleValue(String jsonKey) throws NDLMultivaluedException;
	
	/**
	 * Gets additional info json keyed-values
	 * @param jsonKey json key
	 * @return returns json keyed-values
	 */
	List<String> getAdditionalInfoJSONKeyedValue(String jsonKey);
	
	/**
	 * Normalize a field by a custom normalizer
	 * @param field field to be be normalized
	 * @param normalizer given normalizer
	 * @return returns how many values added after normalization
	 */
	int normalize(String field, Transformer<String, String> normalizer);
	
	/**
	 * Delete duplicate fields, duplicate fields meant by same value (exact match).
	 * <pre>If field values are same then except first field rest gets deleted</pre>
	 * <pre>This method also deletes duplicate field values for fields</pre>
	 * <pre> Don't get confused with {@link #removeDuplicate(String)}</pre>
	 * @param fields list of fields to be compared
	 * @return returns true if delete happens
	 * @throws IllegalArgumentException throws exception when fields count is less than 2
	 */
	public boolean deleteDuplicateFieldValues(String ... fields) throws IllegalArgumentException;
	
	/**
	 * Retains first node and deletes rest nodes of given field
	 * @param field given field
	 * @param indices given index to retain, index starts from 0
	 * @return returns true if so happened otherwise false (single node)
	 * @throws IllegalArgumentException throws exception when indices not provided
	 */
	public boolean retainByIndex(String field, Integer ... indices) throws IllegalArgumentException;
	
	/**
	 * Prints values to SYS.OUT for a given field
	 * @param field given field name
	 * @param separator separator for multiple values
	 */
	public void print(String field, char separator);
	
	/**
	 * Prints values to SYS.OUT for a given field
	 * <pre>Default separator is pipe</pre>
	 * @param field given field name
	 */
	public void print(String field);
	
	/**
	 * Returns size in bytes
	 * @return returns size in bytes
	 */
	public long size();
}