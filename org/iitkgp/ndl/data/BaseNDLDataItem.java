package org.iitkgp.ndl.data;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.iitkgp.ndl.data.exception.NDLMultivaluedException;

/**
 * This interface describes a base contract of NDL data item
 * @author Debasis
 */
public interface BaseNDLDataItem {

	/**
     * Gets single value for a given field
     * @param field given field name
     * @return a single value corresponding to name, NULL if not found
     * @throws NDLMultivaluedException throws error when more than one values exist
     */
    String getSingleValue(String field) throws NDLMultivaluedException;
    
    /**
     * Gets list of values for a given field/attribute
     * @param field field name
     * @return a list of value corresponding to name, empty in case no data found
     */
    List<String> getValue(String field);
    
    /**
     * Updates an item single item with new value, if not exist then append
     * @param field field name
     * @param value new value to be updated with
     * @return returns whether value updated or not 
     */
    boolean updateSingleValue(String field, String value);
    
    /**
     * Gets all values for current item
     * @return returns all values in <b>Map&lt;String, List&lt;String&gt;&gt;</b> form,
     * key is field name and value is list of values
     */
    Map<String, Collection<String>> getAllValues();
}