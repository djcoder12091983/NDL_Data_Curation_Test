package org.iitkgp.ndl.data;

/**
 * CSV configuration which encapsulates separator, quote, escape etc.
 * @author Debasis
 */
public class CSVConfiguration {
	
	char separator;
	char quote;
	char escape;
	
	int startIndex = 0; // 0 based start-index to read/write
	Character multipleValueSeparator = null;
	
	/**
	 * Constructor
	 * @param separator separator character to differentiate multiple values 
	 * @param quote quote character to wrap cell values
	 */
	public CSVConfiguration(char separator, char quote) {
		this.separator = separator;
		this.quote = quote;
	}
	
	/**
	 * Constructor
	 * @param separator separator character to differentiate multiple values
	 * @param quote quote character to wrap cell values
	 * @param escape escape character
	 */
	public CSVConfiguration(char separator, char quote, char escape) {
		this(separator, quote);
		this.escape = escape;
	}
	
	/**
	 * Sets start index to read/write
	 * @param startIndex start index (0 based)
	 */
	public void setStartIndex(int startIndex) {
		this.startIndex = startIndex;
	}
	
	/**
	 * Gets start index to read/write
	 * @return returns start index
	 */
	public int getStartIndex() {
		return startIndex;
	}
	
	/**
	 * gets escape character
	 * @return returns escape character
	 */
	public char getEscape() {
		return escape;
	}
	
	/**
	 * gets separator
	 * @return returns separator character
	 */
	public char getSeparator() {
		return separator;
	}
	
	/**
	 * gets quote character
	 * @return returns quote character
	 */
	public char getQuote() {
		return quote;
	}
	
	/**
	 * Sets multiple value separator character in a single cell
	 * @param multipleValueSeparator multiple value separator character
	 */
	public void setMultipleValueSeparator(char multipleValueSeparator) {
		this.multipleValueSeparator = multipleValueSeparator;
	}
	
	/**
	 * gets multiple value separator in a single cell
	 * @return returns multiple value separator
	 */
	public char getMultipleValueSeparator() {
		return multipleValueSeparator;
	}
	
	/**
	 * Multiple value separator available
	 * @return returns multiple value
	 */
	public boolean isMultipleValueSeparatorAvailable() {
		// multiple value separator
		return multipleValueSeparator != null;
	}

}
