package org.iitkgp.ndl.core;

/**
 * Text splitting logic to put the entry into Prefix-Tree
 * @see NDLMap
 * @see NDLSet 
 * @param <I> Input data
 * @param <O> Output data
 * @author Debasis
 */
public interface NDLFieldTokenSplitter<I, O> {

	/**
	 * Splits the text into tokens
	 * @param input input text to split
	 * @return returns tokens
	 */
	O split(I input);
}
