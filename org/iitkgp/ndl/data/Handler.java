package org.iitkgp.ndl.data;

/**
 * It's a hook to take action on some event
 * @param <I> input source
 * @param <O> output after handling input source
 * @author Debasis
 */
public interface Handler<I, O> {
	
	/**
	 * Action definition
	 * @param input Action on the basis of input
	 * @return returns output object after operation is done 
	 * @throws Exception throws exception if action happens with any error
	 */
	O handle(I input) throws Exception;
}