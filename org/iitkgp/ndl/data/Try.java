package org.iitkgp.ndl.data;

/**
 * This interface encapsulates a trial logic
 * @param <I> Given input data
 * @param <O> Transformed output data after trial operation
 * @author Debasis
 */
public interface Try<I, O> {
	
	/**
	 * Encapsulates trial operation
	 * @param input given input
	 * @return returns transformed output data after trial operation
	 * @throws Exception throws exception in case of any transformation fails
	 */
	O trial(I input) throws Exception;
}