/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.iitkgp.ndl.data;

import java.util.Collection;

/**
 * Data transformation logic
 * @param <I> Input data
 * @param <O> Output data
 * @author Aurghya
 */
public interface Transformer<I,O> {
	
	/**
	 * Transformer logic
	 * @param input data input
	 * @return returns output data
	 */
    Collection<O> transform(I input);
}
