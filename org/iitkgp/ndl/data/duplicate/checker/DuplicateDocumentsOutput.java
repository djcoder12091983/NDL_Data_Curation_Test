package org.iitkgp.ndl.data.duplicate.checker;

import java.util.Collection;

/**
 * Duplicates document JSON output
 * @author Debasis
 */
public class DuplicateDocumentsOutput {
	
	Collection<DuplicateDocument> documents;
	
	/**
	 * Sets duplicate documents detail
	 * @param documents duplicate documents detail
	 */
	public void setDocuments(Collection<DuplicateDocument> documents) {
		this.documents = documents;
	}
	
	/**
	 * Gets duplicate documents detail
	 * @return returns duplicate documents detail
	 */
	public Collection<DuplicateDocument> getDocuments() {
		return documents;
	}
}