package org.iitkgp.ndl.context;

import org.iitkgp.ndl.data.validator.NDLFieldSchemaDetail;
import org.iitkgp.ndl.data.validator.NDLSchemaDetail;
import org.iitkgp.ndl.service.exception.ServiceRequestException;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.iitkgp.ndl.util.NDLServiceParameters;
import org.iitkgp.ndl.util.NDLServiceUtils;
import org.iitkgp.ndl.validator.exception.NDLSchemaDetailLoadException;

/**
 * Default NDL context validation loader
 * @author debasis
 */
public class DefaultNDLDataValidationContextLoader implements NDLDataValidationContextLoader {
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public NDLSchemaDetail loadSchemaDetail() throws NDLSchemaDetailLoadException {
		// schema detail load
		NDLServiceParameters parameters = new NDLServiceParameters();
		parameters.addParameter("schema[]", "general");
		String response = null;
		try {
			response = NDLServiceUtils.request(NDLServiceUtils.BASE_SERVICE_URL + "getSchema", "POST",
					parameters.getParameters());
		} catch(ServiceRequestException ex) {
			// error
			throw new NDLSchemaDetailLoadException(
					"ERROR:(" + NDLServiceUtils.BASE_SERVICE_URL + ") Loading schema detail: " + ex.getMessage(), ex);
		}
		NDLSchemaDetail detail = NDLDataUtils.HTML_ESCAPE_GSON.fromJson(response, NDLSchemaDetail.class);
		if(detail.available()) {
			// available
			NDLSchemaDetail schemaDetail = detail;
			schemaDetail.loadMoreDetails(); // load more details
			return schemaDetail;
		} else {
			// error
			throw new NDLSchemaDetailLoadException("ERROR: Loading schema detail");
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public NDLFieldSchemaDetail loadSchemaDetail(String field) throws NDLSchemaDetailLoadException {
		// individual schema detail
		NDLServiceParameters parameters = new NDLServiceParameters();
		parameters.addParameter("fields[]", field);
		String response = NDLServiceUtils.request(NDLServiceUtils.BASE_SERVICE_URL + "getConstraints", "POST",
				parameters.getParameters());
		NDLFieldSchemaDetail detail = NDLDataUtils.HTML_ESCAPE_GSON.fromJson(response, NDLFieldSchemaDetail.class);
		if(detail.available()) {
			// available
			return detail;
		} else {
			// error
			throw new NDLSchemaDetailLoadException("ERROR: Loading schema detail");
		}
	}
}