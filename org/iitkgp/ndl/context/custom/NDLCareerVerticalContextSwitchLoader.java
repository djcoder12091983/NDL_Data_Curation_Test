package org.iitkgp.ndl.context.custom;

/**
 * Career vertical context switching loader (custoim settings loader to additional operations)
 * @author debasis
 */
public class NDLCareerVerticalContextSwitchLoader extends NDLContextSwitchLoader {

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void load(NDLContextSwitch parent, String resource) throws Exception {
		super.load(parent, resource); // need to call super method
		
		// TODO domain specific validation context settings if any
		// NDLDataValidationContext.reload(<custom_validation_context_loader>);
		// NDLDataValidationUtils.reset();
	}
}