package org.iitkgp.ndl.data.container;

/**
 * Data container initialization NULL configuration
 * @param <C> Data source initialization configuration
 * @author Debasis
 */
public class DataContainerNULLConfiguration<C> {

	// data source configuration
	C dataSourceConfig;
	
	/**
	 * Constructor
	 * @param dataSourceConfig data source configuration
	 */
	public DataContainerNULLConfiguration(C dataSourceConfig) {
		this.dataSourceConfig = dataSourceConfig;
	}
	
	/**
	 * Gets associated data source configuration
	 * @return returns associated data source configuration
	 */
	public C getDataSourceConfig() {
		return dataSourceConfig;
	}
}