package sales.engine.config;

/**
 * @author vkavuluri
 */
public enum ConfigProperties {
	SPARK_APP_NAME("spark.app-name"),
	SPARK_MASTER("spark.master"),
    TUNING_NUMBER_OF_PARTITIONS("spark.tuning-partitions"),
	SALES_INPUT("spark.data-directories.input.sales"),
	CUSTOMERS_INPUT("spark.data-directories.input.customers"),
	TEMP_OUTPUT("spark.data-directories.output.temp"),
	SALES_BY_STATE_OUTPUT("spark.data-directories.output.sales-by-state"),;

	String value;
	
	ConfigProperties(String value) {
		this.value = value;
	}
	
	public String getValue() {
		return value;
	}
}