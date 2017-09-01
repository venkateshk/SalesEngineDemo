package sales.engine.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author vkavuluri
 */
public class ConfigHelper {
	
	private static Logger LOGGER = LoggerFactory.getLogger(ConfigHelper.class);
	
	private String env;
	
	public ConfigHelper(String[] args) {
		env = args[0];
	}
	
	public Config getConfig() {
		if (StringUtils.isNotBlank(env)) {
			LOGGER.info("Loading application config file {} ", "application-"+env);
			if (ConfigFactory.load("application-" + env).hasPath("sales-engine")) {
				return ConfigFactory.load("application-" + env).getConfig("sales-engine");
			}
		}
		throw new RuntimeException("Error loading environment specific config file : " + "application-" + env + ".conf");
	}
}