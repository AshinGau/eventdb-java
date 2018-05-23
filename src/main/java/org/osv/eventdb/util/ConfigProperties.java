package org.osv.eventdb.util;

import java.util.Properties;
import java.util.Set;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;

public class ConfigProperties {
	private String configFile = "config.properties";
	private Properties prop;

	public ConfigProperties() throws IOException {
		load();
	}

	public ConfigProperties(String configFile) throws IOException {
		this.configFile = configFile;
		load();
	}

	private void load() throws IOException {
		prop = new Properties();
		InputStream in = new FileInputStream(configFile);
		prop.load(in);
		in.close();
	}

	public Set<String> getPropertyNames() {
		return prop.stringPropertyNames();
	}

	public String getProperty(String key) {
		return prop.getProperty(key);
	}
}