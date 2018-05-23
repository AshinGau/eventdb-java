package org.osv.eventdb.util;

import java.util.Properties;
import java.util.Set;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;

/**
 * The ConfigProperties class represents a persistent set of eventdb-project's
 * default setting properties, such as settings of Hadoop, Hdfs, Hbase... and
 * reads ${projectDirctory}/config.properties as its default configuration file.
 */
public class ConfigProperties {
	private String configFile = "config.properties";
	private Properties prop;

	public ConfigProperties() throws IOException {
		load();
	}

	/**
	 * Custom configuration file
	 * 
	 * @param configFile path of the cutom configuration file
	 */
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

	/**
	 * Return a set of names of preperties.
	 */
	public Set<String> getPropertyNames() {
		return prop.stringPropertyNames();
	}

	/**
	 * Get value of the specific property
	 */
	public String getProperty(String key) {
		return prop.getProperty(key);
	}
}