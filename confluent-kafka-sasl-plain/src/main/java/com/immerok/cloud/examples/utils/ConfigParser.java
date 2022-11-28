package com.immerok.cloud.examples.utils;

import java.io.IOException;
import java.util.Properties;

/** Utilities for parsing configuration. */
public class ConfigParser {

    /** Reads the given file into {@link Properties}. */
    public static Properties loadPropertiesResourceFile(String file) {
        try {
            Properties properties = new Properties();
            properties.load(ConfigParser.class.getResourceAsStream(file));
            return properties;
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Could not find configuration file '"
                            + file
                            + "'. "
                            + "Make sure the file is included in the resources folder of your Java project.");
        }
    }

    private ConfigParser() {
        // no instantiation
    }
}
