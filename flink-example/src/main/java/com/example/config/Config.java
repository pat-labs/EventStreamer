package com.example.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import java.lang.reflect.Field;

/**
 * The Config class reads configuration values from various sources, including properties files,
 * command-line arguments, and environment variables. It uses reflection to dynamically set the values
 * of its fields based on the provided ParameterTool.
 *
 * <p>This class is designed to be extended by other classes that may have additional configuration attributes.</p>
 *
 * <p>Example usage:</p>
 * <pre>
 * {@code
 * ParameterTool parameterTool = ParameterTool.fromPropertiesFile(Thread.currentThread().getContextClassLoader().getResourceAsStream("flink.properties"))
 *     .mergeWith(ParameterTool.fromArgs(args))
 *     .mergeWith(ParameterTool.fromSystemProperties())
 *     .mergeWith(ParameterTool.fromMap(System.getenv()));
 * Config config = new Config(parameterTool);
 * }
 * </pre>
 */

@Slf4j
public class Config {
    public Config(ParameterTool parameterTool) {
        for (String key : parameterTool.getProperties().stringPropertyNames()) {
            String variableName = key.replace(".", "");
            try {
                Field field = this.getClass().getDeclaredField(variableName);
                field.setAccessible(true);
                field.set(this, parameterTool.get(key));
            } catch (NoSuchFieldException | IllegalAccessException e) {
                log.error("Error al obtener el valor del variable " + key, e);
            }
        }
    }
}