package ta.nemahuta.neo4j.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for fields, denoting the field is a configuration entry.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface ConfigurationKey {

    /**
     * @return the key value to be used to create the apache configuration
     */
    String value() default "";
}
