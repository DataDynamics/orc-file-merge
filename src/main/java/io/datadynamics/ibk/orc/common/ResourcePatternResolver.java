package io.datadynamics.ibk.orc.common;

import java.io.IOException;

/**
 * Strategy interface for resolving a location pattern (for example,
 * an Ant-style path pattern) into {@link Resource} objects.
 *
 * @see Resource
 * @see ResourceLoader
 */
public interface ResourcePatternResolver extends ResourceLoader {

    /**
     * Pseudo URL prefix for all matching resources from the class path: "classpath*:"
     * <p>This differs from ResourceLoader's classpath URL prefix in that it
     * retrieves all matching resources for a given name (e.g. "/beans.xml"),
     * for example in the root of all deployed JAR files.
     *
     * @see ResourceLoader#CLASSPATH_URL_PREFIX
     */
    String CLASSPATH_ALL_URL_PREFIX = "classpath*:";

    /**
     * Resolve the given location pattern into {@code Resource} objects.
     * <p>Overlapping resource entries that point to the same physical
     * resource should be avoided, as far as possible. The result should
     * have set semantics.
     *
     * @param locationPattern the location pattern to resolve
     * @return the corresponding {@code Resource} objects
     * @throws IOException in case of I/O errors
     */
    Resource[] getResources(String locationPattern) throws IOException;

}
