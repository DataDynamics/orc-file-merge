package io.datadynamics.ibk.orc.common;

/**
 * Extension of the {@link Ordered} interface, expressing a <em>priority</em>
 * ordering: {@code PriorityOrdered} objects are always applied before
 * <em>plain</em> {@link Ordered} objects regardless of their order values.
 *
 * <p>When sorting a set of {@code Ordered} objects, {@code PriorityOrdered}
 * objects and <em>plain</em> {@code Ordered} objects are effectively treated as
 * two separate subsets, with the set of {@code PriorityOrdered} objects preceding
 * the set of <em>plain</em> {@code Ordered} objects and with relative
 * ordering applied within those subsets.
 */
public interface PriorityOrdered extends Ordered {
}
