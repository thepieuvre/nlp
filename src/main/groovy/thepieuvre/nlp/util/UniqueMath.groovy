package thepieuvre.nlp.util

import groovy.lang.Closure;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Groovy .unique {} is O(n^2),
 * http://jira.codehaus.org/browse/GROOVY-4606
 * <p/>
 * Hence our own implementation with a much better performance - O(n).
 *
 * @author Maxym Mykhalchuk
 */
public class UniqueMath {

    /**
     * Returns a new list with unique values only (unlike groovy's counterpart, does not modify source collection).
     * Has O(n * log n) complexity.
     * <p/>
     * A convenience method for making a collection unique using a Closure to determine duplicate (equal) items.
     * The closure takes a single parameter, the argument passed will be each element, and the closure
     * should return a value used for comparison.
     * <p/>
     * <pre class="groovyTestCase">assert [1,4] == [1,3,4,5].unique { it % 2 }</pre>
     *
     * @param collection a Collection
     * @param closure    a 1 arg Closure returning a value for comparison
     * @return new list with same elements in the same order (but without any duplicates)
     */
    public static <T> List<T> unique(Collection<T> collection, Closure closure) {
        if (closure.getMaximumNumberOfParameters() > 1) {
            throw new IllegalArgumentException("The closure supported must return a value used for comparison, " +
                    "not the comparator - use groovy's variant if your closure is a comparator");
        }
        LinkedHashMap<Object, T> answerMap = new LinkedHashMap<Object, T>((int) (collection.size() / 0.75)); // HashMap.DEFAULT_LOAD_FACTOR is not public, but = .75
        for (T t : collection) {
            Object compareByValue = closure.call(t);
            answerMap.put(compareByValue, t);
        }
        return new ArrayList<T>(answerMap.values());
    }
}