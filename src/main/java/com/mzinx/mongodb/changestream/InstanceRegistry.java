package com.mzinx.mongodb.changestream;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Registry of the live application instances (hostnames), typically maintained
 * by the {@code mongodb-spring-discovery} module: seeded from the instance
 * collection on startup and kept current through heartbeat change stream
 * events. A dedicated type (instead of a raw {@code Set<String>} bean) makes
 * injection unambiguous.
 */
public class InstanceRegistry {

    private final Set<String> hostnames = Collections.synchronizedSet(new LinkedHashSet<>());

    public void add(String hostname) {
        this.hostnames.add(hostname);
    }

    public void addAll(Collection<String> hostnames) {
        this.hostnames.addAll(hostnames);
    }

    public void remove(String hostname) {
        this.hostnames.remove(hostname);
    }

    public boolean contains(String hostname) {
        return this.hostnames.contains(hostname);
    }

    public int size() {
        return this.hostnames.size();
    }

    /** Sorted snapshot of the live instance hostnames. */
    public Set<String> all() {
        synchronized (this.hostnames) {
            return new TreeSet<>(this.hostnames);
        }
    }
}
