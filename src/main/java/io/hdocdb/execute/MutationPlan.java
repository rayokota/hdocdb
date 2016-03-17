package io.hdocdb.execute;

import org.ojai.store.exceptions.StoreException;

public interface MutationPlan {

    boolean execute() throws StoreException;
}
