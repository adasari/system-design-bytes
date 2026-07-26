package io.minipinot.core.operator.docidsets;

import io.minipinot.core.operator.dociditerators.BlockDocIdIterator;

/**
 * The set of documents matched by a filter operator, exposed as a lazily-consumable
 * {@link BlockDocIdIterator}. Concrete implementations pick the cheapest representation for their
 * access path: a contiguous range (sorted index), a lazy scan (no index), or a materialized bitmap
 * (inverted / range index). Mirrors Pinot's
 * {@code org.apache.pinot.core.operator.docidsets.BlockDocIdSet}.
 */
public interface BlockDocIdSet {

  BlockDocIdIterator iterator();
}
