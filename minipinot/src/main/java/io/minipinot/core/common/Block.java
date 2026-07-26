package io.minipinot.core.common;

/**
 * A {@code Block} is the unit of data returned by an {@link Operator#nextBlock()}. Depending on the
 * operator it can hold document ids, column values, or (in MiniPinot) a whole intermediate result
 * table. Mirrors Pinot's {@code org.apache.pinot.core.common.Block} marker interface.
 *
 * <p>MiniPinot has a single concrete block — {@link io.minipinot.core.datatable.DataTable} — which is
 * what every query/combine/instance operator emits, so its operators are all
 * {@code Operator<DataTable>}.
 */
public interface Block {
}
