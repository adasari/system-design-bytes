package io.minipinot.core.operator.filter;

import io.minipinot.core.operator.docidsets.BlockDocIdSet;
import io.minipinot.core.operator.docidsets.OrDocIdSet;
import java.util.ArrayList;
import java.util.List;

/** Unions the matching documents of its children ({@code OR}). Mirrors Pinot's
 * {@code OrFilterOperator}. */
public final class OrFilterOperator extends BaseFilterOperator {
  private final List<BaseFilterOperator> _children;

  public OrFilterOperator(List<BaseFilterOperator> children, int numDocs) {
    super(numDocs);
    _children = children;
  }

  @Override
  public BlockDocIdSet getDocIds() {
    List<BlockDocIdSet> childSets = new ArrayList<>(_children.size());
    for (BaseFilterOperator child : _children) {
      childSets.add(child.getDocIds());
    }
    return new OrDocIdSet(childSets);
  }
}
