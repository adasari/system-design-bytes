package io.minipinot.core.operator.filter;

import io.minipinot.core.operator.docidsets.AndDocIdSet;
import io.minipinot.core.operator.docidsets.BlockDocIdSet;
import java.util.ArrayList;
import java.util.List;

/** Intersects the matching documents of its children ({@code AND}). Mirrors Pinot's
 * {@code AndFilterOperator}. */
public final class AndFilterOperator extends BaseFilterOperator {
  private final List<BaseFilterOperator> _children;

  public AndFilterOperator(List<BaseFilterOperator> children, int numDocs) {
    super(numDocs);
    _children = children;
  }

  @Override
  public BlockDocIdSet getDocIds() {
    List<BlockDocIdSet> childSets = new ArrayList<>(_children.size());
    for (BaseFilterOperator child : _children) {
      childSets.add(child.getDocIds());
    }
    return new AndDocIdSet(childSets);
  }
}
