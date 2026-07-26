package io.minipinot.forward;

/**
 * Reads dictionary ids out of a single-valued forward index by document id. The forward index is
 * the mapping {@code docId -> dictId}; combined with the dictionary ({@code dictId -> value}) it
 * reconstructs any column value. Mirrors Pinot's {@code ForwardIndexReader} for the
 * dictionary-encoded, single-valued case.
 */
public interface ForwardIndexReader {

  int getDictId(int docId);

  int getNumDocs();
}
