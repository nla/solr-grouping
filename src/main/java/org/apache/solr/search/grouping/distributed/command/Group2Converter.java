package org.apache.solr.search.grouping.distributed.command;

import java.util.Collection;

import org.apache.lucene.search.grouping.CollectedSearchGroup2;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.solr.schema.SchemaField;

public class Group2Converter{
  public static Collection<SearchGroup<BytesRef>> fromMutable(SchemaField field, Collection<SearchGroup<MutableValue>> values) {
  	return GroupConverter.fromMutable(field, values);
  }
  public static Collection<SearchGroup<MutableValue>> toMutable(SchemaField field, Collection<SearchGroup<BytesRef>> values) {
  	return GroupConverter.toMutable(field, values);
  }
  static TopGroups<BytesRef> fromMutable(SchemaField field, TopGroups<MutableValue> values) {
  	return GroupConverter.fromMutable(field, values);
  }
}
  
