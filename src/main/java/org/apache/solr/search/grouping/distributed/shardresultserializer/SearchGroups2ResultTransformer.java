package org.apache.solr.search.grouping.distributed.shardresultserializer;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.AbstractSecondPassGrouping2Collector;
import org.apache.lucene.search.grouping.CollectedSearchGroup2;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.term.FunctionSecondPassGrouping2Collector;
import org.apache.lucene.search.grouping.term.TermSecondPassGrouping2Collector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.grouping.Command;
import org.apache.solr.search.grouping.distributed.command.Group2Converter;
import org.apache.solr.search.grouping.distributed.command.SearchGroups2FieldCommand;
import org.apache.solr.search.grouping.distributed.command.SearchGroupsFieldCommandResult;

import java.io.IOException;
import java.util.*;

/**
 * Implementation for transforming {@link SearchGroup} into a {@link NamedList} structure and visa versa.
 */
public class SearchGroups2ResultTransformer implements ShardResultTransformer<List<Command>, Map<String, CollectedSearchGroup2<BytesRef, BytesRef>>> {

  protected static final String TOP_GROUPS = "topGroups";
  protected static final String GROUP_COUNT = "groupCount";
  protected static final String GROUPS = "groups";

  private final SolrIndexSearcher searcher;

  public SearchGroups2ResultTransformer(SolrIndexSearcher searcher) {
    this.searcher = searcher;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NamedList transform(List<Command> data) throws IOException {
    final NamedList<NamedList> result = new NamedList<>(data.size());
    for (Command command : data) {
    	String key = null;
      final NamedList<Object> commandResult = new NamedList<>(2);
      if (SearchGroups2FieldCommand.class.isInstance(command)) {
        SearchGroups2FieldCommand fieldCommand = (SearchGroups2FieldCommand) command;
        final SearchGroupsFieldCommandResult fieldCommandResult = fieldCommand.result();
        key = fieldCommand.getParentKey();
        final Collection<SearchGroup<BytesRef>> searchGroups = fieldCommandResult.getSearchGroups();
        AbstractSecondPassGrouping2Collector<BytesRef, BytesRef> collector = fieldCommand.getSecondPassGroupingCollector();
        if (searchGroups != null) {
          result.add(TOP_GROUPS, serializeSearchGroup(collector, fieldCommand.getGroupSort()));
        }
      } else {
        continue;
      }

//      result.add(key, commandResult);
    }
    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, CollectedSearchGroup2<BytesRef, BytesRef>> transformToNative(NamedList<NamedList> shardResponse, Sort groupSort, Sort sortWithinGroup, String shard) {
  	int shardSize = 0;
  	if(shardResponse != null){
  		shardSize = shardResponse.size();
  	}
    final Map<String, CollectedSearchGroup2<BytesRef, BytesRef>> result = new HashMap<>(shardSize);
    if(shardResponse == null){
    	return result;
    }
    for (Map.Entry<String, NamedList> command : shardResponse) {
      NamedList<NamedList> topGroupsAndGroupCount = command.getValue();
      for(Map.Entry<String, NamedList> e : topGroupsAndGroupCount){
      	final String key = e.getKey();
      	CollectedSearchGroup2<BytesRef, BytesRef> collectedGroup = new CollectedSearchGroup2<>();
      	collectedGroup.groupValue = new BytesRef(key);
      	final Long count = (Long)e.getValue().get(GROUP_COUNT);
      	if(count != null){
      		collectedGroup.groupCount = count;
      	}
        Collection<SearchGroup<BytesRef>> searchGroups = new ArrayList<>();
        final NamedList<List<Comparable>> rawSearchGroups = (NamedList<List<Comparable>>) e.getValue().get(GROUPS);
        if (rawSearchGroups != null) {
          for (Map.Entry<String, List<Comparable>> rawSearchGroup : rawSearchGroups){
	          CollectedSearchGroup2<BytesRef, BytesRef> searchGroup = new CollectedSearchGroup2<>();
	          searchGroup.groupValue = rawSearchGroup.getKey() != null ? new BytesRef(rawSearchGroup.getKey()) : null;
	          searchGroup.sortValues = rawSearchGroup.getValue().toArray(new Comparable[rawSearchGroup.getValue().size()]);
	          for (int i = 0; i < searchGroup.sortValues.length; i++) {
	            SchemaField field = groupSort.getSort()[i].getField() != null ? searcher.getSchema().getFieldOrNull(groupSort.getSort()[i].getField()) : null;
	            if (field != null) {
	              FieldType fieldType = field.getType();
	              if (searchGroup.sortValues[i] != null) {
	                searchGroup.sortValues[i] = fieldType.unmarshalSortValue(searchGroup.sortValues[i]);
	              }
	            }
	          }
          searchGroups.add(searchGroup);
          }
        }
        collectedGroup.subGroups = searchGroups;
        result.put(key, collectedGroup);
      }

//      final Integer groupCount = (Integer) topGroupsAndGroupCount.get(GROUP_COUNT);
    }
    return result;
  }

  private NamedList serializeSearchGroup(AbstractSecondPassGrouping2Collector<?, ?> collector, Sort groupSort) {
    final NamedList<Object> result = new NamedList<>();
    Collection<CollectedSearchGroup2<BytesRef, BytesRef>> topGroupsFirstPass = null;
    if(collector instanceof TermSecondPassGrouping2Collector){
    	topGroupsFirstPass = ((TermSecondPassGrouping2Collector)collector).getTopGroupsNested(0, true);
    }
    else{
  xxx get working with G2  	topGroupsFirstPass = Group2Converter.fromMutable(((FunctionSecondPassGrouping2Collector)collector).getGroupParentSchemaField(),
    			((FunctionSecondPassGrouping2Collector)collector).getTopGroupsNested(0, true));
    }
    for (CollectedSearchGroup2<BytesRef, BytesRef> searchGroup : topGroupsFirstPass) {
    	// for each group found in the first pass get their values from the second pass
    	NamedList<Object> groupResult = new NamedList<>();
    	NamedList<Object[]> groupRecord = new NamedList<>();
    	for(SearchGroup<BytesRef> rec : searchGroup.subGroups){
    		Object[] convertedSortValues = new Object[rec.sortValues.length];
	      for (int i = 0; i < rec.sortValues.length; i++) {
	        Object sortValue = rec.sortValues[i];
	        SchemaField field = groupSort.getSort()[i].getField() != null ? searcher.getSchema().getFieldOrNull(groupSort.getSort()[i].getField()) : null;
	        if (field != null) {
	          FieldType fieldType = field.getType();
	          if (sortValue != null) {
	            sortValue = fieldType.marshalSortValue(sortValue);
	          }
	        }
	        convertedSortValues[i] = sortValue;
	      }
	      String groupValue = rec.groupValue != null ? rec.groupValue.utf8ToString() : null;
	      groupRecord.add(groupValue, convertedSortValues);
    	}
      String groupValue = searchGroup.groupValue != null ? searchGroup.groupValue.utf8ToString() : null;
      groupResult.add(GROUP_COUNT, searchGroup.groupCount);
      groupResult.add(GROUPS, groupRecord);
      result.add(groupValue, groupResult);
    }

    return result;
  }

}

