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
package org.apache.solr.search.grouping.distributed.shardresultserializer;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.grouping.Group2Docs;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.search.grouping.TopGroups2;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieIntField;
import org.apache.solr.search.grouping.Command;
import org.apache.solr.search.grouping.distributed.command.Group2Converter;
import org.apache.solr.search.grouping.distributed.command.QueryCommand;
import org.apache.solr.search.grouping.distributed.command.QueryCommandResult;
import org.apache.solr.search.grouping.distributed.command.SearchGroups2FieldCommand;
import org.apache.solr.search.grouping.distributed.command.TopGroups2FieldCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation for transforming {@link TopGroups} and {@link TopDocs} into a {@link NamedList} structure and
 * visa versa.
 */
public class TopGroups2ResultTransformer implements ShardResultTransformer<List<Command>, Map<String, ?>> {

  private final ResponseBuilder rb;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public TopGroups2ResultTransformer(ResponseBuilder rb) {
    this.rb = rb;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NamedList transform(List<Command> data) throws IOException {
    NamedList<NamedList> result = new NamedList<>();
    final IndexSchema schema = rb.req.getSearcher().getSchema();
    for (Command command : data) {
      NamedList commandResult;
      String key = command.getKey();
      if (TopGroups2FieldCommand.class.isInstance(command)) {
        TopGroups2FieldCommand fieldCommand = (TopGroups2FieldCommand) command;
        SchemaField groupField = schema.getField(fieldCommand.getKey());
        SchemaField groupParentField = schema.getField(fieldCommand.getParentKey());
        commandResult = serializeTopGroups(fieldCommand.result(), groupParentField, groupField);
        key = fieldCommand.getParentKey() + "." + command.getKey();
      }
      else if(SearchGroups2FieldCommand.class.isInstance(command)){
      	SearchGroups2FieldCommand fieldCommand = (SearchGroups2FieldCommand) command;
        SchemaField groupField = schema.getField(fieldCommand.getKey());
        SchemaField groupParentField = schema.getField(fieldCommand.getParentKey());
        TopGroups<BytesRef> tg = Group2Converter.fromMutable(groupParentField.getType(), new TrieIntField(), fieldCommand.getSecondPassGroupingCollector().getTopDocs(0));
        commandResult = serializeTopGroups(tg, groupParentField, groupField);
      	key += "."+key;  // expand key to like third pass
      }
      else if (QueryCommand.class.isInstance(command)) {
        QueryCommand queryCommand = (QueryCommand) command;
        commandResult = serializeTopDocs(queryCommand.result());
      } else {
        commandResult = null;
      }

      result.add(key, commandResult);
    }
    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, ?> transformToNative(NamedList<NamedList> shardResponse, Sort groupSort, Sort sortWithinGroup, String shard) {
    Map<String, Object> result = new HashMap<>();

    final IndexSchema schema = rb.req.getSearcher().getSchema();

    for (Map.Entry<String, NamedList> entry : shardResponse) {
      String key = entry.getKey();
      NamedList commandResult = entry.getValue();
      Integer totalGroupedHitCount = (Integer) commandResult.get("totalGroupedHitCount");
      Integer totalHits = (Integer) commandResult.get("totalHits");
      if (totalHits != null) {
        Integer matches = (Integer) commandResult.get("matches");
        Float maxScore = (Float) commandResult.get("maxScore");
        if (maxScore == null) {
          maxScore = Float.NaN;
        }

        @SuppressWarnings("unchecked")
        List<NamedList<Object>> documents = (List<NamedList<Object>>) commandResult.get("documents");
        ScoreDoc[] scoreDocs = transformToNativeShardDoc(documents, groupSort, shard, schema);
        final TopDocs topDocs;
        if (sortWithinGroup.equals(Sort.RELEVANCE)) {
          topDocs = new TopDocs(totalHits, scoreDocs, maxScore);
        } else {
          topDocs = new TopFieldDocs(totalHits, scoreDocs, sortWithinGroup.getSort(), maxScore);
        }
        result.put(key, new QueryCommandResult(topDocs, matches));
        continue;
      }

//      Long totalHitCount = (Long) commandResult.get("totalHitCount");

      List<Group2Docs<BytesRef, BytesRef>> groupDocs = new ArrayList<>();
      for (int i = 2; i < commandResult.size(); i++) {// go through all level 1 groups
        String parentGroupValue = commandResult.getName(i);
        @SuppressWarnings("unchecked")
        NamedList<NamedList<Object>> groupResult = (NamedList<NamedList<Object>>) commandResult.getVal(i);
        for(Map.Entry<String, NamedList<Object>> subEntry : groupResult){// go through all level 2 groups
        	String groupValue = subEntry.getKey();
        	NamedList<Object> subGroupResult = subEntry.getValue();
        	Integer totalGroupHits = (Integer) subGroupResult.get("totalHits");
        	Float maxScore = (Float) subGroupResult.get("maxScore");
        	if (maxScore == null) {
        		maxScore = Float.NaN;
        	}

        	@SuppressWarnings("unchecked")
        	List<NamedList<Object>> documents = (List<NamedList<Object>>) subGroupResult.get("documents");
        	ScoreDoc[] scoreDocs = transformToNativeShardDoc(documents, sortWithinGroup, shard, schema);

        	BytesRef groupValueRef = groupValue != null ? new BytesRef(groupValue) : null;
        	BytesRef parentGroupValueRef = parentGroupValue != null ? new BytesRef(parentGroupValue) : null;
//        	System.out.println("add : "+ parentGroupValue + " : "+ groupValue);
        	groupDocs.add(new Group2Docs<>(Float.NaN, maxScore, totalGroupHits, scoreDocs, parentGroupValueRef, groupValueRef, null));
        }
      }
      @SuppressWarnings("unchecked")
      Group2Docs<BytesRef, BytesRef>[] groupDocsArr = groupDocs.toArray(new Group2Docs[groupDocs.size()]);
      TopGroups2<BytesRef, BytesRef> topGroups = new TopGroups2<>(
      		groupSort.getSort(), sortWithinGroup.getSort(), totalGroupedHitCount, groupDocsArr, Float.NaN
      		);
        
      result.put(key, topGroups);
    }

    return result;
  }

  protected ScoreDoc[] transformToNativeShardDoc(List<NamedList<Object>> documents, Sort groupSort, String shard,
                                                 IndexSchema schema) {
    ScoreDoc[] scoreDocs = new ScoreDoc[documents.size()];
    int j = 0;
    for (NamedList<Object> document : documents) {
      Object docId = document.get("id");
      if (docId != null) {
        docId = docId.toString();
      } else {
        log.error("doc {} has null 'id'", document);
      }
      Float score = (Float) document.get("score");
      if (score == null) {
        score = Float.NaN;
      }
      Object[] sortValues = null;
      Object sortValuesVal = document.get("sortValues");
      if (sortValuesVal != null) {
        sortValues = ((List) sortValuesVal).toArray();
        for (int k = 0; k < sortValues.length; k++) {
          SchemaField field = groupSort.getSort()[k].getField() != null
              ? schema.getFieldOrNull(groupSort.getSort()[k].getField()) : null;
          if (field != null) {
            FieldType fieldType = field.getType();
            if (sortValues[k] != null) {
              sortValues[k] = fieldType.unmarshalSortValue(sortValues[k]);
            }
          }
        }
      } else {
        log.debug("doc {} has null 'sortValues'", document);
      }
      scoreDocs[j++] = new ShardDoc(score, sortValues, docId, shard);
    }
    return scoreDocs;
  }

  protected NamedList serializeTopGroups(TopGroups<BytesRef> d, SchemaField groupParentField, SchemaField groupField) throws IOException {
    NamedList<Object> result = new NamedList<>();
    TopGroups2<BytesRef, BytesRef> data = (TopGroups2<BytesRef, BytesRef>)d;
    result.add("totalGroupedHitCount", data.totalGroupedHitCount);
    result.add("totalHitCount", data.totalHitCount);
    if (data.totalGroupCount != null) {
      result.add("totalGroupCount", data.totalGroupCount);
    }

    final IndexSchema schema = rb.req.getSearcher().getSchema();
    SchemaField uniqueField = schema.getUniqueKeyField();
    for (Group2Docs<BytesRef, BytesRef> searchGroup : (Group2Docs<BytesRef, BytesRef>[])data.groups) {
//      String groupValue = searchGroup.groupValue != null ? groupField.getType().indexedToReadable(searchGroup.groupValue.utf8ToString()): null;
//      String groupParentValue = searchGroup.groupValue != null ? groupParentField.getType().indexedToReadable(searchGroup.groupParentValue.utf8ToString()): null;
      String groupValue = searchGroup.groupValue != null ? searchGroup.groupValue.utf8ToString(): null;
      String groupParentValue = searchGroup.groupValue != null ? searchGroup.groupParentValue.utf8ToString(): null;
      NamedList<NamedList<Object>> groupParentResult = (NamedList<NamedList<Object>>)result.get(groupParentValue);
      if(groupParentResult == null){
      	groupParentResult = new NamedList<>();
      	result.add(groupParentValue, groupParentResult);
      }
      NamedList<Object> groupResult = new NamedList<>();
      groupResult.add("totalHits", searchGroup.totalHits);
      if (!Float.isNaN(searchGroup.maxScore)) {
        groupResult.add("maxScore", searchGroup.maxScore);
      }

      List<NamedList<Object>> documents = new ArrayList<>();
      for (int i = 0; i < searchGroup.scoreDocs.length; i++) {
        NamedList<Object> document = new NamedList<>();
        documents.add(document);

        Document doc = retrieveDocument(uniqueField, searchGroup.scoreDocs[i].doc);
        document.add("id", uniqueField.getType().toExternal(doc.getField(uniqueField.getName())));
        if (!Float.isNaN(searchGroup.scoreDocs[i].score))  {
          document.add("score", searchGroup.scoreDocs[i].score);
        }
        if (!(searchGroup.scoreDocs[i] instanceof FieldDoc)) {
          continue; // thus don't add sortValues below
        }

        FieldDoc fieldDoc = (FieldDoc) searchGroup.scoreDocs[i];
        Object[] convertedSortValues  = new Object[fieldDoc.fields.length];
        for (int j = 0; j < fieldDoc.fields.length; j++) {
          Object sortValue  = fieldDoc.fields[j];
          Sort sortWithinGroup = rb.getGroupingSpec().getSortWithinGroup();
          SchemaField field = sortWithinGroup.getSort()[j].getField() != null ? schema.getFieldOrNull(sortWithinGroup.getSort()[j].getField()) : null;
          if (field != null) {
            FieldType fieldType = field.getType();
            if (sortValue != null) {
              sortValue = fieldType.marshalSortValue(sortValue);
            }
          }
          convertedSortValues[j] = sortValue;
        }
        document.add("sortValues", convertedSortValues);
      }
      groupResult.add("documents", documents);
      groupParentResult.add(groupValue, groupResult);
    }

    return result;
  }

  protected NamedList serializeTopDocs(QueryCommandResult result) throws IOException {
    NamedList<Object> queryResult = new NamedList<>();
    queryResult.add("matches", result.getMatches());
    queryResult.add("totalHits", result.getTopDocs().totalHits);
    // debug: assert !Float.isNaN(result.getTopDocs().getMaxScore()) == rb.getGroupingSpec().isNeedScore();
    if (!Float.isNaN(result.getTopDocs().getMaxScore())) {
      queryResult.add("maxScore", result.getTopDocs().getMaxScore());
    }
    List<NamedList> documents = new ArrayList<>();
    queryResult.add("documents", documents);

    final IndexSchema schema = rb.req.getSearcher().getSchema();
    SchemaField uniqueField = schema.getUniqueKeyField();
    for (ScoreDoc scoreDoc : result.getTopDocs().scoreDocs) {
      NamedList<Object> document = new NamedList<>();
      documents.add(document);

      Document doc = retrieveDocument(uniqueField, scoreDoc.doc);
      document.add("id", uniqueField.getType().toExternal(doc.getField(uniqueField.getName())));
      if (!Float.isNaN(scoreDoc.score))  {
        document.add("score", scoreDoc.score);
      }
      if (!FieldDoc.class.isInstance(scoreDoc)) {
        continue; // thus don't add sortValues below
      }

      FieldDoc fieldDoc = (FieldDoc) scoreDoc;
      Object[] convertedSortValues  = new Object[fieldDoc.fields.length];
      for (int j = 0; j < fieldDoc.fields.length; j++) {
        Object sortValue  = fieldDoc.fields[j];
        Sort groupSort = rb.getGroupingSpec().getGroupSort();
        SchemaField field = groupSort.getSort()[j].getField() != null
                          ? schema.getFieldOrNull(groupSort.getSort()[j].getField()) : null;
        if (field != null) {
          FieldType fieldType = field.getType();
          if (sortValue != null) {
            sortValue = fieldType.marshalSortValue(sortValue);
          }
        }
        convertedSortValues[j] = sortValue;
      }
      document.add("sortValues", convertedSortValues);
    }

    return queryResult;
  }

  private Document retrieveDocument(final SchemaField uniqueField, int doc) throws IOException {
    DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor(uniqueField.getName());
    rb.req.getSearcher().doc(doc, visitor);
    return visitor.getDocument();
  }

}
