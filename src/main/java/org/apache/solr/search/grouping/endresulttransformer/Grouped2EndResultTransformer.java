
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
package org.apache.solr.search.grouping.endresulttransformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.grouping.Group2Docs;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.grouping.distributed.command.QueryCommandResult;

/**
 * Implementation of {@link EndResultTransformer} that keeps each grouped result separate in the final response.
 */
public class Grouped2EndResultTransformer implements EndResultTransformer {

  private final SolrIndexSearcher searcher;

  public Grouped2EndResultTransformer(SolrIndexSearcher searcher) {
    this.searcher = searcher;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void transform(Map<String, ?> result, ResponseBuilder rb, SolrDocumentSource solrDocumentSource) {
    NamedList<Object> grouped = new SimpleOrderedMap<>();
    for (Map.Entry<String, ?> entry : result.entrySet()) {
      Object value = entry.getValue();
      if (TopGroups.class.isInstance(value)) {
        @SuppressWarnings("unchecked")
        TopGroups<BytesRef> topGroups = (TopGroups<BytesRef>) value;
        NamedList<Object> parentCommand = new SimpleOrderedMap<>();
        parentCommand.add("matches", rb.totalHitCount);
        Integer totalGroupCount = rb.mergedGroupCounts.get(entry.getKey());
        if (totalGroupCount != null) {
          parentCommand.add("ngroups", totalGroupCount);
        }

        List<NamedList> groups = new ArrayList<>();
        String key = entry.getKey();
        String parentKey = key.substring(0, key.indexOf("."));
        key = key.substring(key.indexOf(".")+1);
        SchemaField parentGroupField = searcher.getSchema().getField(parentKey);
        SchemaField groupField = searcher.getSchema().getField(key);
        FieldType parentGroupFieldType = parentGroupField.getType();
        FieldType groupFieldType = groupField.getType();
        BytesRef parentGroupValue = null;
        System.out.println("parentKey:"+parentKey +" key:"+ key);
        System.out.println("grouped");
        System.out.println("  "+parentKey);
        List<NamedList> subGroups = new ArrayList<>();
        for (Group2Docs<BytesRef> group : (Group2Docs<BytesRef>[])topGroups.groups) {
          NamedList<Object> subCommand = new SimpleOrderedMap<>();
          NamedList<Object> groupResult = new SimpleOrderedMap<>();
          NamedList<Object> subGrouped = new SimpleOrderedMap<>();
          BytesRef p = group.groupParentValue;
          SimpleOrderedMap<Object> subGroupResult = new SimpleOrderedMap<>();
          if(parentGroupValue == null || !parentGroupValue.bytesEquals(p)){
          	parentGroupValue = p;
          	groups.add(groupResult);
            subGroups = new ArrayList<>();
          	System.out.println("    groups");
          	System.out.println("      groupValue:" +parentGroupFieldType.toObject(parentGroupField.createField(group.groupParentValue.utf8ToString(), 1.0f)));
          	groupResult.add("groupValue", parentGroupFieldType.toObject(parentGroupField.createField(group.groupParentValue.utf8ToString(), 1.0f)));
          	groupResult.add("subGroup", subGrouped);
        		subGrouped.add(key, subCommand);
          	System.out.println("      subGrouped");
          	System.out.println("        "+ key);
          	System.out.println("          groups");
        		subCommand.add("matches", 999L);
	          subCommand.add("groups",subGroups);
         }
          subGroupResult = new SimpleOrderedMap<>();
	          if (group.groupValue != null) {
	          	System.out.println("            groupValueS:"+ groupFieldType.toObject(groupField.createField(group.groupValue.utf8ToString(), 1.0f)));
	            subGroupResult.add(
	                "groupValueS", groupFieldType.toObject(groupField.createField(group.groupValue.utf8ToString(), 1.0f))
	            );
	          } else {
	            subGroupResult.add("groupValue", null);
	          }
	          SolrDocumentList docList = new SolrDocumentList();
	          docList.setNumFound(group.totalHits);
	          if (!Float.isNaN(group.maxScore)) {
	            docList.setMaxScore(group.maxScore);
	          }
	          docList.setStart(rb.getGroupingSpec().getGroupOffset());
	          for (ScoreDoc scoreDoc : group.scoreDocs) {
	            docList.add(solrDocumentSource.retrieve(scoreDoc));
	          }
	          System.out.println("            doclist");
	          subGroupResult.add("doclistX", docList);
	          subGroups.add(subGroupResult);
	          System.out.println("add subGroups."+ subGroupResult.get("groupValueS"));
	        }
        parentCommand.add("groups", groups);        
        grouped.add(parentKey, parentCommand);
      } else if (QueryCommandResult.class.isInstance(value)) {
        QueryCommandResult queryCommandResult = (QueryCommandResult) value;
        NamedList<Object> command = new SimpleOrderedMap<>();
        command.add("matches", queryCommandResult.getMatches());
        SolrDocumentList docList = new SolrDocumentList();
        docList.setNumFound(queryCommandResult.getTopDocs().totalHits);
        if (!Float.isNaN(queryCommandResult.getTopDocs().getMaxScore())) {
          docList.setMaxScore(queryCommandResult.getTopDocs().getMaxScore());
        }
        docList.setStart(rb.getGroupingSpec().getGroupOffset());
        for (ScoreDoc scoreDoc :queryCommandResult.getTopDocs().scoreDocs){
          docList.add(solrDocumentSource.retrieve(scoreDoc));
        }
        command.add("doclist", docList);
        grouped.add(entry.getKey(), command);
      }
    }
    rb.rsp.add("grouped", grouped);
  }

}
