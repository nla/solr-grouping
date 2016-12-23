
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.ws.Response;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.grouping.CollectedSearchGroup2;
import org.apache.lucene.search.grouping.Group2Docs;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.search.grouping.TopGroups2;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.grouping.Grouping2Specification;
import org.apache.solr.search.grouping.distributed.command.QueryCommandResult;

/**
 * Implementation of {@link EndResultTransformer} that keeps each grouped result separate in the final response.
 */
public class Grouped2EndResultTransformer implements EndResultTransformer {

  private final SolrIndexSearcher searcher;
  private final Grouping2Specification spec;

  public Grouped2EndResultTransformer(Grouping2Specification spec, SolrIndexSearcher searcher) {
    this.searcher = searcher;
    this.spec = spec;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void transform(Map<String, ?> result, ResponseBuilder rb, SolrDocumentSource solrDocumentSource) {
  	
    String field = ((Grouping2Specification)rb.getGroupingSpec()).getField();
    NamedList<Object> grouped = new SimpleOrderedMap<>();
    NamedList<Object> groupContainer = new SimpleOrderedMap<>();
    SchemaField schemaField = searcher.getSchema().getField(field);
    String subField = ((Grouping2Specification)rb.getGroupingSpec()).getSubField();
    SchemaField schemaSubField = searcher.getSchema().getField(subField);
    TopGroups2<BytesRef, BytesRef> topGroups = (TopGroups2<BytesRef, BytesRef>)rb.mergedTopGroups.get(field+"."+subField);
    if (topGroups == null) {
    	rb.rsp.add("grouped", grouped);
    	NamedList<Object> rec = new SimpleOrderedMap<>();
    	grouped.add(field, rec);
    	rec.add("matches", 0);
    	rec.add("groups", new ArrayList<>());
    	return;
    }

    for (Map.Entry<String, Collection<SearchGroup<BytesRef>>> entry : rb.mergedSearchGroups.entrySet()) {
    	Collection<SearchGroup<BytesRef>> groups = entry.getValue();
    	long totalCount = 0;
      List<NamedList> groupsList = new ArrayList<>();
    	for(SearchGroup<BytesRef> g : groups){
    		CollectedSearchGroup2<BytesRef, BytesRef> group = (CollectedSearchGroup2<BytesRef, BytesRef>)g;
    		totalCount += group.groupCount;
        NamedList<Object> groupRec = new SimpleOrderedMap<>();
        Object groupVal = schemaField.getType().toObject(schemaField.createField(schemaField.getType().indexedToReadable(group.groupValue.utf8ToString()), 1.0f)); 
        groupRec.add("groupValue", groupVal);
        NamedList<Object> subGroupContainer = new SimpleOrderedMap<>();
        NamedList<Object> subGroupRec = new SimpleOrderedMap<>();
        subGroupContainer.add(subField, subGroupRec);
        groupRec.add("subGrouped", subGroupContainer);
        subGroupRec.add("matches", group.groupCount);
        List<NamedList> subGroupsList = new ArrayList<>();
    		for(SearchGroup<BytesRef> subGroup : group.subGroups){
          NamedList<Object> docRec = new SimpleOrderedMap<>();
//      		CollectedSearchGroup2<BytesRef> subGroup = (CollectedSearchGroup2<BytesRef>)sg;
          Object subGroupVal = schemaSubField.getType().toObject(schemaSubField.createField(schemaSubField.getType().indexedToReadable(subGroup.groupValue.utf8ToString()), 1.0f));
          docRec.add("groupValue", subGroupVal);
          docRec.add("docList", getDocList(rb, topGroups, group.groupValue, subGroup.groupValue, solrDocumentSource));
          subGroupsList.add(docRec);
    		}
    		subGroupRec.add("groups", subGroupsList);
    		groupsList.add(groupRec);
    	}
    	long count = spec.getTotalHitCount();
      groupContainer.add("matches", count);
      groupContainer.add("groups", groupsList);
      grouped.add(field, groupContainer);
    }
    rb.rsp.add("grouped", grouped);
  }
//      Object value = entry.getValue();
//      if (TopGroups.class.isInstance(value)) {
//        @SuppressWarnings("unchecked")
//        TopGroups<BytesRef> topGroups = (TopGroups<BytesRef>) value;
//        NamedList<Object> parentCommand = new SimpleOrderedMap<>();
//        parentCommand.add("matches", rb.totalHitCount);
//        Integer totalGroupCount = rb.mergedGroupCounts.get(entry.getKey());
//        if (totalGroupCount != null) {
//          parentCommand.add("ngroups", totalGroupCount);
//        }
//
//        List<NamedList> groups = new ArrayList<>();
//        String key = entry.getKey();
//        String parentKey = key.substring(0, key.indexOf("."));
//        key = key.substring(key.indexOf(".")+1);
//        SchemaField parentGroupField = searcher.getSchema().getField(parentKey);
//        SchemaField groupField = searcher.getSchema().getField(key);
//        FieldType parentGroupFieldType = parentGroupField.getType();
//        FieldType groupFieldType = groupField.getType();
//        BytesRef parentGroupValue = null;
//        System.out.println("parentKey:"+parentKey +" key:"+ key);
//        System.out.println("grouped");
//        System.out.println("  "+parentKey);
//        List<NamedList> subGroups = new ArrayList<>();
//        for (Group2Docs<BytesRef> group : (Group2Docs<BytesRef>[])topGroups.groups) {
//          NamedList<Object> subCommand = new SimpleOrderedMap<>();
//          NamedList<Object> groupResult = new SimpleOrderedMap<>();
//          NamedList<Object> subGrouped = new SimpleOrderedMap<>();
//          BytesRef p = group.groupParentValue;
//          SimpleOrderedMap<Object> subGroupResult = new SimpleOrderedMap<>();
//          if(parentGroupValue == null || !parentGroupValue.bytesEquals(p)){
//          	parentGroupValue = p;
//          	groups.add(groupResult);
//            subGroups = new ArrayList<>();
//          	System.out.println("    groups");
//          	System.out.println("      groupValue:" +parentGroupFieldType.toObject(parentGroupField.createField(group.groupParentValue.utf8ToString(), 1.0f)));
//          	groupResult.add("groupValue", parentGroupFieldType.toObject(parentGroupField.createField(group.groupParentValue.utf8ToString(), 1.0f)));
//          	groupResult.add("subGroup", subGrouped);
//        		subGrouped.add(key, subCommand);
//          	System.out.println("      subGrouped");
//          	System.out.println("        "+ key);
//          	System.out.println("          groups");
//        		subCommand.add("matches", 999L);
//	          subCommand.add("groups",subGroups);
//         }
//          subGroupResult = new SimpleOrderedMap<>();
//	          if (group.groupValue != null) {
//	          	System.out.println("            groupValueS:"+ groupFieldType.toObject(groupField.createField(group.groupValue.utf8ToString(), 1.0f)));
//	            subGroupResult.add(
//	                "groupValueS", groupFieldType.toObject(groupField.createField(group.groupValue.utf8ToString(), 1.0f))
//	            );
//	          } else {
//	            subGroupResult.add("groupValue", null);
//	          }
//	          SolrDocumentList docList = new SolrDocumentList();
//	          docList.setNumFound(group.totalHits);
//	          if (!Float.isNaN(group.maxScore)) {
//	            docList.setMaxScore(group.maxScore);
//	          }
//	          docList.setStart(rb.getGroupingSpec().getGroupOffset());
//	          for (ScoreDoc scoreDoc : group.scoreDocs) {
//	            docList.add(solrDocumentSource.retrieve(scoreDoc));
//	          }
//	          System.out.println("            doclist");
//	          subGroupResult.add("doclistX", docList);
//	          subGroups.add(subGroupResult);
//	          System.out.println("add subGroups."+ subGroupResult.get("groupValueS"));
//	        }
//        parentCommand.add("groups", groups);        
//        grouped.add(parentKey, parentCommand);
//      } else if (QueryCommandResult.class.isInstance(value)) {
//        QueryCommandResult queryCommandResult = (QueryCommandResult) value;
//        NamedList<Object> command = new SimpleOrderedMap<>();
//        command.add("matches", queryCommandResult.getMatches());
//        SolrDocumentList docList = new SolrDocumentList();
//        docList.setNumFound(queryCommandResult.getTopDocs().totalHits);
//        if (!Float.isNaN(queryCommandResult.getTopDocs().getMaxScore())) {
//          docList.setMaxScore(queryCommandResult.getTopDocs().getMaxScore());
//        }
//        docList.setStart(rb.getGroupingSpec().getGroupOffset());
//        for (ScoreDoc scoreDoc :queryCommandResult.getTopDocs().scoreDocs){
//          docList.add(solrDocumentSource.retrieve(scoreDoc));
//        }
//        command.add("doclist", docList);
//        grouped.add(entry.getKey(), command);
//      }
//    }
//    rb.rsp.add("grouped", grouped);
//  }

  private SolrDocumentList getDocList(ResponseBuilder rb, TopGroups2<BytesRef, BytesRef> topGroups, 
  		  BytesRef groupValue, BytesRef subGroupValue, SolrDocumentSource solrDocumentSource){
    SolrDocumentList docList = new SolrDocumentList();
    Group2Docs<BytesRef, BytesRef> group = null;
    for(Group2Docs<BytesRef, BytesRef> g : topGroups.groups){
    	if(g.groupValue.bytesEquals(subGroupValue) && g.groupParentValue.bytesEquals(groupValue)){
    		group = g;
    		break;
    	}
    }
    docList.setNumFound(group.totalHits);
    if (!Float.isNaN(group.maxScore)) {
      docList.setMaxScore(group.maxScore);
    }
    docList.setStart(rb.getGroupingSpec().getGroupOffset());
    for (ScoreDoc scoreDoc : group.scoreDocs) {
      docList.add(solrDocumentSource.retrieve(scoreDoc));
    }
    return docList;
  }
}
