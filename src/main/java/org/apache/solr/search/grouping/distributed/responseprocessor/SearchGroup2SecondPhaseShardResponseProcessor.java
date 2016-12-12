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
package org.apache.solr.search.grouping.distributed.responseprocessor;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.CollectedSearchGroup2;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.grouping.Grouping2Specification;
import org.apache.solr.search.grouping.distributed.ShardResponseProcessor;
import org.apache.solr.search.grouping.distributed.shardresultserializer.SearchGroups2ResultTransformer;

/**
 * Concrete implementation for merging {@link SearchGroup} instances from shard responses.
 */
public class SearchGroup2SecondPhaseShardResponseProcessor implements ShardResponseProcessor {

  /**
   * {@inheritDoc}
   */
  @Override
  public void process(ResponseBuilder rb, ShardRequest shardRequest) {
    SortSpec ss = rb.getSortSpec();
    Sort groupSort = rb.getGroupingSpec().getGroupSort();
    Grouping2Specification groupSpec = (Grouping2Specification)rb.getGroupingSpec();
    final String field = groupSpec.getField();
    Sort sortWithinGroup = rb.getGroupingSpec().getSortWithinGroup();
    if (sortWithinGroup == null) { // TODO prevent it from being null in the first place
      sortWithinGroup = Sort.RELEVANCE;
    }

    final Map<String, List<Collection<SearchGroup<BytesRef>>>> commandSearchGroups = new HashMap<>(1, 1.0f);
    final Map<String, Map<SearchGroup<BytesRef>, Set<String>>> tempSearchGroupToShards = new HashMap<>(1, 1.0f);
    commandSearchGroups.put(field, new ArrayList<Collection<SearchGroup<BytesRef>>>(shardRequest.responses.size()));
    tempSearchGroupToShards.put(field, new HashMap<SearchGroup<BytesRef>, Set<String>>());
    if (!rb.searchGroupToShards.containsKey(field)) {
    	rb.searchGroupToShards.put(field, new HashMap<SearchGroup<BytesRef>, Set<String>>());
    }

    SearchGroups2ResultTransformer serializer = new SearchGroups2ResultTransformer(rb.req.getSearcher());
    try {
      int maxElapsedTime = 0;
//      int hitCountDuringFirstPhase = 0;

      NamedList<Object> shardInfo = null;
      if (rb.req.getParams().getBool(ShardParams.SHARDS_INFO, false)) {
        shardInfo = new SimpleOrderedMap<>(shardRequest.responses.size());
        rb.rsp.getValues().add(ShardParams.SHARDS_INFO + ".secondPhase", shardInfo);
      }

      Map<String, List<Collection<SearchGroup<BytesRef>>>> all = new HashMap<>();
      Map<String, Long> groupCount = new HashMap<>();
      for (ShardResponse srsp : shardRequest.responses) {
        if (shardInfo != null) {
          SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>(4);

          if (srsp.getException() != null) {
            Throwable t = srsp.getException();
            if (t instanceof SolrServerException) {
              t = ((SolrServerException) t).getCause();
            }
            nl.add("error", t.toString());
            StringWriter trace = new StringWriter();
            t.printStackTrace(new PrintWriter(trace));
            nl.add("trace", trace.toString());
          } else {
            nl.add("numFound", (Integer) srsp.getSolrResponse().getResponse().get("totalHitCount"));
          }
          if (srsp.getSolrResponse() != null) {
            nl.add("time", srsp.getSolrResponse().getElapsedTime());
          }
          if (srsp.getShardAddress() != null) {
            nl.add("shardAddress", srsp.getShardAddress());
          }
          shardInfo.add(srsp.getShard(), nl);
        }
        if (rb.req.getParams().getBool(ShardParams.SHARDS_TOLERANT, false) && srsp.getException() != null) {
          if(rb.rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY) == null) {
            rb.rsp.getResponseHeader().add(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
          }
          continue; // continue if there was an error and we're tolerant.  
        }
        maxElapsedTime = (int) Math.max(maxElapsedTime, srsp.getSolrResponse().getElapsedTime());
        @SuppressWarnings("unchecked")
        NamedList<NamedList> secondPhaseResult = (NamedList<NamedList>) srsp.getSolrResponse().getResponse().get("secondPhase");
        final Map<String, CollectedSearchGroup2<BytesRef, BytesRef>> result = serializer.transformToNative(secondPhaseResult, groupSort, sortWithinGroup, srsp.getShard());
//        for (String field : result.keySet()) {
//          final CollectedSearchGroup2<BytesRef> firstPhaseGroup = result.get(field);
//
//          final Long groupCount = firstPhaseGroup.groupCount;
//
//          final Collection<CollectedSearchGroup2<BytesRef>> searchGroups = firstPhaseGroup.subGroups;
//          if (searchGroups == null) {
//            continue;
//          }

//          commandSearchGroups.get(field).add(searchGroups);
//          for (SearchGroup<BytesRef> searchGroup : searchGroups) {
//            Map<SearchGroup<BytesRef>, java.util.Set<String>> map = tempSearchGroupToShards.get(field);
//            Set<String> shards = map.get(searchGroup);
//            if (shards == null) {
//              shards = new HashSet<>();
//              map.put(searchGroup, shards);
//            }
//            shards.add(srsp.getShard());
//          }
//        }
//        hitCountDuringFirstPhase += (Integer) srsp.getSolrResponse().getResponse().get("totalHitCount");
//      }
//      rb.totalHitCount = hitCountDuringFirstPhase;
        rb.firstPhaseElapsedTime = maxElapsedTime;
        for (String groupField : result.keySet()) {
        	CollectedSearchGroup2<BytesRef, BytesRef> parent = result.get(groupField);
        	Collection<SearchGroup<BytesRef>> topGroups = parent.subGroups;
        	List<Collection<SearchGroup<BytesRef>>> group = all.get(groupField);
        	if(group == null){
        		group = new ArrayList<>();
        		all.put(groupField, group);
        	}
        	group.add(topGroups);
        	// and get the group count
        	Long count = groupCount.get(groupField);
        	if(count == null){
        		count = 0L;
        	}
        	groupCount.put(groupField, (count + parent.groupCount));
        }
      }
      for(Map.Entry<String, List<Collection<SearchGroup<BytesRef>>>> e : all.entrySet()){ 
      	BytesRef groupKey = new BytesRef(e.getKey());
      	Collection<SearchGroup<BytesRef>> mergedTopGroups = SearchGroup.merge(e.getValue(), groupSpec.getGroupOffset(), groupSpec.getGroupLimit(), groupSort);
      	Iterator<SearchGroup<BytesRef>> i = rb.mergedSearchGroups.get(field).iterator();
      	while(i.hasNext()){
      		SearchGroup<BytesRef> g = i.next();
      		if(g.groupValue != null && groupKey.bytesEquals(g.groupValue)){
      			if(g instanceof CollectedSearchGroup2){
      				((CollectedSearchGroup2<BytesRef, BytesRef>)g).subGroups = mergedTopGroups;
      				((CollectedSearchGroup2<BytesRef, BytesRef>)g).groupCount = groupCount.get(e.getKey());
      			}
      			else{
      				CollectedSearchGroup2<BytesRef, BytesRef> newGroup = new CollectedSearchGroup2<>(g);
      				newGroup.subGroups = mergedTopGroups;
      				newGroup.groupCount = groupCount.get(e.getKey());
      				i.remove();
      				rb.mergedSearchGroups.get(field).add(newGroup);
      				break;
      			}
      		}
      	}
      }

//	    for (SearchGroup<BytesRef> mergedTopGroup : mergedTopGroups) {
//	    	rb.searchGroupToShards.get(groupField).put(mergedTopGroup, tempSearchGroupToShards.get(groupField).get(mergedTopGroup));
//	    }
	      
   
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

}
