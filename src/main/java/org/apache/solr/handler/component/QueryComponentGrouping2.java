package org.apache.solr.handler.component;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.CollectedSearchGroup2;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.Group2Params;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.MoreLikeThisParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.Grouping;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.SortSpecParsing;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.grouping.CommandHandler;
import org.apache.solr.search.grouping.Grouping2Specification;
import org.apache.solr.search.grouping.GroupingSpecification;
import org.apache.solr.search.grouping.distributed.ShardRequestFactory;
import org.apache.solr.search.grouping.distributed.ShardResponseProcessor;
import org.apache.solr.search.grouping.distributed.command.QueryCommand.Builder;
import org.apache.solr.search.grouping.distributed.command.SearchGroups2FieldCommand;
import org.apache.solr.search.grouping.distributed.command.SearchGroupsFieldCommand;
import org.apache.solr.search.grouping.distributed.command.TopGroups2FieldCommand;
import org.apache.solr.search.grouping.distributed.command.TopGroupsFieldCommand;
import org.apache.solr.search.grouping.distributed.requestfactory.SearchGroupsRequestFactory;
import org.apache.solr.search.grouping.distributed.requestfactory.StoredFieldsShardRequestFactory;
import org.apache.solr.search.grouping.distributed.requestfactory.TopGroups2ShardRequestFactory;
import org.apache.solr.search.grouping.distributed.requestfactory.TopGroupsShardRequestFactory;
import org.apache.solr.search.grouping.distributed.responseprocessor.SearchGroup2SecondPhaseShardResponseProcessor;
import org.apache.solr.search.grouping.distributed.responseprocessor.SearchGroup2ShardResponseProcessor;
import org.apache.solr.search.grouping.distributed.responseprocessor.StoredFieldsShardResponseProcessor;
import org.apache.solr.search.grouping.distributed.responseprocessor.TopGroups2ShardResponseProcessor;
import org.apache.solr.search.grouping.distributed.shardresultserializer.SearchGroups2ResultTransformer;
import org.apache.solr.search.grouping.distributed.shardresultserializer.SearchGroupsResultTransformer;
import org.apache.solr.search.grouping.distributed.shardresultserializer.TopGroups2ResultTransformer;
import org.apache.solr.search.grouping.distributed.shardresultserializer.TopGroupsResultTransformer;
import org.apache.solr.search.grouping.endresulttransformer.EndResultTransformer;
import org.apache.solr.search.grouping.endresulttransformer.Grouped2EndResultTransformer;
import org.apache.solr.search.stats.StatsCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryComponentGrouping2 extends QueryComponent{
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
	@Override
  protected void prepareGrouping(ResponseBuilder rb) throws IOException {

    SolrQueryRequest req = rb.req;
    SolrParams params = req.getParams();

    if (null != rb.getCursorMark()) {
      // It's hard to imagine, conceptually, what it would mean to combine
      // grouping with a cursor - so for now we just don't allow the combination at all
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can not use Grouping with " +
                              CursorMarkParams.CURSOR_MARK_PARAM);
    }

    SolrIndexSearcher searcher = rb.req.getSearcher();
    Grouping2Specification groupingSpec = new Grouping2Specification();
    rb.setGroupingSpec(groupingSpec);

    final SortSpec sortSpec = rb.getSortSpec();

    //TODO: move weighting of sort
    Sort groupSort = searcher.weightSort(sortSpec.getSort());
    if (groupSort == null) {
      groupSort = Sort.RELEVANCE;
    }

    // groupSort defaults to sort
    String groupSortStr = params.get(GroupParams.GROUP_SORT);
    //TODO: move weighting of sort
    Sort sortWithinGroup = groupSortStr == null ?  groupSort : searcher.weightSort(SortSpecParsing.parseSortSpec(groupSortStr, req).getSort());
    if (sortWithinGroup == null) {
      sortWithinGroup = Sort.RELEVANCE;
    }

    groupingSpec.setSortWithinGroup(sortWithinGroup);
    groupingSpec.setGroupSort(groupSort);

    String formatStr = params.get(GroupParams.GROUP_FORMAT, Grouping.Format.grouped.name());
    Grouping.Format responseFormat;
    try {
       responseFormat = Grouping.Format.valueOf(formatStr);
    } catch (IllegalArgumentException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, String.format(Locale.ROOT, "Illegal %s parameter", GroupParams.GROUP_FORMAT));
    }
    groupingSpec.setResponseFormat(responseFormat);

    groupingSpec.setFields(params.getParams(GroupParams.GROUP_FIELD));
    groupingSpec.setQueries(params.getParams(GroupParams.GROUP_QUERY));
    groupingSpec.setFunctions(params.getParams(GroupParams.GROUP_FUNC));
    groupingSpec.setGroupOffset(params.getInt(GroupParams.GROUP_OFFSET, 0));
    groupingSpec.setGroupLimit(params.getInt(GroupParams.GROUP_LIMIT, 1));
    groupingSpec.setOffset(sortSpec.getOffset());
    groupingSpec.setLimit(sortSpec.getCount());
    groupingSpec.setIncludeGroupCount(params.getBool(GroupParams.GROUP_TOTAL_COUNT, false));
    groupingSpec.setMain(params.getBool(GroupParams.GROUP_MAIN, false));
    groupingSpec.setNeedScore((rb.getFieldFlags() & SolrIndexSearcher.GET_SCORES) != 0);
    groupingSpec.setTruncateGroups(params.getBool(GroupParams.GROUP_TRUNCATE, false));
    
    // check for second level grouping
    for(String f : groupingSpec.getFields()){
    	String[] names = params.getParams(GroupParams.GROUP_FIELD + "."+f);
    	if(names != null && names.length > 0){
    		if(names.length > 1){
    			throw new IllegalStateException("Second level group can only be single.");
    		}
    		groupingSpec.setParentFields(f, names[0]);
    	}
    }
  }
	
	@Override
	public void process(ResponseBuilder rb) throws IOException{

    Grouping2Specification groupingSpec = (Grouping2Specification)rb.getGroupingSpec();
    if (groupingSpec == null) {
    	// not grouping so let super class handle
    	super.process(rb);
    	return;
    }		

    log.debug("process: {}", rb.req.getParams());
    
    SolrQueryRequest req = rb.req;
    SolrParams params = req.getParams();
    if (!params.getBool(COMPONENT_NAME, true)) {
      return;
    }
    SolrIndexSearcher searcher = req.getSearcher();

    StatsCache statsCache = req.getCore().getStatsCache();
    
    int purpose = params.getInt(ShardParams.SHARDS_PURPOSE, ShardRequest.PURPOSE_GET_TOP_IDS);
    if ((purpose & ShardRequest.PURPOSE_GET_TERM_STATS) != 0) {
      statsCache.returnLocalStats(rb, searcher);
      return;
    }
    // check if we need to update the local copy of global dfs
    if ((purpose & ShardRequest.PURPOSE_SET_TERM_STATS) != 0) {
      // retrieve from request and update local cache
      statsCache.receiveGlobalStats(req);
    }

    SolrQueryResponse rsp = rb.rsp;
    IndexSchema schema = searcher.getSchema();

    // Optional: This could also be implemented by the top-level searcher sending
    // a filter that lists the ids... that would be transparent to
    // the request handler, but would be more expensive (and would preserve score
    // too if desired).
    String ids = params.get(ShardParams.IDS);
    if (ids != null) {
      SchemaField idField = schema.getUniqueKeyField();
      List<String> idArr = StrUtils.splitSmart(ids, ",", true);
      int[] luceneIds = new int[idArr.size()];
      int docs = 0;
      for (int i=0; i<idArr.size(); i++) {
        int id = searcher.getFirstMatch(
                new Term(idField.getName(), idField.getType().toInternal(idArr.get(i))));
        if (id >= 0)
          luceneIds[docs++] = id;
      }

      DocListAndSet res = new DocListAndSet();
      res.docList = new DocSlice(0, docs, luceneIds, null, docs, 0);
      if (rb.isNeedDocSet()) {
        // TODO: create a cache for this!
        List<Query> queries = new ArrayList<>();
        queries.add(rb.getQuery());
        List<Query> filters = rb.getFilters();
        if (filters != null) queries.addAll(filters);
        res.docSet = searcher.getDocSet(queries);
      }
      rb.setResults(res);

      ResultContext ctx = new ResultContext();
      ctx.docs = rb.getResults().docList;
      ctx.query = null; // anything?
      rsp.addResponse(ctx);
      return;
    }

    // -1 as flag if not set.
    long timeAllowed = params.getLong(CommonParams.TIME_ALLOWED, -1L);
    if (null != rb.getCursorMark() && 0 < timeAllowed) {
      // fundamentally incompatible
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can not search using both " +
                              CursorMarkParams.CURSOR_MARK_PARAM + " and " + CommonParams.TIME_ALLOWED);
    }

    SolrIndexSearcher.QueryCommand cmd = rb.getQueryCommand();
    cmd.setTimeAllowed(timeAllowed);

    req.getContext().put(SolrIndexSearcher.STATS_SOURCE, statsCache.get(req));
    
    SolrIndexSearcher.QueryResult result = new SolrIndexSearcher.QueryResult();

    //
    // grouping / field collapsing
    //
    if (groupingSpec != null) {
      try {
        boolean needScores = (cmd.getFlags() & SolrIndexSearcher.GET_SCORES) != 0;
        if (params.getBool(GroupParams.GROUP_DISTRIBUTED_FIRST, false)) {
          CommandHandler.Builder topsGroupsActionBuilder = new CommandHandler.Builder()
              .setQueryCommand(cmd)
              .setNeedDocSet(false) // Order matters here
              .setIncludeHitCount(true)
              .setSearcher(searcher);

          // use the first field for first level grouping
          String field = groupingSpec.getFields()[0];
          SearchGroupsFieldCommand groupCommand = new SearchGroupsFieldCommand.Builder()
              .setField(schema.getField(field))
              .setGroupSort(groupingSpec.getGroupSort())
              .setTopNGroups(cmd.getOffset() + cmd.getLen())
              .setIncludeGroupCount(groupingSpec.isIncludeGroupCount())
              .build(); 
          topsGroupsActionBuilder.addCommandField(groupCommand);
          CommandHandler commandHandler = topsGroupsActionBuilder.build();
          commandHandler.execute();
          Long count = (long)commandHandler.getTotalHitCount();
          rsp.add("totalHitCount", count);
       
          SearchGroupsResultTransformer serializer = new SearchGroupsResultTransformer(searcher);
          rsp.add("firstPhase", commandHandler.processResult(result, serializer));
          rb.setResult(result);
          return;
        } 
        else if (params.getBool(GroupParams.GROUP_DISTRIBUTED_SECOND, false)) {
          // now process the middle phase to get the second level group
          CommandHandler.Builder topsGroupsActionBuilder = new CommandHandler.Builder()
              .setQueryCommand(cmd)
              .setNeedDocSet(false) // Order matters here
              .setIncludeHitCount(false)
              .setSearcher(searcher);
          
          for (String field : groupingSpec.getFields()) {
            SchemaField schemaField = schema.getField(field);
            String[] topGroupsParam = params.getParams(GroupParams.GROUP_DISTRIBUTED_TOPGROUPS_PREFIX + field);
            if (topGroupsParam == null) {
              topGroupsParam = new String[0];
            }

            Collection<SearchGroup<BytesRef>> topGroups = new ArrayList<>(topGroupsParam.length);
            for (String topGroup : topGroupsParam) {
              CollectedSearchGroup2<BytesRef> searchGroup = new CollectedSearchGroup2<>();
              if (!topGroup.equals(TopGroupsShardRequestFactory.GROUP_NULL_VALUE)) {
                searchGroup.groupValue = new BytesRef(schemaField.getType().readableToIndexed(topGroup));
                topGroups.add(searchGroup);
              }
            }


//          topsGroupsActionBuilder = new CommandHandler.Builder()
//              .setQueryCommand(cmd)
//              .setNeedDocSet(false) // Order matters here
//              .setIncludeHitCount(false)
//              .setSearcher(searcher);
          
          String field2 = groupingSpec.getParentFields().get(field);
          topsGroupsActionBuilder.addCommandField(new SearchGroups2FieldCommand.Builder()
              .setField(schema.getField(field2))
              .setParentField(schema.getField(field))
              .setGroupSort(groupingSpec.getGroupSort())
              .setTopNGroups(cmd.getOffset() + cmd.getLen())
              .setIncludeGroupCount(groupingSpec.isIncludeGroupCount())
              .setSearchGroups(topGroups)
              .build()
        		);
          }
          CommandHandler commandHandler = topsGroupsActionBuilder.build();
          commandHandler.execute();
          SearchGroups2ResultTransformer serializer = new SearchGroups2ResultTransformer(searcher);
          rsp.add("secondPhase", commandHandler.processResult(result, serializer));
          rb.setResult(result);
          return;
        	
        }
        else if (params.getBool(Group2Params.GROUP_DISTRIBUTED_THIRD, false)) {
          // now process the middle phase to get the second level group
          CommandHandler.Builder topsGroupsActionBuilder = new CommandHandler.Builder()
              .setQueryCommand(cmd)
              .setNeedDocSet(false) // Order matters here
              .setIncludeHitCount(false)
              .setSearcher(searcher);
          SchemaField schemaSubField = schema.getField(groupingSpec.getParentFields().get(groupingSpec.getFields()[0]));
          
          for (String field : groupingSpec.getFields()) {
            SchemaField schemaField = schema.getField(field);
            String[] topGroupsParam = params.getParams(GroupParams.GROUP_DISTRIBUTED_TOPGROUPS_PREFIX + field);
            if (topGroupsParam == null) {
              topGroupsParam = new String[0];
            }

            Collection<CollectedSearchGroup2<BytesRef>> topGroups = new ArrayList<>(topGroupsParam.length);
            for (String topGroup : topGroupsParam) {
              CollectedSearchGroup2<BytesRef> searchGroup = new CollectedSearchGroup2<>();
              if (!topGroup.equals(TopGroupsShardRequestFactory.GROUP_NULL_VALUE)) {
                searchGroup.groupValue = new BytesRef(schemaField.getType().readableToIndexed(topGroup));
                String[] topSubGroupsParam = params.getParams(GroupParams.GROUP_DISTRIBUTED_TOPGROUPS_PREFIX + field
                		+ "."+topGroup);
                if (topSubGroupsParam == null) {
                	topSubGroupsParam = new String[0];
                }
                searchGroup.subGroups = new ArrayList<>(topSubGroupsParam.length);

                for (String subGroup : topSubGroupsParam) {
                  CollectedSearchGroup2<BytesRef> sg = new CollectedSearchGroup2<>();
                  sg.groupValue = new BytesRef(schemaSubField.getType().readableToIndexed(subGroup));
                  searchGroup.subGroups.add(sg);
                }
                topGroups.add(searchGroup);
              }
            }

          String field2 = groupingSpec.getParentFields().get(field);
          topsGroupsActionBuilder.addCommandField(
          		 new TopGroups2FieldCommand.Builder()
               .setField(schema.getField(field2))
               .setParentField(schemaField)
               .setGroupSort(groupingSpec.getGroupSort())
               .setSortWithinGroup(groupingSpec.getSortWithinGroup())
               .setFirstPhaseGroups(topGroups)
               .setMaxDocPerGroup(groupingSpec.getGroupOffset() + groupingSpec.getGroupLimit())
               .setNeedScores(needScores)
               .setNeedMaxScore(needScores)
               .build()
               );
          }
          CommandHandler commandHandler = topsGroupsActionBuilder.build();
          commandHandler.execute();
          TopGroups2ResultTransformer serializer = new TopGroups2ResultTransformer(rb);
          rsp.add("thirdPhase", commandHandler.processResult(result, serializer));
          rb.setResult(result);
          return;
        }
        else if (params.getBool(Group2Params.GROUP_DISTRIBUTED_TOPGROUPS_PREFIX, false)) { 
          CommandHandler.Builder secondPhaseBuilder = new CommandHandler.Builder()
              .setQueryCommand(cmd)
              .setTruncateGroups(groupingSpec.isTruncateGroups() && groupingSpec.getFields().length > 0)
              .setSearcher(searcher);

          for (String field : groupingSpec.getFields()) {
            SchemaField schemaField = schema.getField(field);
            String innerField = groupingSpec.getParentFields().get(field);
            SchemaField innerSchemaField = schema.getField(innerField);
            String[] topGroupsParam = params.getParams(GroupParams.GROUP_DISTRIBUTED_TOPGROUPS_PREFIX + field);
            if (topGroupsParam == null) {
              topGroupsParam = new String[0];
            }

            Collection<SearchGroup<BytesRef>> topGroups = new ArrayList<>(topGroupsParam.length);
            for (String topGroup : topGroupsParam) {
              CollectedSearchGroup2<BytesRef> searchGroup = new CollectedSearchGroup2<>();
              if (!topGroup.equals(TopGroupsShardRequestFactory.GROUP_NULL_VALUE)) {
                searchGroup.groupValue = new BytesRef(schemaField.getType().readableToIndexed(topGroup));
                String[] innerGroupsParam = params.getParams(GroupParams.GROUP_DISTRIBUTED_TOPGROUPS_PREFIX + field+"."+topGroup);
                Collection<SearchGroup<BytesRef>> innerSearchGroup = new ArrayList<>();
                for (String innerGroup : innerGroupsParam) {
                  CollectedSearchGroup2<BytesRef> inner = new CollectedSearchGroup2<>();
                  inner.groupValue = new BytesRef(innerSchemaField.getType().readableToIndexed(innerGroup));
                  innerSearchGroup.add(inner);
                }
                searchGroup.subGroups = innerSearchGroup;
              }
              topGroups.add(searchGroup);
            }

            secondPhaseBuilder.addCommandField(
                new TopGroupsFieldCommand.Builder()
                    .setField(schemaField)
                    .setGroupSort(groupingSpec.getGroupSort())
                    .setSortWithinGroup(groupingSpec.getSortWithinGroup())
                    .setFirstPhaseGroups(topGroups)
                    .setMaxDocPerGroup(groupingSpec.getGroupOffset() + groupingSpec.getGroupLimit())
                    .setNeedScores(needScores)
                    .setNeedMaxScore(needScores)
                    .build()
            );
          }

          for (String query : groupingSpec.getQueries()) {
            secondPhaseBuilder.addCommandField(new Builder()
                .setDocsToCollect(groupingSpec.getOffset() + groupingSpec.getLimit())
                .setSort(groupingSpec.getGroupSort())
                .setQuery(query, rb.req)
                .setDocSet(searcher)
                .build()
            );
          }

          CommandHandler commandHandler = secondPhaseBuilder.build();
          commandHandler.execute();
          TopGroupsResultTransformer serializer = new TopGroupsResultTransformer(rb);
          rsp.add("secondPhase", commandHandler.processResult(result, serializer));
          rb.setResult(result);
          return;
        }
if(1==1){throw new IllegalStateException("not yet supporting non distrib query.");}
        int maxDocsPercentageToCache = params.getInt(GroupParams.GROUP_CACHE_PERCENTAGE, 0);
        boolean cacheSecondPassSearch = maxDocsPercentageToCache >= 1 && maxDocsPercentageToCache <= 100;
        Grouping.TotalCount defaultTotalCount = groupingSpec.isIncludeGroupCount() ?
            Grouping.TotalCount.grouped : Grouping.TotalCount.ungrouped;
        int limitDefault = cmd.getLen(); // this is normally from "rows"
        Grouping grouping =
            new Grouping(searcher, result, cmd, cacheSecondPassSearch, maxDocsPercentageToCache, groupingSpec.isMain());
        grouping.setGroupSort(groupingSpec.getGroupSort())
            .setWithinGroupSort(groupingSpec.getSortWithinGroup())
            .setDefaultFormat(groupingSpec.getResponseFormat())
            .setLimitDefault(limitDefault)
            .setDefaultTotalCount(defaultTotalCount)
            .setDocsPerGroupDefault(groupingSpec.getGroupLimit())
            .setGroupOffsetDefault(groupingSpec.getGroupOffset())
            .setGetGroupedDocSet(groupingSpec.isTruncateGroups());

        if (groupingSpec.getFields() != null) {
          for (String field : groupingSpec.getFields()) {
            grouping.addFieldCommand(field, rb.req);
          }
        }

        if (groupingSpec.getFunctions() != null) {
          for (String groupByStr : groupingSpec.getFunctions()) {
            grouping.addFunctionCommand(groupByStr, rb.req);
          }
        }

        if (groupingSpec.getQueries() != null) {
          for (String groupByStr : groupingSpec.getQueries()) {
            grouping.addQueryCommand(groupByStr, rb.req);
          }
        }

        if (rb.doHighlights || rb.isDebug() || params.getBool(MoreLikeThisParams.MLT, false)) {
          // we need a single list of the returned docs
          cmd.setFlags(SolrIndexSearcher.GET_DOCLIST);
        }

        grouping.execute();
        if (grouping.isSignalCacheWarning()) {
          rsp.add(
              "cacheWarning",
              String.format(Locale.ROOT, "Cache limit of %d percent relative to maxdoc has exceeded. Please increase cache size or disable caching.", maxDocsPercentageToCache)
          );
        }
        rb.setResult(result);

        if (grouping.mainResult != null) {
          ResultContext ctx = new ResultContext();
          ctx.docs = grouping.mainResult;
          ctx.query = null; // TODO? add the query?
          rsp.addResponse(ctx);
          rsp.getToLog().add("hits", grouping.mainResult.matches());
        } else if (!grouping.getCommands().isEmpty()) { // Can never be empty since grouping.execute() checks for this.
          rsp.add("grouped", result.groupedResults);
          rsp.getToLog().add("hits", grouping.getCommands().get(0).getMatches());
        }
        return;
      } catch (SyntaxError e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
    }

	}
	
  protected int groupedDistributedProcess(ResponseBuilder rb) {
    int nextStage = ResponseBuilder.STAGE_DONE;
    ShardRequestFactory shardRequestFactory = null;
    if (rb.stage < ResponseBuilder.STAGE_PARSE_QUERY) {
      nextStage = ResponseBuilder.STAGE_PARSE_QUERY;
    } else if (rb.stage == ResponseBuilder.STAGE_PARSE_QUERY) {
      createDistributedStats(rb);
      nextStage = ResponseBuilder.STAGE_TOP_GROUPS;
    } else if (rb.stage < ResponseBuilder.STAGE_TOP_GROUPS) {
      nextStage = ResponseBuilder.STAGE_TOP_GROUPS;
    } else if (rb.stage == ResponseBuilder.STAGE_TOP_GROUPS) {
      shardRequestFactory = new SearchGroupsRequestFactory();
      nextStage = ResponseBuilder.STAGE_TOP_GROUPS +10;
    } else if (rb.stage < ResponseBuilder.STAGE_TOP_GROUPS +10) {
      nextStage = ResponseBuilder.STAGE_TOP_GROUPS +10;
    } else if (rb.stage == ResponseBuilder.STAGE_TOP_GROUPS +10) {
      shardRequestFactory = new TopGroups2ShardRequestFactory();
      nextStage = ResponseBuilder.STAGE_EXECUTE_QUERY;
    } else if (rb.stage < ResponseBuilder.STAGE_EXECUTE_QUERY) {
      nextStage = ResponseBuilder.STAGE_EXECUTE_QUERY;
    } else if (rb.stage == ResponseBuilder.STAGE_EXECUTE_QUERY) {
      shardRequestFactory = new TopGroups2ShardRequestFactory();
      nextStage = ResponseBuilder.STAGE_GET_FIELDS;
    } else if (rb.stage < ResponseBuilder.STAGE_GET_FIELDS) {
      nextStage = ResponseBuilder.STAGE_GET_FIELDS;
    } else if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      shardRequestFactory = new StoredFieldsShardRequestFactory();
      nextStage = ResponseBuilder.STAGE_DONE;
    }

    if (shardRequestFactory != null) {
      for (ShardRequest shardRequest : shardRequestFactory.constructRequest(rb)) {
        rb.addRequest(this, shardRequest);
      }
    }
    return nextStage;
  }

  protected void handleGroupedResponses(ResponseBuilder rb, ShardRequest sreq) {
    ShardResponseProcessor responseProcessor = null;
    System.out.println("XXXXXXXXXXXXXXXXXXXXXX  " + sreq.purpose);
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_GROUPS) != 0) {
      responseProcessor = new SearchGroup2ShardResponseProcessor();
    } else if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
    	if(sreq.responses.get(0).getSolrResponse().getResponse().get("thirdPhase")!=null){
    		responseProcessor = new TopGroups2ShardResponseProcessor();
    	}
    	else{
    		responseProcessor = new SearchGroup2SecondPhaseShardResponseProcessor();
    	}
    } else if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
      responseProcessor = new StoredFieldsShardResponseProcessor();
    }

    if (responseProcessor != null) {
      responseProcessor.process(rb, sreq);
    }
  }

  @SuppressWarnings("unchecked")
  protected void groupedFinishStage(final ResponseBuilder rb) {
    // To have same response as non-distributed request.
    GroupingSpecification groupSpec = rb.getGroupingSpec();
    if (rb.mergedTopGroups.isEmpty()) {
      for (String field : groupSpec.getFields()) {
        rb.mergedTopGroups.put(field, new TopGroups(null, null, 0, 0, new GroupDocs[]{}, Float.NaN));
      }
      rb.resultIds = new HashMap<>();
    }

    EndResultTransformer.SolrDocumentSource solrDocumentSource = new EndResultTransformer.SolrDocumentSource() {

      @Override
      public SolrDocument retrieve(ScoreDoc doc) {
        ShardDoc solrDoc = (ShardDoc) doc;
        return rb.retrievedDocuments.get(solrDoc.id);
      }

    };
    EndResultTransformer endResultTransformer;
    if (groupSpec.isMain()) {
      endResultTransformer = MAIN_END_RESULT_TRANSFORMER;
    } else if (Grouping.Format.grouped == groupSpec.getResponseFormat()) {
      endResultTransformer = new Grouped2EndResultTransformer(rb.req.getSearcher());
    } else if (Grouping.Format.simple == groupSpec.getResponseFormat() && !groupSpec.isMain()) {
      endResultTransformer = SIMPLE_END_RESULT_TRANSFORMER;
    } else {
      return;
    }
    Map<String, Object> combinedMap = new LinkedHashMap<>();
    combinedMap.putAll(rb.mergedTopGroups);
    combinedMap.putAll(rb.mergedQueryCommandResults);
    endResultTransformer.transform(combinedMap, rb, solrDocumentSource);
  }

}
