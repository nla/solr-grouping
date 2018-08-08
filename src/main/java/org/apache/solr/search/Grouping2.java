package org.apache.solr.search;

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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.lucene.index.ExitableDirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.CachingCollector;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.grouping.AbstractAllGroupHeadsCollector;
import org.apache.lucene.search.grouping.CollectedSearchGroup2;
import org.apache.lucene.search.grouping.Group2Docs;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups2;
import org.apache.lucene.search.grouping.function.FunctionAllGroupHeadsCollector;
import org.apache.lucene.search.grouping.function.FunctionAllGroupsCollector;
import org.apache.lucene.search.grouping.function.FunctionFirstPassGrouping2Collector;
import org.apache.lucene.search.grouping.function.FunctionNullSecondPassGrouping2Collector;
import org.apache.lucene.search.grouping.function.FunctionNullThirdPassGrouping2Collector;
import org.apache.lucene.search.grouping.function.FunctionSecondPassGrouping2Collector;
import org.apache.lucene.search.grouping.function.FunctionTermSecondPassGrouping2Collector;
import org.apache.lucene.search.grouping.function.FunctionTermThirdPassGrouping2Collector;
import org.apache.lucene.search.grouping.function.FunctionThirdPassGrouping2Collector;
import org.apache.lucene.search.grouping.term.TermAllGroupHeadsCollector;
import org.apache.lucene.search.grouping.term.TermAllGroupsCollector;
import org.apache.lucene.search.grouping.term.TermFirstPassGrouping2Collector;
import org.apache.lucene.search.grouping.term.TermFunctionSecondPassGrouping2Collector;
import org.apache.lucene.search.grouping.term.TermFunctionThirdPassGrouping2Collector;
import org.apache.lucene.search.grouping.term.TermNullSecondPassGrouping2Collector;
import org.apache.lucene.search.grouping.term.TermNullThirdPassGrouping2Collector;
import org.apache.lucene.search.grouping.term.TermSecondPassGrouping2Collector;
import org.apache.lucene.search.grouping.term.TermThirdPassGrouping2Collector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrFieldSource;
import org.apache.solr.schema.TrieIntField;
import org.apache.solr.search.grouping.collector.FilterCollector;
import org.apache.solr.search.grouping.distributed.command.Group2Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic Solr Grouping infrastructure.
 * Warning NOT thread safe!
 *
 * @lucene.experimental
 */
public class Grouping2 {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrIndexSearcher searcher;
  private final SolrIndexSearcher.QueryResult qr;
  private final SolrIndexSearcher.QueryCommand cmd;
  private final List<Command> commands = new ArrayList<>();
  private final boolean main;
  private final boolean cacheSecondPassSearch;
  private final int maxDocsPercentageToCache;

  private Sort groupSort;
  private Sort withinGroupSort;
  private int limitDefault;
  private int docsPerGroupDefault;
  private int groupOffsetDefault;
  private Format defaultFormat;
  private TotalCount defaultTotalCount;

  private int maxDoc;
  private boolean needScores;
  private boolean getDocSet;
  private boolean getGroupedDocSet;
  private boolean getDocList; // doclist needed for debugging or highlighting
  private Query query;
  private DocSet filter;
  private Filter luceneFilter;
  private NamedList grouped = new SimpleOrderedMap();
  private Set<Integer> idSet = new LinkedHashSet<>();  // used for tracking unique docs when we need a doclist
  private int maxMatches;  // max number of matches from any grouping command
  private float maxScore = Float.NaN;  // max score seen in any doclist
  private boolean signalCacheWarning = false;
  private TimeLimitingCollector timeLimitingCollector;


  public DocList mainResult;  // output if one of the grouping commands should be used as the main result.

  /**
   * @param cacheSecondPassSearch    Whether to cache the documents and scores from the first pass search for the second
   *                                 pass search.
   * @param maxDocsPercentageToCache The maximum number of documents in a percentage relative from maxdoc
   *                                 that is allowed in the cache. When this threshold is met,
   *                                 the cache is not used in the second pass search.
   */
  public Grouping2(SolrIndexSearcher searcher,
                  SolrIndexSearcher.QueryResult qr,
                  SolrIndexSearcher.QueryCommand cmd,
                  boolean cacheSecondPassSearch,
                  int maxDocsPercentageToCache,
                  boolean main) {
    this.searcher = searcher;
    this.qr = qr;
    this.cmd = cmd;
    this.cacheSecondPassSearch = cacheSecondPassSearch;
    this.maxDocsPercentageToCache = maxDocsPercentageToCache;
    this.main = main;
  }

  public void add(Grouping2.Command groupingCommand){
    commands.add(groupingCommand);
  }

  /**
   * Adds a field command based on the specified field.
   * If the field is not compatible with {@link CommandField} it invokes the
   * {@link #addFunctionCommand(String, org.apache.solr.request.SolrQueryRequest)} method.
   *
   * @param field The fieldname to group by.
   */
  public void addFieldCommand(String field, String subField, SolrQueryRequest request) throws SyntaxError {
    SchemaField schemaField = searcher.getSchema().getField(field); // Throws an exception when field doesn't exist. Bad request.
    FieldType fieldType = schemaField.getType();
    ValueSource valueSource = fieldType.getValueSource(schemaField, null);
    Grouping2.Command<?, ?> gc;
    SchemaField schemaSubField = null;
    if(subField == null){
    	if (!(valueSource instanceof StrFieldSource)){
    		gc = new CommandFuncNull();
    	}
    	else{
    		gc = new CommandFieldNull();
    	}
    }
    else{
	    schemaSubField = searcher.getSchema().getField(subField); // Throws an exception when field doesn't exist. Bad request.
	    fieldType = schemaSubField.getType();
	    ValueSource valueSubSource = fieldType.getValueSource(schemaSubField, null);
	    if ((!(valueSource instanceof StrFieldSource)) && (!(valueSubSource instanceof StrFieldSource))) {
	    	gc = new CommandFunc();
	    }
	    else if (((valueSource instanceof StrFieldSource)) && (!(valueSubSource instanceof StrFieldSource))) {
	    	gc = new CommandFieldFunc();
	    }
	    else if ((!(valueSource instanceof StrFieldSource)) && ((valueSubSource instanceof StrFieldSource))) {
	    	gc = new CommandFuncField();
	    }
	    else{
	    	gc = new CommandField();
	    }
    }
    gc.withinGroupSort = withinGroupSort;
    gc.groupBy = schemaSubField;
    gc.parentGroupBy = schemaField;
    gc.key = field;
    gc.numGroups = limitDefault;
    gc.docsPerGroup = docsPerGroupDefault;
    gc.groupOffset = groupOffsetDefault;
    gc.offset = cmd.getOffset();
    gc.groupSort = groupSort;
    gc.format = defaultFormat;
    gc.totalCount = defaultTotalCount;

    if (main) {
      gc.main = true;
      gc.format = Grouping2.Format.simple;
    }

    if (gc.format == Grouping2.Format.simple) {
      gc.groupOffset = 0;  // doesn't make sense
    }
    commands.add(gc);
  }


  public void addQueryCommand(String groupByStr, SolrQueryRequest request) throws SyntaxError {
    QParser parser = QParser.getParser(groupByStr, null, request);
    Query gq = parser.getQuery();
    Grouping2.CommandQuery gc = new CommandQuery();
    gc.query = gq;
    gc.withinGroupSort = withinGroupSort;
    gc.key = groupByStr;
    gc.numGroups = limitDefault;
    gc.docsPerGroup = docsPerGroupDefault;
    gc.groupOffset = groupOffsetDefault;

    // these two params will only be used if this is for the main result set
    gc.offset = cmd.getOffset();
    gc.numGroups = limitDefault;
    gc.format = defaultFormat;

    if (main) {
      gc.main = true;
      gc.format = Grouping2.Format.simple;
    }
    if (gc.format == Grouping2.Format.simple) {
      gc.docsPerGroup = gc.numGroups;  // doesn't make sense to limit to one
      gc.groupOffset = gc.offset;
    }

    commands.add(gc);
  }

  public Grouping2 setGroupSort(Sort groupSort) {
    this.groupSort = groupSort;
    return this;
  }

  public Grouping2 setWithinGroupSort(Sort withinGroupSort) {
    this.withinGroupSort = withinGroupSort;
    return this;
  }

  public Grouping2 setLimitDefault(int limitDefault) {
    this.limitDefault = limitDefault;
    return this;
  }

  public Grouping2 setDocsPerGroupDefault(int docsPerGroupDefault) {
    this.docsPerGroupDefault = docsPerGroupDefault;
    return this;
  }

  public Grouping2 setGroupOffsetDefault(int groupOffsetDefault) {
    this.groupOffsetDefault = groupOffsetDefault;
    return this;
  }

  public Grouping2 setDefaultFormat(Format defaultFormat) {
    this.defaultFormat = defaultFormat;
    return this;
  }

  public Grouping2 setDefaultTotalCount(TotalCount defaultTotalCount) {
    this.defaultTotalCount = defaultTotalCount;
    return this;
  }

  public Grouping2 setGetGroupedDocSet(boolean getGroupedDocSet) {
    this.getGroupedDocSet = getGroupedDocSet;
    return this;
  }

  public List<Command> getCommands() {
    return commands;
  }

  public void execute() throws IOException {
    if (commands.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Specify at least one field, function or query to group by.");
    }

    DocListAndSet out = new DocListAndSet();
    qr.setDocListAndSet(out);

    SolrIndexSearcher.ProcessedFilter pf = searcher.getProcessedFilter(cmd.getFilter(), cmd.getFilterList());
    final Filter luceneFilter = pf.filter;
    maxDoc = searcher.maxDoc();

    needScores = (cmd.getFlags() & SolrIndexSearcher.GET_SCORES) != 0;
    boolean cacheScores = false;
    // NOTE: Change this when withinGroupSort can be specified per group
    if (!needScores && !commands.isEmpty()) {
      Sort withinGroupSort = commands.get(0).withinGroupSort;
      cacheScores = withinGroupSort == null || withinGroupSort.needsScores();
    } else if (needScores) {
      cacheScores = needScores;
    }
    getDocSet = (cmd.getFlags() & SolrIndexSearcher.GET_DOCSET) != 0;
    getDocList = (cmd.getFlags() & SolrIndexSearcher.GET_DOCLIST) != 0;
    query = QueryUtils.makeQueryable(cmd.getQuery());

    for (Command cmd : commands) {
      cmd.prepare();
    }

    AbstractAllGroupHeadsCollector<?> allGroupHeadsCollector = null;
    List<Collector> collectors = new ArrayList<>(commands.size());
    TotalHitCountCollector totalHitCollector = new TotalHitCountCollector(); 
    for (Command cmd : commands) {// in grouping 2 there should only be one command
      Collector collector = cmd.createFirstPassCollector();
      if (collector != null) {
        collectors.add(collector);
      }
      collectors.add(totalHitCollector);
      if (getGroupedDocSet && allGroupHeadsCollector == null) {
        collectors.add(allGroupHeadsCollector = cmd.createAllGroupCollector());
      }
    }

    DocSetCollector setCollector = null;
    if (getDocSet && allGroupHeadsCollector == null) {
      setCollector = new DocSetCollector(maxDoc);
      collectors.add(setCollector);
    }
    Collector allCollectors = MultiCollector.wrap(collectors);

    CachingCollector cachedCollector = null;
    if (cacheSecondPassSearch && allCollectors != null) {
      int maxDocsToCache = (int) Math.round(maxDoc * (maxDocsPercentageToCache / 100.0d));
      // Only makes sense to cache if we cache more than zero.
      // Maybe we should have a minimum and a maximum, that defines the window we would like caching for.
      if (maxDocsToCache > 0) {
        allCollectors = cachedCollector = CachingCollector.create(allCollectors, cacheScores, maxDocsToCache);
      }
    }

    if (pf.postFilter != null) {
      pf.postFilter.setLastDelegate(allCollectors);
      allCollectors = pf.postFilter;
    }

    if (allCollectors != null) {
      searchWithTimeLimiter(luceneFilter, allCollectors);

      if(allCollectors instanceof DelegatingCollector) {
        ((DelegatingCollector) allCollectors).finish();
      }
    }

    if (getGroupedDocSet && allGroupHeadsCollector != null) {
      qr.setDocSet(new BitDocSet(allGroupHeadsCollector.retrieveGroupHeads(maxDoc)));
    } else if (getDocSet) {
      qr.setDocSet(setCollector.getDocSet());
    }

    for(Command cmd : commands){// in grouping 2 there should only be one command
    	cmd.totalHitCount = totalHitCollector.getTotalHits();
    }
    collectors.clear();
    for (Command cmd : commands) {
      Collector collector = cmd.createSecondPassCollector();
      if (collector != null)
        collectors.add(collector);
    }

    if (!collectors.isEmpty()) {
      Collector secondPhaseCollectors = MultiCollector.wrap(collectors.toArray(new Collector[collectors.size()]));
      if (collectors.size() > 0) {
        if (cachedCollector != null) {
          if (cachedCollector.isCached()) {
            cachedCollector.replay(secondPhaseCollectors);
          } else {
            signalCacheWarning = true;
            logger.warn(String.format(Locale.ROOT, "The grouping cache is active, but not used because it exceeded the max cache limit of %d percent", maxDocsPercentageToCache));
            logger.warn("Please increase cache size or disable group caching.");
            searchWithTimeLimiter(luceneFilter, secondPhaseCollectors);
          }
        } else {
          if (pf.postFilter != null) {
            pf.postFilter.setLastDelegate(secondPhaseCollectors);
            secondPhaseCollectors = pf.postFilter;
          }
          searchWithTimeLimiter(luceneFilter, secondPhaseCollectors);
        }
        if (secondPhaseCollectors instanceof DelegatingCollector) {
          ((DelegatingCollector) secondPhaseCollectors).finish();
        }
      }
    }

 // third pass
    collectors.clear();
    for (Command cmd : commands) {
      Collector collector = cmd.createThirdPassCollector();
      if (collector != null)
        collectors.add(collector);
    }

    if (!collectors.isEmpty()) {
      Collector thirdPhaseCollectors = MultiCollector.wrap(collectors.toArray(new Collector[collectors.size()]));
      if (collectors.size() > 0) {
        if (cachedCollector != null) {
          if (cachedCollector.isCached()) {
            cachedCollector.replay(thirdPhaseCollectors);
          } else {
            signalCacheWarning = true;
            logger.warn(String.format(Locale.ROOT, "The grouping cache is active, but not used because it exceeded the max cache limit of %d percent", maxDocsPercentageToCache));
            logger.warn("Please increase cache size or disable group caching.");
            searchWithTimeLimiter(luceneFilter, thirdPhaseCollectors);
          }
        } else {
          if (pf.postFilter != null) {
            pf.postFilter.setLastDelegate(thirdPhaseCollectors);
            thirdPhaseCollectors = pf.postFilter;
          }
          searchWithTimeLimiter(luceneFilter, thirdPhaseCollectors);
        }
        if (thirdPhaseCollectors instanceof DelegatingCollector) {
          ((DelegatingCollector) thirdPhaseCollectors).finish();
        }
      }
    }

    for (Command cmd : commands) {
      cmd.finish();
    }

    qr.groupedResults = grouped;

    if (getDocList) {
      int sz = idSet.size();
      int[] ids = new int[sz];
      int idx = 0;
      for (int val : idSet) {
        ids[idx++] = val;
      }
      qr.setDocList(new DocSlice(0, sz, ids, null, maxMatches, maxScore));
    }
  }

  /**
   * Invokes search with the specified filter and collector.  
   * If a time limit has been specified, wrap the collector in a TimeLimitingCollector
   */
  private void searchWithTimeLimiter(final Filter luceneFilter, Collector collector) throws IOException {
    if (cmd.getTimeAllowed() > 0) {
      if (timeLimitingCollector == null) {
        timeLimitingCollector = new TimeLimitingCollector(collector, TimeLimitingCollector.getGlobalCounter(), cmd.getTimeAllowed());
      } else {
        /*
         * This is so the same timer can be used for grouping's multiple phases.   
         * We don't want to create a new TimeLimitingCollector for each phase because that would 
         * reset the timer for each phase.  If time runs out during the first phase, the 
         * second phase should timeout quickly.
         */
        timeLimitingCollector.setCollector(collector);
      }
      collector = timeLimitingCollector;
    }
    try {
      Query q = query;
      if (luceneFilter != null) {
        q = new FilteredQuery(q, luceneFilter);
      }
      searcher.search(q, collector);
    } catch (TimeLimitingCollector.TimeExceededException | ExitableDirectoryReader.ExitingReaderException x) {
      logger.warn( "Query: " + query + "; " + x.getMessage() );
      qr.setPartialResults(true);
    }
  }

  /**
   * Returns offset + len if len equals zero or higher. Otherwise returns max.
   *
   * @param offset The offset
   * @param len    The number of documents to return
   * @param max    The number of document to return if len < 0 or if offset + len < 0
   * @return offset + len if len equals zero or higher. Otherwise returns max
   */
  int getMax(int offset, int len, int max) {
    int v = len < 0 ? max : offset + len;
    if (v < 0 || v > max) v = max;
    return v;
  }

  /**
   * Returns whether a cache warning should be send to the client.
   * The value <code>true</code> is returned when the cache is emptied because the caching limits where met, otherwise
   * <code>false</code> is returned.
   *
   * @return whether a cache warning should be send to the client
   */
  public boolean isSignalCacheWarning() {
    return signalCacheWarning;
  }

  //======================================   Inner classes =============================================================

  public static enum Format {

    /**
     * Grouped result. Each group has its own result set.
     */
    grouped,

    /**
     * Flat result. All documents of all groups are put in one list.
     */
    simple
  }

  public static enum TotalCount {
    /**
     * Computations should be based on groups.
     */
    grouped,

    /**
     * Computations should be based on plain documents, so not taking grouping into account.
     */
    ungrouped
  }

  /**
   * General group command. A group command is responsible for creating the first and second pass collectors.
   * A group command is also responsible for creating the response structure.
   * <p>
   * Note: Maybe the creating the response structure should be done in something like a ReponseBuilder???
   * Warning NOT thread save!
   */
  public abstract class Command<GROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE> {

    public String key;       // the name to use for this group in the response
    public Sort withinGroupSort;   // the sort of the documents *within* a single group.
    public Sort groupSort;        // the sort between groups
    public int docsPerGroup; // how many docs in each group - from "group.limit" param, default=1
    public int groupOffset;  // the offset within each group (for paging within each group)
    public int numGroups;    // how many groups - defaults to the "rows" parameter
    int actualGroupsToFind;  // How many groups should actually be found. Based on groupOffset and numGroups.
    public int offset;       // offset into the list of groups
    public Format format;
    public boolean main;     // use as the main result in simple format (grouped.main=true param)
    public TotalCount totalCount = TotalCount.ungrouped;
    public SchemaField groupBy;
    public SchemaField parentGroupBy;
    public int totalHitCount = 0;

    TopGroups2<GROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE> result;


    /**
     * Prepare this <code>Command</code> for execution.
     *
     * @throws IOException If I/O related errors occur
     */
    protected abstract void prepare() throws IOException;

    /**
     * Returns one or more {@link Collector} instances that are needed to perform the first pass search.
     * If multiple Collectors are returned then these wrapped in a {@link org.apache.lucene.search.MultiCollector}.
     *
     * @return one or more {@link Collector} instances that are need to perform the first pass search
     * @throws IOException If I/O related errors occur
     */
    protected abstract Collector createFirstPassCollector() throws IOException;

    /**
     * Returns zero or more {@link Collector} instances that are needed to perform the second pass search.
     * In the case when no {@link Collector} instances are created <code>null</code> is returned.
     * If multiple Collectors are returned then these wrapped in a {@link org.apache.lucene.search.MultiCollector}.
     *
     * @return zero or more {@link Collector} instances that are needed to perform the second pass search
     * @throws IOException If I/O related errors occur
     */
//    protected abstract Collector createSecondPassCollector() throws IOException;
    protected Collector createSecondPassCollector() throws IOException{return null;}
    
//    protected abstract Collector createThirdPassCollector() throws IOException;
    protected Collector createThirdPassCollector() throws IOException{return null;}

    /**
     * Returns a collector that is able to return the most relevant document of all groups.
     * Returns <code>null</code> if the command doesn't support this type of collector.
     *
     * @return a collector that is able to return the most relevant document of all groups.
     * @throws IOException If I/O related errors occur
     */
    public AbstractAllGroupHeadsCollector<?> createAllGroupCollector() throws IOException {
      return null;
    }

    /**
     * Performs any necessary post actions to prepare the response.
     *
     * @throws IOException If I/O related errors occur
     */
    protected abstract void finish() throws IOException;
    protected void finish(TopGroups2<BytesRef, BytesRef> result) throws IOException {
      if (main) {
        mainResult = createSimpleResponse();
        return;
      }

      NamedList groupResult = commonResponse();

      if (format == Format.simple) {
        groupResult.add("doclist", createSimpleResponse());
        return;
      }

      List<NamedList> parentGroupList = new ArrayList<>();
      groupResult.add("groups", parentGroupList);        // grouped={ parentkey={ groups=[
      List groupList = new ArrayList();

      if (result == null) {
        NamedList<Object> grouped = new SimpleOrderedMap<>();
        NamedList<Object> rec = new SimpleOrderedMap<>();
//        grouped.add(field, rec);
        return;
      }

      // handle case of rows=0
      if (numGroups == 0) return;

      SchemaField schemaField = groupBy;
      FieldType fieldType = null;
      if(schemaField != null){
      	fieldType = schemaField.getType();
      }
      SchemaField schemaParentField = parentGroupBy;
      FieldType parentFieldType = schemaParentField.getType();
      String parentGroupValue = "";
      for (Group2Docs<BytesRef, BytesRef> group : result.groups) {
      	String readableValue = parentFieldType.indexedToReadable(group.groupParentValue.utf8ToString());
      	IndexableField field = schemaParentField.createField(readableValue, 1.0f);
      	if(!parentGroupValue.equals(readableValue)){
//      		System.out.println(readableValue);
      		NamedList<Object> parentRec = new SimpleOrderedMap<>();
      		parentGroupList.add(parentRec);
      		parentRec.add("groupValue", parentFieldType.toObject(field));
      		parentGroupValue = readableValue;

        	if(schemaField == null){
        		addDocList(parentRec, group);
        		continue;
        	}
      		NamedList<Object> subGrouped = new SimpleOrderedMap<>();
      		NamedList<Object> subKey = new SimpleOrderedMap<>();
      		NamedList<Object> subRec = new SimpleOrderedMap<>();
          groupList = new ArrayList();
          parentRec.add("grouped", subGrouped);
          subGrouped.add(groupBy.getName(), subKey);
          // have to get the count from the second pass collector
          long count = getCount(group.groupParentValue);
          subKey.add("matches", count);
//          subKey.add("matches2", group.);
          subKey.add("groups", groupList);        // grouped={ key={ groups=[
      	}
        NamedList nl = new SimpleOrderedMap();
        groupList.add(nl);                         // grouped={ key={ groups=[ {


        // To keep the response format compatable with trunk.
        // In trunk MutableValue can convert an indexed value to its native type. E.g. string to int
        // The only option I currently see is the use the FieldType for this
        if (group.groupValue != null) {
          readableValue = fieldType.indexedToReadable(group.groupValue.utf8ToString());
          field = schemaField.createField(readableValue, 1.0f);
          nl.add("groupValue", fieldType.toObject(field));
        } else {
          nl.add("groupValue", null);
        }

        addDocList(nl, group);
      }
    }

    protected abstract long getCount(BytesRef group);
    /**
     * Returns the number of matches for this <code>Command</code>.
     *
     * @return the number of matches for this <code>Command</code>
     */
    public abstract int getMatches();

    /**
     * Returns the number of groups found for this <code>Command</code>.
     * If the command doesn't support counting the groups <code>null</code> is returned.
     *
     * @return the number of groups found for this <code>Command</code>
     */
    protected Integer getNumberOfGroups() {
      return null;
    }

    protected NamedList commonResponse() {
      NamedList groupResult = new SimpleOrderedMap();
      grouped.add(key, groupResult);  // grouped={ key={

      long matches = totalHitCount;
      groupResult.add("matches", matches);
      if (totalCount == TotalCount.grouped) {
        Integer totalNrOfGroups = getNumberOfGroups();
        groupResult.add("ngroups", totalNrOfGroups == null ? 0 : totalNrOfGroups);
      }
      maxMatches = Math.max(maxMatches, totalHitCount);
      return groupResult;
    }

    protected DocList getDocList(GroupDocs groups) {
      int max = groups.totalHits;
      int off = groupOffset;
      int len = docsPerGroup;
      if (format == Format.simple) {
        off = offset;
        len = numGroups;
      }
      int docsToCollect = getMax(off, len, max);

      // TODO: implement a DocList impl that doesn't need to start at offset=0
      int docsCollected = Math.min(docsToCollect, groups.scoreDocs.length);

      int ids[] = new int[docsCollected];
      float[] scores = needScores ? new float[docsCollected] : null;
      for (int i = 0; i < ids.length; i++) {
        ids[i] = groups.scoreDocs[i].doc;
        if (scores != null)
          scores[i] = groups.scoreDocs[i].score;
      }

      float score = groups.maxScore;
      maxScore = maxAvoidNaN(score, maxScore);
      DocSlice docs = new DocSlice(off, Math.max(0, ids.length - off), ids, scores, groups.totalHits, score);

      if (getDocList) {
        DocIterator iter = docs.iterator();
        while (iter.hasNext())
          idSet.add(iter.nextDoc());
      }
      return docs;
    }

    protected void addDocList(NamedList rsp, GroupDocs groups) {
      rsp.add("doclist", getDocList(groups));
    }

    // Flatten the groups and get up offset + rows documents
    protected DocList createSimpleResponse() {
      GroupDocs[] groups = result != null ? result.groups : new GroupDocs[0];

      List<Integer> ids = new ArrayList<>();
      List<Float> scores = new ArrayList<>();
      int docsToGather = getMax(offset, numGroups, maxDoc);
      int docsGathered = 0;
      float maxScore = Float.NaN;

      outer:
      for (GroupDocs group : groups) {
        maxScore = maxAvoidNaN(maxScore, group.maxScore);

        for (ScoreDoc scoreDoc : group.scoreDocs) {
          if (docsGathered >= docsToGather) {
            break outer;
          }

          ids.add(scoreDoc.doc);
          scores.add(scoreDoc.score);
          docsGathered++;
        }
      }

      int len = docsGathered > offset ? docsGathered - offset : 0;
      int[] docs = ArrayUtils.toPrimitive(ids.toArray(new Integer[ids.size()]));
      float[] docScores = ArrayUtils.toPrimitive(scores.toArray(new Float[scores.size()]));
      DocSlice docSlice = new DocSlice(offset, len, docs, docScores, getMatches(), maxScore);

      if (getDocList) {
        for (int i = offset; i < docs.length; i++) {
          idSet.add(docs[i]);
        }
      }

      return docSlice;
    }

  }

  /** Differs from {@link Math#max(float, float)} in that if only one side is NaN, we return the other. */
  private float maxAvoidNaN(float valA, float valB) {
    if (Float.isNaN(valA) || valB > valA) {
      return valB;
    } else {
      return valA;
    }
  }

  /**
   * A group command for grouping on a field.
   */
  public class CommandField extends Command<BytesRef, BytesRef> {

    TermFirstPassGrouping2Collector firstPass;
    TermSecondPassGrouping2Collector secondPass;
    TermThirdPassGrouping2Collector thirdPass;

    TermAllGroupsCollector allGroupsCollector;

    // If offset falls outside the number of documents a group can provide use this collector instead of secondPass
    TotalHitCountCollector fallBackCollector;
    Collection<SearchGroup<BytesRef>> topGroups;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void prepare() throws IOException {
      actualGroupsToFind = getMax(offset, numGroups, maxDoc);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createFirstPassCollector() throws IOException {
      // Ok we don't want groups, but do want a total count
      if (actualGroupsToFind <= 0) {
        fallBackCollector = new TotalHitCountCollector();
        return fallBackCollector;
      }

      groupSort = groupSort == null ? Sort.RELEVANCE : groupSort;
      firstPass = new TermFirstPassGrouping2Collector(parentGroupBy.getName(), groupSort, actualGroupsToFind);
      return firstPass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createSecondPassCollector() throws IOException {
      if (actualGroupsToFind <= 0) {
        allGroupsCollector = new TermAllGroupsCollector(groupBy.getName());
        return totalCount == TotalCount.grouped ? allGroupsCollector : null;
      }
      topGroups = format == Format.grouped ? firstPass.getTopGroups(offset, false) : firstPass.getTopGroups(0, false);
      if (topGroups == null) {
        if (totalCount == TotalCount.grouped) {
          allGroupsCollector = new TermAllGroupsCollector(groupBy.getName());
          fallBackCollector = new TotalHitCountCollector();
          return MultiCollector.wrap(allGroupsCollector, fallBackCollector);
        } else {
          fallBackCollector = new TotalHitCountCollector();
          return fallBackCollector;
        }
      }

      int groupedDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      groupedDocsToCollect = Math.max(groupedDocsToCollect, 1);
      secondPass = new TermSecondPassGrouping2Collector(
      		groupBy, parentGroupBy, null, topGroups, groupSort, groupedDocsToCollect
      );

      if (totalCount == TotalCount.grouped) {
        allGroupsCollector = new TermAllGroupsCollector(groupBy.getName());
        return MultiCollector.wrap(secondPass, allGroupsCollector);
      } else {
        return secondPass;
      }
    }
    
    @Override
    protected Collector createThirdPassCollector() throws IOException{
    	if(secondPass == null){
    		return null;
    	}
      Sort withinGroupSort = this.withinGroupSort != null ? this.withinGroupSort : Sort.RELEVANCE;
      int groupedDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      groupedDocsToCollect = Math.max(groupedDocsToCollect, 1);
    	Collection<CollectedSearchGroup2<BytesRef, BytesRef>> topGroups = secondPass.getTopGroupsNested(groupOffset, false);
    	thirdPass = new TermThirdPassGrouping2Collector(groupBy, parentGroupBy, topGroups, groupSort, withinGroupSort, groupedDocsToCollect, needScores, needScores, false);
    	return thirdPass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AbstractAllGroupHeadsCollector<?> createAllGroupCollector() throws IOException {
      Sort sortWithinGroup = withinGroupSort != null ? withinGroupSort : Sort.RELEVANCE;
      return TermAllGroupHeadsCollector.create(groupBy.getName(), sortWithinGroup);
    }

    @Override
    protected long getCount(BytesRef group){
    	return secondPass.getCollectors(group).getTotalHitCount();
    }
    /**
     * {@inheritDoc}
     */
    @Override
    protected void finish() throws IOException{
    	result = thirdPass.getTopGroups(0);
    	finish(result);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public int getMatches() {
      if (result == null && fallBackCollector == null) {
        return 0;
      }

      return result != null ? (int)result.totalHitCount : fallBackCollector.getTotalHits();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Integer getNumberOfGroups() {
      return allGroupsCollector == null ? null : allGroupsCollector.getGroupCount();
    }
  }

  public class CommandFieldFunc extends Command<BytesRef, MutableValue> {

    TermFirstPassGrouping2Collector firstPass;
    TermFunctionSecondPassGrouping2Collector secondPass;
    TermFunctionThirdPassGrouping2Collector thirdPass;

    TermAllGroupsCollector allGroupsCollector;

    // If offset falls outside the number of documents a group can provide use this collector instead of secondPass
    TotalHitCountCollector fallBackCollector;
    Collection<SearchGroup<BytesRef>> topGroups;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void prepare() throws IOException {
      actualGroupsToFind = getMax(offset, numGroups, maxDoc);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createFirstPassCollector() throws IOException {
      // Ok we don't want groups, but do want a total count
      if (actualGroupsToFind <= 0) {
        fallBackCollector = new TotalHitCountCollector();
        return fallBackCollector;
      }

      groupSort = groupSort == null ? Sort.RELEVANCE : groupSort;
      firstPass = new TermFirstPassGrouping2Collector(parentGroupBy.getName(), groupSort, actualGroupsToFind);
      return firstPass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createSecondPassCollector() throws IOException {
      if (actualGroupsToFind <= 0) {
        allGroupsCollector = new TermAllGroupsCollector(groupBy.getName());
        return totalCount == TotalCount.grouped ? allGroupsCollector : null;
      }
      topGroups = format == Format.grouped ? firstPass.getTopGroups(offset, false) : firstPass.getTopGroups(0, false);
      if (topGroups == null) {
        if (totalCount == TotalCount.grouped) {
          allGroupsCollector = new TermAllGroupsCollector(groupBy.getName());
          fallBackCollector = new TotalHitCountCollector();
          return MultiCollector.wrap(allGroupsCollector, fallBackCollector);
        } else {
          fallBackCollector = new TotalHitCountCollector();
          return fallBackCollector;
        }
      }

      int groupedDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      groupedDocsToCollect = Math.max(groupedDocsToCollect, 1);
      secondPass = new TermFunctionSecondPassGrouping2Collector(
      		groupBy, parentGroupBy, null, topGroups, groupSort, groupedDocsToCollect
      );

      if (totalCount == TotalCount.grouped) {
        allGroupsCollector = new TermAllGroupsCollector(groupBy.getName());
        return MultiCollector.wrap(secondPass, allGroupsCollector);
      } else {
        return secondPass;
      }
    }
    
    @Override
    protected Collector createThirdPassCollector() throws IOException{
    	if(secondPass == null){
    		return null;
    	}
      Sort withinGroupSort = this.withinGroupSort != null ? this.withinGroupSort : Sort.RELEVANCE;
      int groupedDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      groupedDocsToCollect = Math.max(groupedDocsToCollect, 1);
    	Collection<CollectedSearchGroup2<BytesRef, MutableValue>> topGroups = secondPass.getTopGroupsNested(groupOffset, false);
    	thirdPass = new TermFunctionThirdPassGrouping2Collector(groupBy, parentGroupBy, topGroups, groupSort, withinGroupSort, groupedDocsToCollect, needScores, needScores, false);
    	return thirdPass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AbstractAllGroupHeadsCollector<?> createAllGroupCollector() throws IOException {
      Sort sortWithinGroup = withinGroupSort != null ? withinGroupSort : Sort.RELEVANCE;
      return TermAllGroupHeadsCollector.create(groupBy.getName(), sortWithinGroup);
    }

    @Override
    protected long getCount(BytesRef group){
    	return secondPass.getCollectors(group).getTotalHitCount();
    }
    /**
     * {@inheritDoc}
     */
    @Override
    protected void finish() throws IOException {
      result = thirdPass != null ? (TopGroups2<BytesRef, MutableValue>)thirdPass.getTopGroups(0) : null;
      finish(Group2Converter.fromMutable(parentGroupBy.getType(), groupBy.getType(), result));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMatches() {
      if (result == null && fallBackCollector == null) {
        return 0;
      }

      return result != null ? (int)result.totalHitCount : fallBackCollector.getTotalHits();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Integer getNumberOfGroups() {
      return allGroupsCollector == null ? null : allGroupsCollector.getGroupCount();
    }
  }
  /**
   * A group command for grouping on a query.
   */
  //NOTE: doesn't need to be generic. Maybe Command interface --> First / Second pass abstract impl.
  public class CommandQuery extends Command {

    public Query query;
    TopDocsCollector topCollector;
    FilterCollector collector;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void prepare() throws IOException {
      actualGroupsToFind = getMax(offset, numGroups, maxDoc);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createFirstPassCollector() throws IOException {
      DocSet groupFilt = searcher.getDocSet(query);
      topCollector = newCollector(withinGroupSort, needScores);
      collector = new FilterCollector(groupFilt, topCollector);
      return collector;
    }

    TopDocsCollector newCollector(Sort sort, boolean needScores) throws IOException {
      int groupDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      if (sort == null || sort.equals(Sort.RELEVANCE)) {
        return TopScoreDocCollector.create(groupDocsToCollect);
      } else {
        return TopFieldCollector.create(searcher.weightSort(sort), groupDocsToCollect, false, needScores, needScores);
      }
    }

    @Override
    protected long getCount(BytesRef group){
    	return 0;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    protected void finish() throws IOException {
      TopDocsCollector topDocsCollector = (TopDocsCollector) collector.getDelegate();
      TopDocs topDocs = topDocsCollector.topDocs();
      GroupDocs<String> groupDocs = new GroupDocs<>(Float.NaN, topDocs.getMaxScore(), topDocs.totalHits, topDocs.scoreDocs, query.toString(), null);
      if (main) {
        mainResult = getDocList(groupDocs);
      } else {
        NamedList rsp = commonResponse();
        addDocList(rsp, groupDocs);
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMatches() {
      return collector.getMatches();
    }
  }

  /**
   * A command for grouping on a function.
   */
  public class CommandFunc extends Command<MutableValue, MutableValue> {

    public ValueSource groupByVS;
    Map context;

    FunctionFirstPassGrouping2Collector firstPass;
    FunctionSecondPassGrouping2Collector secondPass;
    FunctionThirdPassGrouping2Collector thirdPass;
    
    // If offset falls outside the number of documents a group can provide use this collector instead of secondPass
    TotalHitCountCollector fallBackCollector;
    FunctionAllGroupsCollector allGroupsCollector;
    Collection<SearchGroup<BytesRef>> topGroups;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void prepare() throws IOException {
    	groupByVS = parentGroupBy.getType().getValueSource(parentGroupBy, null);;
      context = ValueSource.newContext(searcher);
      groupByVS.createWeight(context, searcher);
      actualGroupsToFind = getMax(offset, numGroups, maxDoc);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createFirstPassCollector() throws IOException {
      // Ok we don't want groups, but do want a total count
      if (actualGroupsToFind <= 0) {
        fallBackCollector = new TotalHitCountCollector();
        return fallBackCollector;
      }

      groupSort = groupSort == null ? Sort.RELEVANCE : groupSort;
      firstPass = new FunctionFirstPassGrouping2Collector(parentGroupBy, searcher.weightSort(groupSort), actualGroupsToFind);
      return firstPass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createSecondPassCollector() throws IOException {
//      if (actualGroupsToFind <= 0) {
//        allGroupsCollector = new FunctionAllGroupsCollector(groupBy, context);
//        return totalCount == TotalCount.grouped ? allGroupsCollector : null;
//      }

//      topGroups = format == Format.grouped ? firstPass.getTopGroups(offset, false) : firstPass.getTopGroups(0, false);
//      if (topGroups == null) {
//        if (totalCount == TotalCount.grouped) {
//          allGroupsCollector = new FunctionAllGroupsCollector(groupBy, context);
//          fallBackCollector = new TotalHitCountCollector();
//          return MultiCollector.wrap(allGroupsCollector, fallBackCollector);
//        } else {
//          fallBackCollector = new TotalHitCountCollector();
//          return fallBackCollector;
//        }
//      }
      topGroups = Group2Converter.fromMutable(parentGroupBy.getType(), null, (Collection)firstPass.getTopGroups(offset, false));

      int groupdDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      groupdDocsToCollect = Math.max(groupdDocsToCollect, 1);
      Sort withinGroupSort = this.withinGroupSort != null ? this.withinGroupSort : Sort.RELEVANCE;
      secondPass = new FunctionSecondPassGrouping2Collector(groupBy, parentGroupBy, null,
          topGroups, groupSort, groupdDocsToCollect
      );

//      if (totalCount == TotalCount.grouped) {
//        allGroupsCollector = new FunctionAllGroupsCollector(groupBy, context);
//        return MultiCollector.wrap(secondPass, allGroupsCollector);
//      } else {
        return secondPass;
//      }
    }

    @Override
    protected Collector createThirdPassCollector() throws IOException{
    	if(secondPass == null){
    		return null;
    	}
      Sort withinGroupSort = this.withinGroupSort != null ? this.withinGroupSort : Sort.RELEVANCE;
      int groupedDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      groupedDocsToCollect = Math.max(groupedDocsToCollect, 1);
    	Collection<CollectedSearchGroup2<MutableValue, MutableValue>> topGroups = secondPass.getTopGroupsNested(groupOffset, false);
    	thirdPass = new FunctionThirdPassGrouping2Collector(groupBy, parentGroupBy, topGroups, groupSort, withinGroupSort, groupedDocsToCollect, needScores, needScores, false);
    	return thirdPass;
    }
    
    @Override
    public AbstractAllGroupHeadsCollector<?> createAllGroupCollector() throws IOException {
      Sort sortWithinGroup = withinGroupSort != null ? withinGroupSort : Sort.RELEVANCE;
      return new FunctionAllGroupHeadsCollector(groupByVS, context, sortWithinGroup);
    }

    @Override
    protected long getCount(BytesRef group){
    	MutableValue mv = Group2Converter.convertToMutableValue(parentGroupBy, group);
    	return secondPass.getCollectors(mv).getTotalHitCount();
    }
    /**
     * {@inheritDoc}
     */
    @Override
    protected void finish() throws IOException {
      result = thirdPass != null ? thirdPass.getTopGroups(0) : null;
      finish(Group2Converter.fromMutable(parentGroupBy.getType(), groupBy.getType(), result));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMatches() {
      if (result == null && fallBackCollector == null) {
        return 0;
      }

      return result != null ? (int)result.totalHitCount : fallBackCollector.getTotalHits();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Integer getNumberOfGroups() {
      return allGroupsCollector == null ? null : allGroupsCollector.getGroupCount();
    }

  }

  public class CommandFuncField extends Command<MutableValue, BytesRef> {

    public ValueSource groupByVS;
    Map context;

    FunctionFirstPassGrouping2Collector firstPass;
    FunctionTermSecondPassGrouping2Collector secondPass;
    FunctionTermThirdPassGrouping2Collector thirdPass;
    
    // If offset falls outside the number of documents a group can provide use this collector instead of secondPass
    TotalHitCountCollector fallBackCollector;
    FunctionAllGroupsCollector allGroupsCollector;
    Collection<SearchGroup<BytesRef>> topGroups;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void prepare() throws IOException {
    	groupByVS = parentGroupBy.getType().getValueSource(parentGroupBy, null);;
      context = ValueSource.newContext(searcher);
      groupByVS.createWeight(context, searcher);
      actualGroupsToFind = getMax(offset, numGroups, maxDoc);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createFirstPassCollector() throws IOException {
      // Ok we don't want groups, but do want a total count
      if (actualGroupsToFind <= 0) {
        fallBackCollector = new TotalHitCountCollector();
        return fallBackCollector;
      }

      groupSort = groupSort == null ? Sort.RELEVANCE : groupSort;
      firstPass = new FunctionFirstPassGrouping2Collector(parentGroupBy, searcher.weightSort(groupSort), actualGroupsToFind);
      return firstPass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createSecondPassCollector() throws IOException {
      topGroups = Group2Converter.fromMutable(parentGroupBy.getType(), null, (Collection)firstPass.getTopGroups(offset, false));

      int groupdDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      groupdDocsToCollect = Math.max(groupdDocsToCollect, 1);
      Sort withinGroupSort = this.withinGroupSort != null ? this.withinGroupSort : Sort.RELEVANCE;
      secondPass = new FunctionTermSecondPassGrouping2Collector(groupBy, parentGroupBy, null,
          topGroups, groupSort, groupdDocsToCollect
      );

//      if (totalCount == TotalCount.grouped) {
//        allGroupsCollector = new FunctionAllGroupsCollector(groupBy, context);
//        return MultiCollector.wrap(secondPass, allGroupsCollector);
//      } else {
        return secondPass;
//      }
    }

    @Override
    protected Collector createThirdPassCollector() throws IOException{
    	if(secondPass == null){
    		return null;
    	}
      Sort withinGroupSort = this.withinGroupSort != null ? this.withinGroupSort : Sort.RELEVANCE;
      int groupedDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      groupedDocsToCollect = Math.max(groupedDocsToCollect, 1);
    	Collection<CollectedSearchGroup2<MutableValue, BytesRef>> topGroups = secondPass.getTopGroupsNested(groupOffset, false);
    	thirdPass = new FunctionTermThirdPassGrouping2Collector(groupBy, parentGroupBy, topGroups, groupSort, withinGroupSort, groupedDocsToCollect, needScores, needScores, false);
    	return thirdPass;
    }
    
    @Override
    public AbstractAllGroupHeadsCollector<?> createAllGroupCollector() throws IOException {
      Sort sortWithinGroup = withinGroupSort != null ? withinGroupSort : Sort.RELEVANCE;
      return new FunctionAllGroupHeadsCollector(groupByVS, context, sortWithinGroup);
    }

    @Override
    protected long getCount(BytesRef group){
    	MutableValue mv = Group2Converter.convertToMutableValue(parentGroupBy, group);
    	return secondPass.getCollectors(mv).getTotalHitCount();
    }
    /**
     * {@inheritDoc}
     */
    @Override
    protected void finish() throws IOException {
      result = thirdPass != null ? thirdPass.getTopGroups(0) : null;
      finish(Group2Converter.fromMutable(parentGroupBy.getType(), groupBy.getType(), result));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMatches() {
      if (result == null && fallBackCollector == null) {
        return 0;
      }

      return result != null ? (int)result.totalHitCount : fallBackCollector.getTotalHits();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Integer getNumberOfGroups() {
      return allGroupsCollector == null ? null : allGroupsCollector.getGroupCount();
    }

  }

  public class CommandFuncNull extends Command<MutableValue, MutableValue> {

    public ValueSource groupByVS;
    Map context;

    FunctionFirstPassGrouping2Collector firstPass;
    FunctionNullSecondPassGrouping2Collector secondPass;
    FunctionNullThirdPassGrouping2Collector thirdPass;
    
    // If offset falls outside the number of documents a group can provide use this collector instead of secondPass
    TotalHitCountCollector fallBackCollector;
    FunctionAllGroupsCollector allGroupsCollector;
    Collection<SearchGroup<BytesRef>> topGroups;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void prepare() throws IOException {
    	groupByVS = parentGroupBy.getType().getValueSource(parentGroupBy, null);;
      context = ValueSource.newContext(searcher);
      groupByVS.createWeight(context, searcher);
      actualGroupsToFind = getMax(offset, numGroups, maxDoc);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createFirstPassCollector() throws IOException {
      // Ok we don't want groups, but do want a total count
      if (actualGroupsToFind <= 0) {
        fallBackCollector = new TotalHitCountCollector();
        return fallBackCollector;
      }

      groupSort = groupSort == null ? Sort.RELEVANCE : groupSort;
      firstPass = new FunctionFirstPassGrouping2Collector(parentGroupBy, searcher.weightSort(groupSort), actualGroupsToFind);
      return firstPass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createSecondPassCollector() throws IOException {
      topGroups = Group2Converter.fromMutable(parentGroupBy.getType(), null, (Collection)firstPass.getTopGroups(offset, false));

      int groupedDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      groupedDocsToCollect = Math.max(groupedDocsToCollect, 1);
      Sort withinGroupSort = this.withinGroupSort != null ? this.withinGroupSort : Sort.RELEVANCE;
      secondPass = new FunctionNullSecondPassGrouping2Collector(parentGroupBy, null,
          topGroups, groupSort, groupSort, actualGroupsToFind, groupedDocsToCollect, true, true, true);

//      if (totalCount == TotalCount.grouped) {
//        allGroupsCollector = new FunctionAllGroupsCollector(groupBy, context);
//        return MultiCollector.wrap(secondPass, allGroupsCollector);
//      } else {
        return secondPass;
//      }
    }

    @Override
    protected Collector createThirdPassCollector() throws IOException{
    	if(secondPass == null){
    		return null;
    	}
      Sort withinGroupSort = this.withinGroupSort != null ? this.withinGroupSort : Sort.RELEVANCE;
      int groupedDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      groupedDocsToCollect = Math.max(groupedDocsToCollect, 1);
    	Collection<CollectedSearchGroup2<MutableValue, MutableValue>> topGroups = secondPass.getTopGroupsNested(groupOffset, false);
    	thirdPass = new FunctionNullThirdPassGrouping2Collector(parentGroupBy, topGroups, groupSort, withinGroupSort, groupedDocsToCollect, needScores, needScores, false);
    	return thirdPass;
    }
    
    @Override
    public AbstractAllGroupHeadsCollector<?> createAllGroupCollector() throws IOException {
      Sort sortWithinGroup = withinGroupSort != null ? withinGroupSort : Sort.RELEVANCE;
      return new FunctionAllGroupHeadsCollector(groupByVS, context, sortWithinGroup);
    }

    @Override
    protected long getCount(BytesRef group){
    	MutableValue mv = Group2Converter.convertToMutableValue(parentGroupBy, group);
    	return secondPass.getCollectors(mv).getTotalHitCount();
    }
    /**
     * {@inheritDoc}
     */
    @Override
    protected void finish() throws IOException {
      result = thirdPass != null ? thirdPass.getTopGroups(0) : null;
      finish(Group2Converter.fromMutable(parentGroupBy.getType(), new TrieIntField(), result));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMatches() {
      if (result == null && fallBackCollector == null) {
        return 0;
      }

      return result != null ? (int)result.totalHitCount : fallBackCollector.getTotalHits();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Integer getNumberOfGroups() {
      return allGroupsCollector == null ? null : allGroupsCollector.getGroupCount();
    }

  }

  public class CommandFieldNull extends Command<BytesRef, MutableValue> {

    TermFirstPassGrouping2Collector firstPass;
    TermNullSecondPassGrouping2Collector secondPass;
    TermNullThirdPassGrouping2Collector thirdPass;

    TermAllGroupsCollector allGroupsCollector;

    // If offset falls outside the number of documents a group can provide use this collector instead of secondPass
    TotalHitCountCollector fallBackCollector;
    Collection<SearchGroup<BytesRef>> topGroups;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void prepare() throws IOException {
      actualGroupsToFind = getMax(offset, numGroups, maxDoc);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createFirstPassCollector() throws IOException {
      // Ok we don't want groups, but do want a total count
      if (actualGroupsToFind <= 0) {
        fallBackCollector = new TotalHitCountCollector();
        return fallBackCollector;
      }

      groupSort = groupSort == null ? Sort.RELEVANCE : groupSort;
      firstPass = new TermFirstPassGrouping2Collector(parentGroupBy.getName(), groupSort, actualGroupsToFind);
      return firstPass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createSecondPassCollector() throws IOException {
      if (actualGroupsToFind <= 0) {
        allGroupsCollector = new TermAllGroupsCollector(groupBy.getName());
        return totalCount == TotalCount.grouped ? allGroupsCollector : null;
      }
      topGroups = format == Format.grouped ? firstPass.getTopGroups(offset, false) : firstPass.getTopGroups(0, false);
      if (topGroups == null) {
        if (totalCount == TotalCount.grouped) {
          allGroupsCollector = new TermAllGroupsCollector(groupBy.getName());
          fallBackCollector = new TotalHitCountCollector();
          return MultiCollector.wrap(allGroupsCollector, fallBackCollector);
        } else {
          fallBackCollector = new TotalHitCountCollector();
          return fallBackCollector;
        }
      }

      int groupedDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      groupedDocsToCollect = Math.max(groupedDocsToCollect, 1);
      secondPass = new TermNullSecondPassGrouping2Collector(
      		parentGroupBy, null, topGroups, groupSort, groupSort, actualGroupsToFind, groupedDocsToCollect, true, true, true
      );

      if (totalCount == TotalCount.grouped) {
        allGroupsCollector = new TermAllGroupsCollector(groupBy.getName());
        return MultiCollector.wrap(secondPass, allGroupsCollector);
      } else {
        return secondPass;
      }
    }
    
    @Override
    protected Collector createThirdPassCollector() throws IOException{
    	if(secondPass == null){
    		return null;
    	}
      Sort withinGroupSort = this.withinGroupSort != null ? this.withinGroupSort : Sort.RELEVANCE;
      int groupedDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      groupedDocsToCollect = Math.max(groupedDocsToCollect, 1);
    	Collection<CollectedSearchGroup2<BytesRef, MutableValue>> topGroups = secondPass.getTopGroupsNested(groupOffset, false);
    	thirdPass = new TermNullThirdPassGrouping2Collector(parentGroupBy, topGroups, groupSort, withinGroupSort, groupedDocsToCollect, needScores, needScores, false);
    	return thirdPass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AbstractAllGroupHeadsCollector<?> createAllGroupCollector() throws IOException {
      Sort sortWithinGroup = withinGroupSort != null ? withinGroupSort : Sort.RELEVANCE;
      return TermAllGroupHeadsCollector.create(groupBy.getName(), sortWithinGroup);
    }

    @Override
    protected long getCount(BytesRef group){
    	return secondPass.getCollectors(group).getTotalHitCount();
    }
    /**
     * {@inheritDoc}
     */
    @Override
    protected void finish() throws IOException{
    	result = thirdPass.getTopGroups(0);
    	finish(Group2Converter.fromMutable(parentGroupBy.getType(), new TrieIntField(), result));
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public int getMatches() {
      if (result == null && fallBackCollector == null) {
        return 0;
      }

      return result != null ? (int)result.totalHitCount : fallBackCollector.getTotalHits();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Integer getNumberOfGroups() {
      return allGroupsCollector == null ? null : allGroupsCollector.getGroupCount();
    }
  }
}
