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
package org.apache.lucene.search.grouping.term;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.AbstractThirdPassGrouping2Collector;
import org.apache.lucene.search.grouping.CollectedSearchGroup2;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.AbstractSecondPassGroupingCollector.SearchGroupDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SentinelIntSet;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.solr.schema.SchemaField;

/**
 * Concrete implementation of {@link org.apache.lucene.search.grouping.AbstractSecondPassGroupingCollector} that groups based on
 * field values and more specifically uses {@link org.apache.lucene.index.SortedDocValues}
 * to collect grouped docs.
 *
 * @lucene.experimental
 */
public class TermFunctionThirdPassGrouping2Collector extends AbstractThirdPassGrouping2Collector<BytesRef, MutableValue> {

  private final SchemaField groupParentField;
  private final SchemaField groupField;
  private final SentinelIntSet ordSet;

  private SortedDocValues indexParent;
  private FunctionValues.ValueFiller filler;
  private MutableValue mval;
  private ValueSource groupByVS;
  private Map<?, ?> vsContext;

  @SuppressWarnings({"unchecked", "rawtypes"})
  public TermFunctionThirdPassGrouping2Collector(SchemaField groupField, SchemaField groupParentField, Collection<CollectedSearchGroup2<BytesRef, MutableValue>> groups, 
  		                                   Sort groupSort, Sort withinGroupSort,
                                         int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields)
      throws IOException {
    super(groups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
    this.groupParentField = groupParentField;
    this.groupField = groupField;
    this.ordSet = new SentinelIntSet(groupMap.size(), -2);
    super.groupDocs = (SearchGroupDocs<BytesRef>[][]) new SearchGroupDocs[ordSet.keys.length][ordSet.keys.length];
  	groupByVS = groupField.getType().getValueSource(groupField, null);
  	vsContext = new HashMap<>();
  }

  @Override
  protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
    super.doSetNextReader(readerContext);
    indexParent = DocValues.getSorted(readerContext.reader(), groupParentField.getName());
    FunctionValues values = groupByVS.getValues(vsContext, readerContext);
    filler = values.getValueFiller();
    mval = filler.getValue();
  }

  @Override
  protected SearchGroupDocs<MutableValue> retrieveGroup(int doc) throws IOException {
  	
  	Map<MutableValue, SearchGroupDocs<MutableValue>> m = groupMap.get(indexParent.get(doc));
  	if( m == null){
  		return null;
  	}
  	filler.fillValue(doc);
    if(!mval.exists()){
    	return null;
    }
  	return m.get(mval);
  }

}
