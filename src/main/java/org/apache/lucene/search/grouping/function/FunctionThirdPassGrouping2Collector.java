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
package org.apache.lucene.search.grouping.function;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.solr.search.grouping.distributed.command.Group2Converter;

/**
 * Concrete implementation of {@link org.apache.lucene.search.grouping.AbstractSecondPassGroupingCollector} that groups based on
 * field values and more specifically uses {@link org.apache.lucene.index.SortedDocValues}
 * to collect grouped docs.
 *
 * @lucene.experimental
 */
public class FunctionThirdPassGrouping2Collector extends AbstractThirdPassGrouping2Collector<MutableValue, MutableValue> {

  private String groupField;
  private SchemaField groupSchemaField;
  private String groupParentField;
  private SchemaField groupParentSchemaField;

  private FunctionValues.ValueFiller parentFiller;
  private MutableValue parentMVal;
  private ValueSource parentGroupByVS;
  private Map<?, ?> parentVsContext;
  private FunctionValues.ValueFiller filler;
  private MutableValue mval;
  private ValueSource groupByVS;
  private Map<?, ?> vsContext;

  private FunctionThirdPassGrouping2Collector parent;

  @SuppressWarnings({"unchecked", "rawtypes"})
  public FunctionThirdPassGrouping2Collector(SchemaField groupField, SchemaField groupParentField, Collection<CollectedSearchGroup2<MutableValue, MutableValue>> groups, 
  		                                   Sort groupSort, Sort withinGroupSort,
                                         int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields)
      throws IOException {
    super(groups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
    this.groupParentField = groupParentField.getName();
    this.groupParentSchemaField = groupParentField;
    this.groupField = groupField.getName();
    this.groupSchemaField = groupField;
  	parentGroupByVS = groupParentField.getType().getValueSource(groupParentField, null);
  	groupByVS = groupField.getType().getValueSource(groupField, null);
  	parentVsContext = new HashMap<>();
  	vsContext = new HashMap<>();

  }

  @Override
  protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
    super.doSetNextReader(readerContext);
    FunctionValues values = groupByVS.getValues(vsContext, readerContext);
    filler = values.getValueFiller();
    mval = filler.getValue();
    values = parentGroupByVS.getValues(parentVsContext, readerContext);
    parentFiller = values.getValueFiller();
    parentMVal = parentFiller.getValue();
  }

  @Override
  protected SearchGroupDocs<MutableValue> retrieveGroup(int doc) throws IOException {
  	parentFiller.fillValue(doc);
    filler.fillValue(doc);
    if(!parentMVal.exists()){
    	return null;
    }
  	Map<MutableValue, SearchGroupDocs<MutableValue>> m = groupMap.get(parentMVal);
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
