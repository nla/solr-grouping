package org.apache.lucene.search.grouping.function;

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
import org.apache.lucene.search.grouping.AbstractSecondPassGrouping2Collector;
import org.apache.lucene.search.grouping.CollectedSearchGroup2;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.term.TermSecondPassGrouping2Collector;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.grouping.distributed.command.Group2Converter;

/**
 * Concrete implementation of {@link org.apache.lucene.search.grouping.AbstractFirstPassGroupingCollector} that groups based on
 * field values and more specifically uses {@link org.apache.lucene.index.SortedDocValues}
 * to collect groups.
 *
 * @lucene.experimental
 */
public class FunctionTermSecondPassGrouping2Collector extends AbstractSecondPassGrouping2Collector<MutableValue, BytesRef> {

  private SortedDocValues index;
  private String groupField;
  private SchemaField groupSchemaField;
  private String groupParentField;
  private SchemaField groupParentSchemaField;

  private FunctionValues.ValueFiller parentFiller;
  private MutableValue parentMVal;
  private ValueSource parentGroupByVS;
  private Map<?, ?> parentVsContext;

  
  /**
   * Create the first pass collector.
   *
   *  @param groupField The field used to group
   *    documents. This field must be single-valued and
   *    indexed (DocValues is used to access its value
   *    per-document).
   *  @param groupSort The {@link Sort} used to sort the
   *    groups.  The top sorted document within each group
   *    according to groupSort, determines how that group
   *    sorts against other groups.  This must be non-null,
   *    ie, if you want to groupSort by relevance use
   *    Sort.RELEVANCE.
   *  @param topNGroups How many top groups to keep.
   *  @throws IOException When I/O related errors occur
   */
  public FunctionTermSecondPassGrouping2Collector(SchemaField groupField, SchemaField groupParentField,
  		AbstractSecondPassGrouping2Collector<?, ?> parent,
  		Collection<SearchGroup<BytesRef>> topGroups, Sort groupSort, int topNGroups) throws IOException {
    super(groupSort, topNGroups);
    this.groupField = groupField.getName();
    this.groupSchemaField = groupField;
    this.groupParentField = groupParentField.getName();
    this.parent = parent;
    this.topGroupsFirstPass = new ArrayList<>();
    if(parent == null){
    	// this is the parent so setup the document readers.
    	parentGroupByVS = groupParentField.getType().getValueSource(groupParentField, null);
    	parentVsContext = new HashMap<>();
    }
    if(topGroups != null){
      Collection<SearchGroup<MutableValue>> topGroupsMV = Group2Converter.toMutable(groupParentField, topGroups);
    	collectors = new HashMap<>();
	    for(SearchGroup<MutableValue> e : topGroupsMV){
	    	TermSecondPassGrouping2Collector c = new TermSecondPassGrouping2Collector(groupField, groupParentField, this, null, groupSort, topNGroups);
	    	collectors.put(e.groupValue, c);
	    	topGroupsFirstPass.add(new CollectedSearchGroup2(e));
	    }
    }
  }

  @Override
  public MutableValue getDocGroupParentValue(int doc) {
  	if(parent != null){
  		return (MutableValue)parent.getDocGroupParentValue(doc);
  	}
    parentFiller.fillValue(doc);
    if(!parentMVal.exists()){
    	return null;
    }
    return parentMVal;
  }
  
	@Override
	public BytesRef getDocGroupValue(int doc){
  	if(parent != null){
  		return (BytesRef)parent.getDocGroupValue(doc);
  	}
    final int ord = index.getOrd(doc);
    if (ord == -1) {
      return null;
    } else {
      return index.lookupOrd(ord);
    }
	}

  @Override
  protected BytesRef copyDocGroupValue(BytesRef groupValue, BytesRef reuse) {
    if (groupValue == null) {
      return null;
    } else if (reuse != null) {
      reuse.bytes = ArrayUtil.grow(reuse.bytes, groupValue.length);
      reuse.offset = 0;
      reuse.length = groupValue.length;
      System.arraycopy(groupValue.bytes, groupValue.offset, reuse.bytes, 0, groupValue.length);
      return reuse;
    } else {
      return BytesRef.deepCopyOf(groupValue);
    }
  }



  @Override
  protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
    super.doSetNextReader(readerContext);
    index = DocValues.getSorted(readerContext.reader(), groupField);
    FunctionValues  values = parentGroupByVS.getValues(parentVsContext, readerContext);
    parentFiller = values.getValueFiller();
    parentMVal = parentFiller.getValue();
  }
}
