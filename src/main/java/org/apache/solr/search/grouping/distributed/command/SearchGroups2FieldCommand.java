package org.apache.solr.search.grouping.distributed.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

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

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.AbstractAllGroupsCollector;
import org.apache.lucene.search.grouping.AbstractSecondPassGrouping2Collector;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.function.FunctionAllGroupsCollector;
import org.apache.lucene.search.grouping.term.FunctionSecondPassGrouping2Collector;
import org.apache.lucene.search.grouping.term.TermAllGroupsCollector;
import org.apache.lucene.search.grouping.term.TermSecondPassGrouping2Collector;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.grouping.Command;

/**
 * Creates all the collectors needed for the first phase and how to handle the results.
 */
public class SearchGroups2FieldCommand implements Command<SearchGroupsFieldCommandResult> {

  public static class Builder {

    private SchemaField field;
    private SchemaField parentField;
    private Sort groupSort;
    private Integer topNGroups;
    private boolean includeGroupCount = false;
    private Collection<SearchGroup<BytesRef>> searchGroups;

    public Builder setField(SchemaField field) {
      this.field = field;
      return this;
    }

    public Builder setParentField(SchemaField field) {
      this.parentField = field;
      return this;
    }

    public Builder setGroupSort(Sort groupSort) {
      this.groupSort = groupSort;
      return this;
    }

    public Builder setTopNGroups(int topNGroups) {
      this.topNGroups = topNGroups;
      return this;
    }

    public Builder setIncludeGroupCount(boolean includeGroupCount) {
      this.includeGroupCount = includeGroupCount;
      return this;
    }

    public Builder setSearchGroups(Collection<SearchGroup<BytesRef>> searchGroups){
			this.searchGroups = searchGroups;
			return this;
		}
    
    public SearchGroups2FieldCommand build() {
      if (field == null || groupSort == null || topNGroups == null) {
        throw new IllegalStateException("All fields must be set");
      }

      return new SearchGroups2FieldCommand(field, parentField, groupSort, topNGroups, includeGroupCount, // firstGroupField, 
      		searchGroups);
    }

  }

  private final SchemaField field;
  private final SchemaField parentField;
  private final Sort groupSort;
  private final int topNGroups;
  private final boolean includeGroupCount;
  private Collection<SearchGroup<BytesRef>> searchGroups;

  private AbstractSecondPassGrouping2Collector secondPassGroupingCollector;
  private AbstractAllGroupsCollector allGroupsCollector;

  private SearchGroups2FieldCommand(SchemaField field, SchemaField parentField, Sort groupSort, 
  		int topNGroups, boolean includeGroupCount, Collection<SearchGroup<BytesRef>> searchGroups) {
    this.field = field;
    this.parentField = parentField;
    this.groupSort = groupSort;
    this.topNGroups = topNGroups;
    this.includeGroupCount = includeGroupCount;
    this.searchGroups = searchGroups;
  }

  @Override
  public List<Collector> create() throws IOException {
    final List<Collector> collectors = new ArrayList<>(2);
    final FieldType fieldType = field.getType();
    if (topNGroups > 0) {
      if (fieldType.getNumericType() != null) {
      	secondPassGroupingCollector = new FunctionSecondPassGrouping2Collector(field, parentField, 
        		null,  searchGroups, groupSort, topNGroups);
      } else {
      	secondPassGroupingCollector = new TermSecondPassGrouping2Collector(field.getName(), parentField.getName(), 
        		null,  searchGroups, groupSort, topNGroups);
//        firstPassGroupingCollector = new TermFirstPassGroupingCollector(field.getName(), (parentField == null ? null : parentField.getName()), 
//        		groupSort, topNGroups);
//        if(searchGroups != null){
//	        for(SearchGroup sg : searchGroups){
//	        	firstPassGroupingCollector.getGroupMap().put(sg.groupValue, sg);
//	        }
//        }
      }
      collectors.add(secondPassGroupingCollector);
    }
    if (includeGroupCount) {
      if (fieldType.getNumericType() != null) {
        ValueSource vs = fieldType.getValueSource(field, null);
        allGroupsCollector = new FunctionAllGroupsCollector(vs, new HashMap<Object,Object>());
      } else {
        allGroupsCollector = new TermAllGroupsCollector(field.getName());
      }
      collectors.add(allGroupsCollector);
    }
    return collectors;
  }

  @Override
  public SearchGroupsFieldCommandResult result() {
    final Collection<SearchGroup<BytesRef>> topGroups;
    if (secondPassGroupingCollector != null) {
//      if (field.getType().getNumericType() != null) {
//        topGroups = GroupConverter.fromMutable(field, secondPassGroupingCollector.getTopGroupsNested(0, true));
//      } else {
        topGroups = secondPassGroupingCollector.getTopGroupsNested(0, true);
//      }
    } else {
      topGroups = Collections.emptyList();
    }
    final Integer groupCount;
    if (allGroupsCollector != null) {
      groupCount = allGroupsCollector.getGroupCount();
    } else {
      groupCount = null;
    }
    return new SearchGroupsFieldCommandResult(groupCount, topGroups);
  }

  @Override
  public Sort getSortWithinGroup() {
    return null;
  }

  @Override
  public Sort getGroupSort() {
    return groupSort;
  }

  public String getParentKey() {
    return parentField.getName();
  }
  @Override
  public String getKey() {
    return field.getName();
  }
  public Collection<SearchGroup<BytesRef>> getSearchGroups(){
		return searchGroups;
	}
  public AbstractSecondPassGrouping2Collector getSecondPassGroupingCollector(){
		return secondPassGroupingCollector;
	}
}
