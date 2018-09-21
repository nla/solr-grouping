package org.apache.solr.search.grouping.distributed.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.AbstractAllGroupsCollector;
import org.apache.lucene.search.grouping.AbstractFirstPassGrouping2Collector;
import org.apache.lucene.search.grouping.AbstractSecondPassGrouping2Collector;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.function.FunctionFirstPassGrouping2Collector;
import org.apache.lucene.search.grouping.function.FunctionNullSecondPassGrouping2Collector;
import org.apache.lucene.search.grouping.function.FunctionSecondPassGrouping2Collector;
import org.apache.lucene.search.grouping.function.FunctionTermSecondPassGrouping2Collector;
import org.apache.lucene.search.grouping.term.TermFirstPassGrouping2Collector;
import org.apache.lucene.search.grouping.term.TermFunctionSecondPassGrouping2Collector;
import org.apache.lucene.search.grouping.term.TermNullSecondPassGrouping2Collector;
import org.apache.lucene.search.grouping.term.TermSecondPassGrouping2Collector;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieIntField;
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
    private Integer maxDocsPerGroup = 1;

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
    
    public Builder setMaxDocsPerGroup(Integer maxDocsPerGroup){
			this.maxDocsPerGroup = maxDocsPerGroup;
			return this;
		}
    
    public SearchGroups2FieldCommand build() {
      if (parentField == null || groupSort == null || topNGroups == null) {
        throw new IllegalStateException("All fields must be set");
      }

      return new SearchGroups2FieldCommand(field, parentField, groupSort, topNGroups, maxDocsPerGroup, includeGroupCount, // firstGroupField, 
      		searchGroups);
    }

  }

  private SchemaField field;
  private final SchemaField parentField;
  private final Sort groupSort;
  private final int topNGroups;
  private Integer maxDocsPerGroup;
  private final boolean includeGroupCount;
  private Collection<SearchGroup<BytesRef>> searchGroups;

  private AbstractFirstPassGrouping2Collector firstPassGroupingCollector = null;
  private AbstractSecondPassGrouping2Collector secondPassGroupingCollector = null;
  private AbstractAllGroupsCollector allGroupsCollector;

  private SearchGroups2FieldCommand(SchemaField field, SchemaField parentField, Sort groupSort, 
  		int topNGroups, int maxDocsPerGroup, boolean includeGroupCount, Collection<SearchGroup<BytesRef>> searchGroups) {
    this.field = field;
    this.parentField = parentField;
    this.groupSort = groupSort;
    this.topNGroups = topNGroups;
    this.maxDocsPerGroup = maxDocsPerGroup;
    this.includeGroupCount = includeGroupCount;
    this.searchGroups = searchGroups;
  }

  @Override
  public List<Collector> create() throws IOException {
    final List<Collector> collectors = new ArrayList<>(2);
    final FieldType parentFieldType = parentField.getType();
    if (topNGroups > 0) {
      if(searchGroups == null){
        if (parentFieldType.getNumericType() != null) {
        	firstPassGroupingCollector = new FunctionFirstPassGrouping2Collector(parentField, groupSort, topNGroups);
        }
        else{
        	firstPassGroupingCollector = new TermFirstPassGrouping2Collector(parentField.getName(), groupSort, topNGroups);
        }
        collectors.add(firstPassGroupingCollector);
      }
      else{
        FieldType fieldType = null;
        if(field != null){
        	fieldType = field.getType();
        }
        if (parentFieldType.getNumericType() != null){
        	if(field == null){
	      		secondPassGroupingCollector = new FunctionNullSecondPassGrouping2Collector(parentField, 
	      				null,  searchGroups, groupSort, groupSort, topNGroups, maxDocsPerGroup, true, true, true);        		
        	}
        	else if (fieldType.getNumericType() != null ) {
	      		secondPassGroupingCollector = new FunctionSecondPassGrouping2Collector(field, parentField, 
	      				null,  searchGroups, groupSort, topNGroups);
	      	} 
	      	else if(fieldType.getNumericType() == null){
	      		secondPassGroupingCollector = new FunctionTermSecondPassGrouping2Collector(field, parentField, 
	      				null,  searchGroups, groupSort, topNGroups);
	      	}
        }
        else{
        	if(field == null){
	      		secondPassGroupingCollector = new TermNullSecondPassGrouping2Collector(parentField, 
	      				null,  searchGroups, groupSort, groupSort, topNGroups, maxDocsPerGroup, true, true, true);        		
        	}
	      	else if(parentFieldType.getNumericType() == null && fieldType.getNumericType() != null){
	      		secondPassGroupingCollector = new TermFunctionSecondPassGrouping2Collector(field, parentField, 
	      				null,  searchGroups, groupSort, topNGroups);
	      	}
	      	else{
	      		secondPassGroupingCollector = new TermSecondPassGrouping2Collector(field, parentField, 
	      				null,  searchGroups, groupSort, topNGroups);
	      	}
        }
      	collectors.add(secondPassGroupingCollector);
      }
    }
//    if (includeGroupCount) {
//      if (fieldType.getNumericType() != null) {
//        ValueSource vs = fieldType.getValueSource(field, null);
//        allGroupsCollector = new FunctionAllGroupsCollector(vs, new HashMap<Object,Object>());
//      } else {
//        allGroupsCollector = new TermAllGroupsCollector(field.getName());
//      }
//      collectors.add(allGroupsCollector);
//    }
    return collectors;
  }

  @Override
  public SearchGroupsFieldCommandResult result() {
    final Collection<SearchGroup<BytesRef>> topGroups;
    if(firstPassGroupingCollector != null){
//    	 if (parentField.getType().getNumericType() != null){
    		 topGroups = Group2Converter.fromMutable(parentField.getType(), null,firstPassGroupingCollector.getTopGroups(0, true)); 
//    	 }
//    	 else{
//    		 topGroups = firstPassGroupingCollector.getTopGroups(0, true);
//    	 }
    }
    else if (secondPassGroupingCollector != null) {
//      if (parentField.getType().getNumericType() != null) {
    	FieldType ft;
    	if(field != null){
    		ft = field.getType();
    	}
    	else{
    		ft = new TrieIntField();
    	}
        topGroups = Group2Converter.fromMutable(parentField.getType(), ft, secondPassGroupingCollector.getTopGroupsNested(0, true));
//      } else {
//        topGroups = secondPassGroupingCollector.getTopGroupsNested(0, true);
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
  	if(field == null){
  		return getParentKey();
  	}
    return field.getName();
  }
  public Collection<SearchGroup<BytesRef>> getSearchGroups(){
		return searchGroups;
	}
  public AbstractSecondPassGrouping2Collector getSecondPassGroupingCollector(){
		return secondPassGroupingCollector;
	}
}
