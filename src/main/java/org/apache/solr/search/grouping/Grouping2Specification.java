package org.apache.solr.search.grouping;

import java.util.HashMap;
import java.util.Map;

public class Grouping2Specification extends GroupingSpecification{
  private Map<String, String> parentFields = new HashMap<>();

  public Map<String, String> getParentFields(){
		return parentFields;
	}
  public void setParentFields(Map<String, String> parentFields){
		this.parentFields = parentFields;
	}
  public void setParentFields(String parent, String field){
		this.parentFields.put(parent, field);
	}
}
