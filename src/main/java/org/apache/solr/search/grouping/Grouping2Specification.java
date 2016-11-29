package org.apache.solr.search.grouping;

public class Grouping2Specification extends GroupingSpecification{
  private String subField = null;

//  public Map<String, String> getParentFields(){
//		return parentFields;
//	}
//  public void setParentFields(Map<String, String> parentFields){
//		this.parentFields = parentFields;
//	}
//  public void setParentFields(String parent, String field){
//		this.parentFields.put(parent, field);
//	}
  
  public String getField(){
  	return this.getFields()[0];
  }
  public String getSubField(){
  	return subField;
  }
  public void setSubField(String subField){
		this.subField = subField;
	}
}
