package org.apache.solr.search.grouping;

public class Grouping2Specification extends GroupingSpecification{
  private String subField = null;
  private long totalHitCount;
  
  public String getField(){
  	return this.getFields()[0];
  }
  public String getSubField(){
  	return subField;
  }
  public void setSubField(String subField){
		this.subField = subField;
	}
  
  public long getTotalHitCount(){
		return totalHitCount;
	}
  public void setTotalHitCount(long totalHitCount){
		this.totalHitCount = totalHitCount;
	}
  public boolean isSingleGrouped(){
		return subField == null;
	}
}
