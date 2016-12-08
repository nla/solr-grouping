package org.apache.lucene.search.grouping;

import org.apache.lucene.search.ScoreDoc;

public class Group2Docs<GROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE> extends GroupDocs<SUBGROUP_VALUE_TYPE>{
  public final GROUP_VALUE_TYPE groupParentValue;
  
	public Group2Docs(float score,
      float maxScore,
      int totalHits,
      ScoreDoc[] scoreDocs,
      GROUP_VALUE_TYPE groupParentValue,
      SUBGROUP_VALUE_TYPE groupValue,
      Object[] groupSortValues){
		super(score, maxScore, totalHits, scoreDocs, groupValue, groupSortValues);
		this.groupParentValue = groupParentValue;
	}
}
