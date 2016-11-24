package org.apache.lucene.search.grouping;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.search.Sort;

public class CollectedSearchGroup2<GROUP_VALUE_TYPE> extends CollectedSearchGroup<GROUP_VALUE_TYPE>{
	public Collection<SearchGroup<GROUP_VALUE_TYPE>> subGroups;
	public long groupCount;

	public CollectedSearchGroup2(){
	}
	
	public CollectedSearchGroup2(SearchGroup<GROUP_VALUE_TYPE> searchGroup){
		this.groupValue = searchGroup.groupValue;
		this.sortValues = searchGroup.sortValues;
		if(searchGroup instanceof CollectedSearchGroup){
			this.topDoc = ((CollectedSearchGroup<?>)searchGroup).topDoc;
			this.comparatorSlot = ((CollectedSearchGroup<?>)searchGroup).comparatorSlot;
			if(searchGroup instanceof CollectedSearchGroup2){
				this.subGroups = ((CollectedSearchGroup2<GROUP_VALUE_TYPE>)searchGroup).subGroups;
				this.groupCount = ((CollectedSearchGroup2<GROUP_VALUE_TYPE>)searchGroup).groupCount;
			}
		}
	}
	
//  public static <GROUP_VALUE_TYPE> Collection<SearchGroup<GROUP_VALUE_TYPE>> merge(List<Collection<SearchGroup<GROUP_VALUE_TYPE>>> topGroups, int offset, int topN, Sort groupSort){
//  	if()
//  }

}
