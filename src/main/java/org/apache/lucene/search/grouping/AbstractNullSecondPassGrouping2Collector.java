package org.apache.lucene.search.grouping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.util.mutable.MutableValueInt;

public abstract class AbstractNullSecondPassGrouping2Collector<GROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE>
		extends AbstractSecondPassGrouping2Collector<GROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE>{

	public  TopDocsCollector<?> collector = null;
	private LeafCollector leafCollector;
	private int maxDocsPerGroup;

	public AbstractNullSecondPassGrouping2Collector(Sort groupSort,
			int topNGroups, int maxDocsPerGroup) throws IOException{
		super(groupSort, topNGroups);
		this.maxDocsPerGroup = maxDocsPerGroup;
	}

	@Override
	public void collect(int doc) throws IOException{
  	if(collectors != null){
  		super.collect(doc);
		}
		else{
			leafCollector.setScorer(this.holdScore);
			leafCollector.collect(doc);
			// also fill group map
			super.collect(doc);
		}
	}

	@Override
	protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
		if(collectors != null){
			for(AbstractSecondPassGrouping2Collector<?, ?> c : collectors.values()){
				if(c instanceof AbstractNullSecondPassGrouping2Collector<?, ?>){
					AbstractNullSecondPassGrouping2Collector< ?, ?> col = (AbstractNullSecondPassGrouping2Collector<?, ?>)c;
					col.leafCollector = col.collector.getLeafCollector(readerContext);
				}
			}
			super.doSetNextReader(readerContext);
		}
  }

	@Override
	public TopGroups2<GROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE> getTopDocs(
			int withinGroupOffset){
    final List<Group2Docs<GROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE>> groupDocsResult = new ArrayList<>();
    float maxScore = Float.MIN_VALUE;

    for(CollectedSearchGroup2<GROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE> group : topGroupsFirstPass){
			AbstractNullSecondPassGrouping2Collector<SUBGROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE> c = 
					(AbstractNullSecondPassGrouping2Collector<SUBGROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE>)collectors.get(group.groupValue);
			 
			final TopDocs topDocs = c.collector.topDocs(withinGroupOffset, maxDocsPerGroup);
			MutableValueInt mv = new MutableValueInt();
			mv.value = 1;
			Group2Docs g2d = new Group2Docs<>(Float.NaN,
	        topDocs.getMaxScore(),
	        topDocs.totalHits,
	        topDocs.scoreDocs,
	        group.groupValue,
	        mv,	                                                                    
	        group.sortValues);
			groupDocsResult.add(g2d);
      maxScore = Math.max(maxScore, topDocs.getMaxScore());
		}
    return new TopGroups2<>(groupSort.getSort(),
        groupSort.getSort(),
        -1 /*totalGroupedHitCount*/, groupDocsResult.toArray(new Group2Docs[0]),
        maxScore);
	}
}
