package org.apache.lucene.search.grouping;

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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.search.grouping.term.TermSecondPassGrouping2Collector;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.*;

/** FirstPassGrouping2Collector is the first of three passes necessary
 *  to collect grouped hits.  This pass gathers the top N sorted
 *  groups. Concrete subclasses define what a group is and how it
 *  is internally collected.
 *
 *  <p>See {@link org.apache.lucene.search.grouping} for more
 *  details including a full code example.</p>
 *
 * @lucene.experimental
 */
abstract public class AbstractSecondPassGrouping2Collector<GROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE> extends SimpleCollector {

  private final FieldComparator<?>[] comparators;
  private final LeafFieldComparator[] leafComparators;
  private final int[] reversed;
  private final int topNGroups;
  private final boolean needsScores;
  protected Map<SUBGROUP_VALUE_TYPE, CollectedSearchGroup2<SUBGROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE>> groupMap;
  private final int compIDXEnd;
  protected Collection<CollectedSearchGroup2<GROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE>> topGroupsFirstPass;
  protected Map<GROUP_VALUE_TYPE, AbstractSecondPassGrouping2Collector<SUBGROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE>> collectors;
  protected AbstractSecondPassGrouping2Collector<?, ?> parent;

  // Set once we reach topNGroups unique groups:
  /** @lucene.internal */
  protected TreeSet<CollectedSearchGroup2<SUBGROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE>> orderedGroups;
  private int docBase;
  private int spareSlot;

  private long totalHitCount = 0;

  /**
   * Create the first pass collector.
   *
   *  @param groupSort The {@link Sort} used to sort the
   *    groups.  The top sorted document within each group
   *    according to groupSort, determines how that group
   *    sorts against other groups.  This must be non-null,
   *    ie, if you want to groupSort by relevance use
   *    Sort.RELEVANCE.
   *  @param topNGroups How many top groups to keep.
   *  @throws IOException If I/O related errors occur
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public AbstractSecondPassGrouping2Collector(Sort groupSort, int topNGroups) throws IOException {
    if (topNGroups < 1) {
      throw new IllegalArgumentException("topNGroups must be >= 1 (got " + topNGroups + ")");
    }

    // TODO: allow null groupSort to mean "by relevance",
    // and specialize it?

    this.topNGroups = topNGroups;
    this.needsScores = groupSort.needsScores();
    final SortField[] sortFields = groupSort.getSort();
    comparators = new FieldComparator[sortFields.length];
    leafComparators = new LeafFieldComparator[sortFields.length];
    compIDXEnd = comparators.length - 1;
    reversed = new int[sortFields.length];
    for (int i = 0; i < sortFields.length; i++) {
      final SortField sortField = sortFields[i];

      // use topNGroups + 1 so we have a spare slot to use for comparing (tracked by this.spareSlot):
      comparators[i] = sortField.getComparator(topNGroups + 1, i);
      reversed[i] = sortField.getReverse() ? -1 : 1;
    }

    spareSlot = topNGroups;
    groupMap = new HashMap<>(topNGroups);
  }

  @Override
  public boolean needsScores() {
    return needsScores;
  }

  public Collection<CollectedSearchGroup2<GROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE>> getTopGroupsNested(int groupOffset, boolean fillFields) {
  	// as this is the parent collector we go through the top groups from the first pass 
  	// and add the second level data
  	for(CollectedSearchGroup2<GROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE> g : topGroupsFirstPass){
  		AbstractSecondPassGrouping2Collector collector = collectors.get(g.groupValue); 
  		g.subGroups = collector.getTopGroups(groupOffset, fillFields);
  		g.groupCount = collector.totalHitCount;
  	}
  	return topGroupsFirstPass;
  }

//  protected abstract BytesRef getValueAsBytesRefx(GROUP_VALUE_TYPE ref);

  /**
   * Returns top groups, starting from offset.  This may
   * return null, if no groups were collected, or if the
   * number of unique groups collected is &lt;= offset.
   *
   * @param groupOffset The offset in the collected groups
   * @param fillFields Whether to fill to {@link SearchGroup2#sortValues}
   * @return top groups, starting from offset
   */
  public Collection<SearchGroup<SUBGROUP_VALUE_TYPE>> getTopGroups(int groupOffset, boolean fillFields) {

    //System.out.println("FP.getTopGroups groupOffset=" + groupOffset + " fillFields=" + fillFields + " groupMap.size()=" + groupMap.size());

    if (groupOffset < 0) {
      throw new IllegalArgumentException("groupOffset must be >= 0 (got " + groupOffset + ")");
    }

    if (groupMap.size() <= groupOffset) {
      return null;
    }

    if (orderedGroups == null) {
      buildSortedSet();
    }

    final Collection<SearchGroup<SUBGROUP_VALUE_TYPE>> result = new ArrayList<>();
    int upto = 0;
    final int sortFieldCount = comparators.length;
    for(CollectedSearchGroup2<SUBGROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE> group : orderedGroups) {
      if (upto++ < groupOffset) {
        continue;
      }
      //System.out.println("  group=" + (group.groupValue == null ? "null" : group.groupValue));
      CollectedSearchGroup2<SUBGROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE> searchGroup = new CollectedSearchGroup2<>();
      searchGroup.groupValue = group.groupValue;
      if (fillFields) {
        searchGroup.sortValues = new Object[sortFieldCount];
        for(int sortFieldIDX=0;sortFieldIDX<sortFieldCount;sortFieldIDX++) {
          searchGroup.sortValues[sortFieldIDX] = comparators[sortFieldIDX].value(group.comparatorSlot);
        }
      }
      result.add(searchGroup);
    }
    //System.out.println("  return " + result.size() + " groups");
    return result;
  }

  private Scorer holdScore;
  @Override
  public void setScorer(Scorer scorer) throws IOException {
  	if(collectors != null){
  		holdScore = scorer;
  		return;
  	}
    for (LeafFieldComparator comparator : leafComparators) {
      comparator.setScorer(scorer);
    }
  }

  @Override
  public void collect(int doc) throws IOException {
  	if(collectors != null){
      final GROUP_VALUE_TYPE groupParentValue = getDocGroupParentValue(doc);
      AbstractSecondPassGrouping2Collector<SUBGROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE> collector = collectors.get(groupParentValue);
      if(collector == null){
      	return; // not collecting this record
      }
      collector.setScorer(holdScore);
      collector.collect(doc);
      return;
  	}
    totalHitCount++;

//    System.out.println("SP.collect doc=" + doc);

    // If orderedGroups != null we already have collected N groups and
    // can short circuit by comparing this document to the bottom group,
    // without having to find what group this document belongs to.
    
    // Even if this document belongs to a group in the top N, we'll know that
    // we don't have to update that group.

    // Downside: if the number of unique groups is very low, this is
    // wasted effort as we will most likely be updating an existing group.
    if (orderedGroups != null) {
      for (int compIDX = 0;; compIDX++) {
        final int c = reversed[compIDX] * leafComparators[compIDX].compareBottom(doc);
        if (c < 0) {
          // Definitely not competitive. So don't even bother to continue
          return;
        } else if (c > 0) {
          // Definitely competitive.
          break;
        } else if (compIDX == compIDXEnd) {
          // Here c=0. If we're at the last comparator, this doc is not
          // competitive, since docs are visited in doc Id order, which means
          // this doc cannot compete with any other document in the queue.
          return;
        }
      }
    }

    // TODO: should we add option to mean "ignore docs that
    // don't have the group field" (instead of stuffing them
    // under null group)?
    final CollectedSearchGroup2<SUBGROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE> group;
    final SUBGROUP_VALUE_TYPE groupValue;

    groupValue = getDocGroupValue(doc);
    if(groupValue == null){
    	return; // don't wand grouping by null.
    }
    group = groupMap.get(groupValue);
    
    if (group == null) {

      // First time we are seeing this group, or, we've seen
      // it before but it fell out of the top N and is now
      // coming back

      if (groupMap.size() < topNGroups) {

        // Still in startup transient: we have not
        // seen enough unique groups to start pruning them;
        // just keep collecting them

        // Add a new CollectedSearchGroup:
        CollectedSearchGroup2<SUBGROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE> sg = new CollectedSearchGroup2<>();
        sg.groupValue = copyDocGroupValue(groupValue, null);
        sg.comparatorSlot = groupMap.size();
//        sg.subGroupMap = new HashMap<>();
        sg.topDoc = docBase + doc;
        for (LeafFieldComparator fc : leafComparators) {
          fc.copy(sg.comparatorSlot, doc);
        }
        groupMap.put(sg.groupValue, sg);

        if (groupMap.size() == topNGroups) {
          // End of startup transient: we now have max
          // number of groups; from here on we will drop
          // bottom group when we insert new one:
          buildSortedSet();
        }

        return;
      }
      // We already tested that the document is competitive, so replace
      // the bottom group with this new group.
      final CollectedSearchGroup2<SUBGROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE> bottomGroup = orderedGroups.pollLast();
      assert orderedGroups.size() == topNGroups -1;

      groupMap.remove(bottomGroup.groupValue);

      // reuse the removed CollectedSearchGroup
      bottomGroup.groupValue = copyDocGroupValue(groupValue, bottomGroup.groupValue);
      bottomGroup.topDoc = docBase + doc;

      for (LeafFieldComparator fc : leafComparators) {
        fc.copy(bottomGroup.comparatorSlot, doc);
      }

      groupMap.put(bottomGroup.groupValue, bottomGroup);
      orderedGroups.add(bottomGroup);
      assert orderedGroups.size() == topNGroups;

      final int lastComparatorSlot = orderedGroups.last().comparatorSlot;
      for (LeafFieldComparator fc : leafComparators) {
        fc.setBottom(lastComparatorSlot);
      }

      return;
    }

    // Update existing group:
    for (int compIDX = 0;; compIDX++) {
      leafComparators[compIDX].copy(spareSlot, doc);

      final int c = reversed[compIDX] * comparators[compIDX].compare(group.comparatorSlot, spareSlot);
      if (c < 0) {
        // Definitely not competitive.
        return;
      } else if (c > 0) {
        // Definitely competitive; set remaining comparators:
        for (int compIDX2=compIDX+1; compIDX2<comparators.length; compIDX2++) {
          leafComparators[compIDX2].copy(spareSlot, doc);
        }
        break;
      } else if (compIDX == compIDXEnd) {
        // Here c=0. If we're at the last comparator, this doc is not
        // competitive, since docs are visited in doc Id order, which means
        // this doc cannot compete with any other document in the queue.
        return;
      }
    }

    // Remove before updating the group since lookup is done via comparators
    // TODO: optimize this

    final CollectedSearchGroup<SUBGROUP_VALUE_TYPE> prevLast;
    if (orderedGroups != null) {
      prevLast = orderedGroups.last();
      orderedGroups.remove(group);
      assert orderedGroups.size() == topNGroups-1;
    } else {
      prevLast = null;
    }

    group.topDoc = docBase + doc;

    // Swap slots
    final int tmp = spareSlot;
    spareSlot = group.comparatorSlot;
    group.comparatorSlot = tmp;

    // Re-add the changed group
    if (orderedGroups != null) {
      orderedGroups.add(group);
      assert orderedGroups.size() == topNGroups;
      final CollectedSearchGroup<?> newLast = orderedGroups.last();
      // If we changed the value of the last group, or changed which group was last, then update bottom:
      if (group == newLast || prevLast != newLast) {
        for (LeafFieldComparator fc : leafComparators) {
          fc.setBottom(newLast.comparatorSlot);
        }
      }
    }
  }

  private void buildSortedSet() {
    final Comparator<CollectedSearchGroup<?>> comparator = new Comparator<CollectedSearchGroup<?>>() {
      @Override
      public int compare(CollectedSearchGroup<?> o1, CollectedSearchGroup<?> o2) {
        for (int compIDX = 0;; compIDX++) {
          FieldComparator<?> fc = comparators[compIDX];
          final int c = reversed[compIDX] * fc.compare(o1.comparatorSlot, o2.comparatorSlot);
          if (c != 0) {
            return c;
          } else if (compIDX == compIDXEnd) {
            return o1.topDoc - o2.topDoc;
          }
        }
      }
    };

    orderedGroups = new TreeSet<>(comparator);
    orderedGroups.addAll(groupMap.values());
    assert orderedGroups.size() > 0;

    for (LeafFieldComparator fc : leafComparators) {
      fc.setBottom(orderedGroups.last().comparatorSlot);
    }
  }

  @Override
  protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
    docBase = readerContext.docBase;
    for(AbstractSecondPassGrouping2Collector<SUBGROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE> c : collectors.values()){
	    for (int i=0; i<c.comparators.length; i++) {
	      c.leafComparators[i] = c.comparators[i].getLeafComparator(readerContext);
	    }
    }
  }

  public Map<SUBGROUP_VALUE_TYPE, CollectedSearchGroup2<SUBGROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE>> getGroupMap(){
		return groupMap;
	}
  public void setGroupMap(Map<SUBGROUP_VALUE_TYPE, CollectedSearchGroup2<SUBGROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE>> map){
  	this.groupMap = map;
	}
  
  public Collection<CollectedSearchGroup2<GROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE>> getTopGroupsFirstPass(){
		return topGroupsFirstPass;
	}
  
  public AbstractSecondPassGrouping2Collector<SUBGROUP_VALUE_TYPE, SUBGROUP_VALUE_TYPE> getCollectors(GROUP_VALUE_TYPE ref){
		return collectors.get(ref);
	}
  
  public long getTotalHitCount(){
		return totalHitCount;
	}
  /**
   * Returns the group value for the specified doc.
   *
   * @param doc The specified doc
   * @return the group value for the specified doc
   */
  public abstract SUBGROUP_VALUE_TYPE getDocGroupValue(int doc);
  /**
   * Returns the parent group value for the specified doc.<p/>
   * If this is the collect for the first group then this will return null;
   *
   * @param doc The specified doc
   * @return the parent group value for the specified doc
   */
  public abstract GROUP_VALUE_TYPE getDocGroupParentValue(int doc);

  /**
   * Returns a copy of the specified group value by creating a new instance and copying the value from the specified
   * groupValue in the new instance. Or optionally the reuse argument can be used to copy the group value in.
   *
   * @param groupValue The group value to copy
   * @param reuse Optionally a reuse instance to prevent a new instance creation
   * @return a copy of the specified group value
   */
  protected abstract SUBGROUP_VALUE_TYPE copyDocGroupValue(SUBGROUP_VALUE_TYPE groupValue, SUBGROUP_VALUE_TYPE reuse);

}

