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
package org.apache.solr.search.grouping.distributed.command;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.lucene.search.grouping.CollectedSearchGroup2;
import org.apache.lucene.search.grouping.Group2Docs;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.search.grouping.TopGroups2;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueDate;
import org.apache.lucene.util.mutable.MutableValueDouble;
import org.apache.lucene.util.mutable.MutableValueFloat;
import org.apache.lucene.util.mutable.MutableValueInt;
import org.apache.lucene.util.mutable.MutableValueLong;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.util.DateFormatUtil;

/** 
 * this is a transition class: for numeric types we use function-based distributed grouping,
 * otherwise term-based. so for now we internally use function-based but pretend like we did 
 * it all with bytes, to not change any wire serialization etc.
 */
public class Group2Converter {
  
  public static Collection<SearchGroup<BytesRef>> fromMutable(SchemaField field, SchemaField subField, Collection<SearchGroup<?>> values) {
    if (values == null) {
      return null;
    }
    List<SearchGroup<BytesRef>> result = new ArrayList<>(values.size());
    for (SearchGroup<?> original : values) {
      CollectedSearchGroup2<BytesRef, BytesRef> converted = new CollectedSearchGroup2<>();
      converted.sortValues = original.sortValues;
      converted.groupValue = convertToBytesRef(field, original.groupValue);
      if(original instanceof CollectedSearchGroup2){
      	CollectedSearchGroup2<?, ?> org = (CollectedSearchGroup2<?,?>)original;
      	converted.groupCount = org.groupCount;
      	// process subGroups
      	converted.subGroups = new ArrayList<>();
      	for(SearchGroup<?> sub : org.subGroups){
      		SearchGroup<BytesRef> subConverted = new SearchGroup<>();
          subConverted.sortValues = sub.sortValues;
      		subConverted.groupValue = convertToBytesRef(subField, sub.groupValue); 
      		converted.subGroups.add(subConverted);
      	}
      }
      result.add(converted);
    }
    return result;
  }
  
  public static BytesRef convertToBytesRef(SchemaField field, Object val){
  	if(val instanceof BytesRef){
  		return (BytesRef)val;
  	}
  	if(val instanceof MutableValue){
  		MutableValue mv = (MutableValue)val;
      if (mv.exists) {
        BytesRefBuilder binary = new BytesRefBuilder();
        String s;
        if(mv instanceof MutableValueDate){
        	s = DateFormatUtil.formatExternal((Date)mv.toObject());
        }
        else{
        	s = mv.toString();
        }
        field.getType().readableToIndexed(s, binary);
        return binary.get();
      } else {
        return null;
      }
  	}
  	return null;
  }
  
  public static MutableValue convertToMutableValue(SchemaField field, Object val){
    FieldType fieldType = field.getType();
    TrieField.TrieTypes type = ((TrieField)fieldType).getType();
  	if(val == null){
  		switch (type){
			case INTEGER:
				MutableValueInt mvi = new MutableValueInt();
	  		mvi.value = 0;
	  		mvi.exists = false;
	  		return mvi;
			case LONG:
				MutableValueLong mvl = new MutableValueLong();
	  		mvl.value = 0;
	  		mvl.exists = false;
	  		return mvl;
			case DATE:
				MutableValueDate mvda = new MutableValueDate();
	  		mvda.value = 0;
	  		mvda.exists = false;
	  		return mvda;
			case DOUBLE:
				MutableValueDouble mvd = new MutableValueDouble();
	  		mvd.value = 0;
	  		mvd.exists = false;
	  		return mvd;
			case FLOAT:
				MutableValueFloat mvf = new MutableValueFloat();
	  		mvf.value = 0;
	  		mvf.exists = false;
	  		return mvf;

			default:
				return null;
			}
  	}
  	
  	if(val instanceof MutableValue){
  		return(MutableValue)val;
  	}
  	if(val instanceof BytesRef){
  		BytesRef br = (BytesRef)val;
	    final MutableValue v;
	    switch (type) {
	      case INTEGER:
	        MutableValueInt mutableInt = new MutableValueInt();
          mutableInt.value = (Integer) fieldType.toObject(field, br);
	        v = mutableInt;
	        break;
	      case FLOAT:
	        MutableValueFloat mutableFloat = new MutableValueFloat();
	        mutableFloat.value = (Float) fieldType.toObject(field, br);
	        v = mutableFloat;
	        break;
	      case DOUBLE:
	        MutableValueDouble mutableDouble = new MutableValueDouble();
	        mutableDouble.value = (Double) fieldType.toObject(field, br);
	        v = mutableDouble;
	        break;
	      case LONG:
	        MutableValueLong mutableLong = new MutableValueLong();
	        mutableLong.value = (Long) fieldType.toObject(field, br);
	        v = mutableLong;
	        break;
	      case DATE:
	        MutableValueDate mutableDate = new MutableValueDate();
	        mutableDate.value = ((Date)fieldType.toObject(field, br)).getTime();
	        v = mutableDate;
	        break;
	      default:
	        throw new AssertionError();
	    }
	    return v;
    }

  	return null;
  }
  
  public static Collection<SearchGroup<MutableValue>> toMutable(SchemaField field,
  		Collection<SearchGroup<BytesRef>> values) {
  	return GroupConverter.toMutable(field, values);
  }  		
  
  public static Collection<CollectedSearchGroup2<MutableValue, MutableValue>> toMutable(SchemaField field, 
  		SchemaField subField, Collection<CollectedSearchGroup2<BytesRef, BytesRef>> values) {
    List<CollectedSearchGroup2<MutableValue, MutableValue>> result = new ArrayList<>(values.size());
    for (CollectedSearchGroup2<?,?> original : values) {
      CollectedSearchGroup2<MutableValue, MutableValue> converted = new CollectedSearchGroup2<>();
      converted.sortValues = original.sortValues; 
      converted.groupValue = convertToMutableValue(field, original.groupValue);
      converted.groupCount = original.groupCount;
      converted.subGroups = new ArrayList<>();
      for(SearchGroup<?> subOriginal : original.subGroups){
      	SearchGroup<MutableValue> subConverted = new SearchGroup<>();
      	subConverted.sortValues = subOriginal.sortValues;
      	subConverted.groupValue = convertToMutableValue(subField, subOriginal.groupValue);
      	converted.subGroups.add(subConverted);
      }
      result.add(converted);
    }
    return result;
  }
  public static Collection<CollectedSearchGroup2> toCollectorTypes(SchemaField field, 
  		SchemaField subField, Collection<CollectedSearchGroup2<BytesRef, BytesRef>> values) {
    List<CollectedSearchGroup2> result = new ArrayList<>(values.size());
    for (CollectedSearchGroup2<?,?> original : values) {
      CollectedSearchGroup2 converted = new CollectedSearchGroup2();
      converted.sortValues = original.sortValues; 
      if(field.getType().getNumericType() != null){
      	converted.groupValue = convertToMutableValue(field, original.groupValue);
      }
      else{
      	converted.groupValue = original.groupValue;
      }
      converted.groupCount = original.groupCount;
      converted.subGroups = new ArrayList<>();
      for(SearchGroup<?> subOriginal : original.subGroups){
      	SearchGroup subConverted = new SearchGroup<>();
      	subConverted.sortValues = subOriginal.sortValues;
        if(subField.getType().getNumericType() != null){
      	subConverted.groupValue = convertToMutableValue(subField, subOriginal.groupValue);
        }
        else{
        	subConverted.groupValue = subOriginal.groupValue;
        }
      	converted.subGroups.add(subConverted);
      }
      result.add(converted);
    }
    return result;
  }
  
  public static TopGroups2<BytesRef, BytesRef> fromMutable(SchemaField field, SchemaField subField,
  		TopGroups2<MutableValue, MutableValue> values) {
    if (values == null) {
      return null;
    }
    
    FieldType fieldType = field.getType();
    
    @SuppressWarnings("unchecked")
    Group2Docs<BytesRef, BytesRef> groupDocs[] = new Group2Docs[values.groups.length];
    
    for (int i = 0; i < values.groups.length; i++) {
      Group2Docs<MutableValue, MutableValue> original = values.groups[i];
      final BytesRef groupValue = convertToBytesRef(subField, original.groupValue);
      final BytesRef groupParentValue = convertToBytesRef(field, original.groupParentValue);
      groupDocs[i] = new Group2Docs<BytesRef, BytesRef>(original.score, original.maxScore, original.totalHits, original.scoreDocs, groupParentValue, groupValue, original.groupSortValues);
    }
    
    return new TopGroups2<BytesRef, BytesRef>(values.groupSort, values.withinGroupSort, values.totalHitCount, values.totalGroupedHitCount, groupDocs, values.maxScore);
  }
}
