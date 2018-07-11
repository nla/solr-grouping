package org.apache.solr.handler.component;

import java.io.IOException;

import org.apache.solr.common.params.GroupParams;

/**
 * Two level grouping has extra stage added so we modify faceting to only work
 * on the first pass by fiddling some values to use the standard component.
 * 
 * @author icaldwell
 *
 */
public class FacetComponentGrouping2 extends FacetComponent{

	@Override
	public void prepare(ResponseBuilder rb) throws IOException{
		// for grouping we don't set doc set needed except on first pass(set in query component)
		boolean need = rb.isNeedDocSet();
		super.prepare(rb);
		rb.setNeedDocSet(need);
	}
	
	@Override
	public int distributedProcess(ResponseBuilder rb) throws IOException{
		if(rb.stage == ResponseBuilder.STAGE_GET_FIELDS){
      return ResponseBuilder.STAGE_DONE;
		}
		
		int ret = 0;
		int holdStage = -1;
    if (rb.stage == ResponseBuilder.STAGE_TOP_GROUPS){
    	holdStage = rb.stage;
    	rb.stage = ResponseBuilder.STAGE_GET_FIELDS;
    }
		ret = super.distributedProcess(rb);
		if(holdStage != -1){
			 rb.stage = holdStage;
		}
		return ret;
	}
	
	@Override
	public void modifyRequest(ResponseBuilder rb, SearchComponent who,
			ShardRequest sreq){
    if (!rb.doFacets) return;

    if (sreq.params.getBool(GroupParams.GROUP_DISTRIBUTED_FIRST, false)){
      sreq.purpose |= ShardRequest.PURPOSE_GET_FACETS;
      int sreqPurpose = sreq.purpose;
      sreq.purpose = ShardRequest.PURPOSE_GET_TOP_IDS;

      super.modifyRequest(rb, who, sreq);
      
      sreq.purpose = sreqPurpose;
    }
	}
	
	@Override
	public void handleResponses(ResponseBuilder rb, ShardRequest sreq){
    if (!rb.doFacets) return;
    
    if (sreq.params.getBool(GroupParams.GROUP_DISTRIBUTED_FIRST, false)){
    	int purpose = sreq.purpose;
    	sreq.purpose |= ShardRequest.PURPOSE_GET_FACETS;
    	super.handleResponses(rb, sreq);
    	sreq.purpose = purpose;
    }
	}

	@Override
	public void process(ResponseBuilder rb) throws IOException{
		if(rb.getResults() == null || !rb.isNeedDocSet()){
			return;
		}
		super.process(rb);
	}
	
	@Override
	public void finishStage(ResponseBuilder rb){
    if (!rb.doFacets) return;
    if (rb.stage == ResponseBuilder.STAGE_TOP_GROUPS){
    	int stage = rb.stage;
    	rb.stage = ResponseBuilder.STAGE_GET_FIELDS;
    	super.finishStage(rb);
    	rb.stage = stage;
    }
	}
	
	@Override
	public String getDescription(){
		return super.getDescription() + "Grouping2";
	}
}
