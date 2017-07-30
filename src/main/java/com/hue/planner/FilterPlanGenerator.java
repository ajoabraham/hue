package com.hue.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vero.model.services.IPathService;
import com.vero.common.sql.parser.nodeextractor.EnhancedNodeExtractor;
import com.vero.common.sql.parser.nodeextractor.ExtractResult;
import com.vero.server.engine.MultiBlockPlan;
import com.vero.server.engine.OptimizedPlan;
import com.vero.model.Dimension;
import com.vero.model.Expressible;
import com.vero.model.Expression;
import com.vero.model.ExpressionRef;
import com.vero.model.Measure;
import com.vero.model.VeroBase;
import com.vero.server.model.common.PersistenceException;
import com.vero.model.report.BlockFilter;
import com.vero.model.report.EngineBlock;
import com.vero.model.report.QueryBlock;
import com.vero.model.report.SetBlock;

public class FilterPlanGenerator extends PlanGenerator {
	private static final Logger logger = LoggerFactory.getLogger(FilterPlanGenerator.class.getName());
	private BlockFilter blockFilter;


	public FilterPlanGenerator(IPathService pathSvc) {
		super(pathSvc);
	}
	
	public SetBlock getSetBlock(QueryBlock sourceBlock, BlockFilter sf) throws PlannerException, PersistenceException {
		this.blockFilter = sf;
		this.sourceBlock = sourceBlock;
		this.virtualPu = veroFactory.createVirtualProcessor();

		selections = Lists.newArrayList();
		for(Dimension d : sourceBlock.getSelectDimensions()){
			selections.add(d);
		}

		for(Dimension d : sf.getDimensions()){
			selections.add(d);
		}

		for(Measure m : sf.getMeasures()){
			selections.add(m);
		}

		List<OptimizedPlan> ops = process();
		SetBlock sb = new SetBlock(sourceBlock,null);

		if(ops.size()==1) {
//			ops.get(0).setUseDisjointedPlans(true);
//			ops.get(0).setAllDisjointsAsCrossJoin();

			OptimizedPlan plan = ops.get(0);
			logger.debug(ops.get(0).getPlanPaths().toString());

			// check if any measures are windowed
			List<Expressible> windowed = getWindowedMeasuresInFilter(plan);

			// if there are windowed functions then we need to
			// add an engineblock to first calculate the window
			// then in the setblock apply the filter on the window
			// in the where clause
			if(windowed.size()>0 ){
				logger.debug("Filter is on a windowed measure in an optimized plan. Creating pre-engine block.");
				EngineBlock eb = new EngineBlock(sourceBlock, plan);
				// clear all measures first
				List<Expressible> allsels = Lists.newArrayList(eb.getSelections());
				for(Expressible e : eb.getSelections()){
					if(e instanceof Measure){
						allsels.remove(e);
					}
				}
//				TODO: not sure this should be here, just commented it out to test
//				allsels.addAll(plan.getPlanMeasures());
				allsels.addAll(windowed);
				eb.setSelections(allsels);

				BlockFilter prefilter = sf.duplicateFilter();
				windowed.stream().forEach(w -> {
					prefilter.removeNode(w.getName(), w.getClass());
				});
				eb.setBlockFilter(prefilter);
				eb.setParent(sb);

				BlockFilter postfilter = sf.duplicateFilter();
				List<String> mids = windowed.stream().map(Expressible::getName).collect(Collectors.toList());
				// keep only window filters now
				Set<String> removeNodes = Sets.newHashSet();
				postfilter.getExpressionNodes().stream().forEach(ef -> {
					int currWindows = 0;

					for(ExpressionRef<? extends VeroBase> ep : ef.getReferences()){
						if(mids.contains(ep.getReference().getName())){
							currWindows ++;
						}
					}
					if(currWindows==0){
						removeNodes.add(ef.getReferences().get(0).getReference().getName());
					}
				});
				removeNodes.forEach(r -> postfilter.removeNode(r,Measure.class));

				sb.setBlockFilter(postfilter);
				sb.getChildren().add(eb);
				MultiBlockPlan mb = new MultiBlockPlan(sourceBlock, sb.getChildren().toArray(new EngineBlock[sb.getChildren().size()]));
				sb.setPlan(mb);
			}else{
				sb.setPlan(plan);
				sb.setBlockFilter(sf);
			}
		}else{
			for(OptimizedPlan op : ops){
				List<Expressible> windows = getWindowedMeasuresInFilter(op);
				EngineBlock eb = new EngineBlock(sourceBlock, op);

				eb.getSelections().addAll(windows);
				eb.setParent(sb);

				if(windows.size()>0){
					BlockFilter f = sf.duplicateFilter();
					windows.forEach(w -> f.removeNode(w.getName(),w.getClass()));
					eb.setBlockFilter(f);
				}else{
					eb.setBlockFilter(sf.duplicateFilter());
				}

				sb.getChildren().add(eb);
				logger.debug("Set Block Child (Engine Block) =>\n"+eb.toString());
			}
			MultiBlockPlan mb = new MultiBlockPlan(sourceBlock, sb.getChildren().toArray(new EngineBlock[sb.getChildren().size()]));

			// check if this mb plan has virtual resolutions
			// that are widowed and filtered
			List<Measure> windowed = getWindowedMeasuresInFilter(mb);
			if(windowed.size()>0){
				logger.debug("Creating engineblock to process windowed virtuals before setblock processing...");

				EngineBlock eb = new EngineBlock(sourceBlock, mb);
				ArrayList<Expressible> selList = Lists.newArrayList(eb.getSelectMeasures());
//				selList.addAll(windowed);
				windowed.stream().forEach( w -> {
					if(!selList.contains(w)) eb.getSelections().add(w);
				});
//				selList.addAll(eb.getSelectDimensions());
//				eb.setSelections(selList);

				BlockFilter prefilter = sf.duplicateFilter();
				windowed.stream().forEach(w -> {
					prefilter.removeNode(w.getName(),w.getClass());
				});
				eb.setBlockFilter(prefilter);
				eb.setChildren(Lists.newArrayList(sb.getChildren()));
				sb.getChildren().clear();
				eb.setParent(sb);

				BlockFilter postfilter = sf.duplicateFilter();
				List<String> mids = windowed.stream().map(Measure::getName).collect(Collectors.toList());
				// keep only window filters now
				Set<String> removeNodes = Sets.newHashSet();
				postfilter.getExpressionNodes().stream().forEach(ef -> {
					int currWindows = 0;

					for(ExpressionRef<? extends VeroBase> ep : ef.getReferences()){
						if(mids.contains(ep.getReference().getName())){
							currWindows ++;
						}
					}
					if(currWindows==0){
						removeNodes.add(ef.getReferences().get(0).getReference().getName());
					}
				});
				removeNodes.forEach(r -> postfilter.removeNode(r,Measure.class));

				sb.setBlockFilter(postfilter);
				sb.getChildren().add(eb);
				MultiBlockPlan m2 = new MultiBlockPlan(sourceBlock, sb.getChildren().toArray(new EngineBlock[sb.getChildren().size()]));
				sb.setPlan(m2);
			}else{
				sb.setPlan(mb);
				logger.debug("Set block with children: => \n" +sb.toString());
				sb.setBlockFilter(sf);
			}
		}

		return sb;
	}

	private List<Measure> getWindowedMeasuresInFilter(MultiBlockPlan mb) throws PersistenceException {
		return blockFilter.getMeasures().stream().filter(m -> {
			try {
				return mb.getPlanVirtualMeasures().contains(m) &&
							isWindowFunction(mb.getExpression(m));
			} catch (Exception e) {
				logger.error(e.getMessage());
				return false;
			}
		} ).collect(Collectors.toList());
	}

	public boolean isWindowFunction(Expression e) {
	    ExtractResult extractResult = EnhancedNodeExtractor.extract(e.getFormula());
		return extractResult.isWindowFunction();
	}

	@Override
	public boolean hasMeasures() throws PlannerException {
		
			return sourceBlock.getSelectMeasures().size()
					+ blockFilter.getMeasures().size() > 0;
		

	}

	private List<Expressible> getWindowedMeasuresInFilter(OptimizedPlan plan) throws PersistenceException{
		return blockFilter.getMeasures().stream().filter(m -> {
			try {
				return isWindowFunction(plan.getExpression(m));
			} catch (Exception e) {
				logger.error(e.getMessage());
				return false;
			}
		} ).collect(Collectors.toList());
	}
}
