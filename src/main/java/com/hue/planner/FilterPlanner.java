package com.hue.planner;

import java.util.List;

import com.google.common.collect.Lists;
import com.vero.model.Expressible;
import com.vero.model.Expression;
import com.vero.model.Measure;
import com.vero.model.graph.GraphException;
import com.vero.model.graph.Path;
import com.vero.model.report.QueryBlock;
import com.vero.model.services.IPathService;
import com.vero.server.engine.OptimizedPlan;

public class FilterPlanner {
		
	public static void plan(QueryBlock qb,IPathService pathSvc) throws  PlannerException, GraphException{
		if(qb.getFilterMeasures() ==null || qb.getFilterMeasures().size()==0) return;

		if(qb.getEngineBlocks().size()==0){
			planSingleBlock(qb,pathSvc);
		}else{
			planMultiBlock(qb,pathSvc);
		}

	}

	private static void planMultiBlock(QueryBlock qb, IPathService pathSvc) throws PlannerException {
		qb.getBlockFilter().genMeasureFilters().forEach((sf) -> {
			FilterPlanGenerator pg = new FilterPlanGenerator(pathSvc);
			try {
				qb.getSetBlocks().add(pg.getSetBlock(qb,sf));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}

	private static void planSingleBlock(QueryBlock qb, IPathService pathSvc) throws  PlannerException, GraphException {

		if(qb.getBlockFilter().needsSpecialProcessing() ){
			planMultiBlock(qb,pathSvc);
			return;
		}

		OptimizedPlan pl = (OptimizedPlan) qb.getPlan();

		List<Expressible> targets = Lists.newArrayList();
		for(Measure m : qb.getFilterMeasures()){
			targets.add(m);
		}

		// Check if the filtered measure can reach the root table used
		// in this plan.  If its not found then we fall back to multiblock set filter.
		
			Expressible[] tda = targets.toArray(new Expressible[targets.size()]);


//			TraversalDescription td = gdb.traversalDescription()
//			 .uniqueness(Uniqueness.NODE_PATH)
//			 .relationships(RelTypes.HAS_EXPRESSION, Direction.OUTGOING)
//			 .relationships(RelTypes.QUERIES_FROM, Direction.OUTGOING)
//			 .relationships(RelTypes.JOINS,Direction.INCOMING)
//			 .evaluator(PathEvaluators.endsWith(pl.getRootTable().getVeroObj()));

			List<Path> fpaths = pathSvc.getBasePathsToTargetRootTable(pl.getRootTable(), tda);
			for(Path p : fpaths){
				pl.addPath(pl.getFactory().createVeroPath(p));
				targets.remove(p.startNode());
			}

		if(targets.size()>0){
			// It is possible that the selections and filters
			// can be resolved in a more expensive table with an overall
			// reduced plan cost.  Here we create a sudo QueryBlock to
			// see if we can generate a more efficient single plan.
			QueryBlock qbtemp = new QueryBlock();
			qbtemp.setBlockFilter(qb.getBlockFilter().duplicateFilter());
			qbtemp.setSelections(Lists.newArrayList(qb.getSelections()));
			qbtemp.getSelections().addAll(qb.getFilterMeasures());
			PlanGenerator pg = new PlanGenerator(pathSvc);
			List<OptimizedPlan> ops = pg.getPlans(qbtemp);

			if(ops.size()==1){
				// if windowing then move to multiBlock
				if(possibleWindowFilter(qb, ops.get(0))){
					planMultiBlock(qb,pathSvc);
					return;
				}

				List<Measure> sm = Lists.newArrayList(qb.getSelectMeasures());
				qb.setPlan(ops.get(0));
				qb.setSelectMeasures(sm);
			}else{
				planMultiBlock(qb,pathSvc);
			}
		}
	}

	private static boolean possibleWindowFilter(QueryBlock qb, OptimizedPlan plan) throws PlannerException {
		for(Measure m : qb.getBlockFilter().getMeasures()){
			Expression e = plan.getExpression(m);
			if(e.getFormula().toLowerCase().contains("rank") ||
				e.getFormula().toLowerCase().contains("over(")){
				return true;
			}
		}
		return false;
	}
}
