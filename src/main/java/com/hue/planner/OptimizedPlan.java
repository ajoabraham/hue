package com.hue.planner;


import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hue.graph.Path;
import com.hue.model.Dimension;
import com.hue.model.Expressible;
import com.hue.model.FieldExpression;
import com.hue.model.IJoinable;
import com.hue.model.Measure;
import com.hue.model.Table;

public class OptimizedPlan{
	private static final Logger logger = LoggerFactory.getLogger(OptimizedPlan.class.getName());

	private double planCost=0;
	private HashSet<Dimension> dimensions;
	private HashSet<Measure> measures;
	HashMap<Expressible, IJoinable> expressibleJoinableMapping = Maps.newHashMap();
	HashMap<Expressible, FieldExpression> expressibleExpressionMapping = Maps.newHashMap();
	private JoinTree joinTree;
	private List<DisjointPlan> disjointedPlans = Lists.newArrayList();
	private boolean useDisjointedPlans = false;
	private List<PlanPath> paths = Lists.newArrayList();
	private List<Measure> measureNodes;
	private List<Expressible> allNodes;

	private List<String> planMessages = Lists.newArrayList();

	public OptimizedPlan(){	}

	public boolean isEmpty(){
		return paths.isEmpty();
	}

	public List<PlanPath> getPaths(){
		return paths;
	}

	public List<String> getPlanMessages(){
		return planMessages;
	}
	
	public Table getRootTable(){
		if(paths.size()>0)
			return paths.get(0).getRootTable();

		return null;
	}

	public void setPlanCost(double d){
		this.planCost = d;
	}

	public double getPlanCost(){
		return this.planCost;
	}

	public JoinTree getJoinTree() throws PlannerException{

		if (joinTree != null) return joinTree;

		joinTree = new JoinTree(getPlanPaths());
		logger.debug("main plan paths");
		logger.debug(getPlanPaths().toString());
		if(useDisjointedPlans()){
			for(DisjointPlan pl : disjointedPlans){
				try {
					joinTree.addCrossJoin(pl.getJoinTree());
					logger.debug("cross join plan paths");
					logger.debug(joinTree.toString());
				} catch (PlannerException e) {
					throw new PlannerException(e);
				}
			}
		}

		return joinTree;
	}

	/**
	 * Return neo4j paths from the current plan. Does not included paths
	 * from disjointed paths.
	 *
	 * @return
	 */
	public Set<Path> getPlanPaths() {
		Set<Path> ppaths = Sets.newLinkedHashSet();
		Collections.sort(paths);
		for(PlanPath vp : paths){
			ppaths.add(vp.getPath());
		}
		return ppaths;
	}

	public void addPath(PlanPath path) {
		paths.add(path);
	}

	public void addPaths(Set<PlanPath> veroPath) {
		Iterables.addAll(paths,veroPath);
	}

	/**
	 * Every virtualTree is tested against every plan to see if there is a
	 * portion of the tree that can be resolved by the tree.
	 *
	 * We first check if any part of the provided tree can be consumed
	 * by this plan.  If it is consumable then we need to check if any
	 * virtual expression can completely be consumed by the current plan.
	 * This can happen if you have a virtual expression that can be resolved
	 * by a single table.
	 *
	 * @param vt - a VirtualTree to be tested for add
	 * @throws PlannerException
	 */
//	public void append(VirtualTree vt) throws PlannerException{
//		boolean addable = false;
//		ConsumptionCompleteVisitor completeVisitor = new ConsumptionCompleteVisitor(this);
//		for(Expressible consumable : getVeroNodes()){
//			VirtualNode vn = (VirtualNode) vt.find(consumable);
//			if(vn != null && !vn.isConsumed() && !vt.getRoot().equals(vn)){
//				addable = true;
//				vn.consume(this);
//			}
//		}
//		if(addable) {
//			virtualTrees.add(vt);
//			for(VirtualExpNode ve : vt.getVirtualExpNodes()){
//				completeVisitor.restart();
//				completeVisitor.visit(ve);
//				if(completeVisitor.isComplete())
//					ve.getParent().consume(this);
//			}
//		}
//	}

	public HashSet<Dimension> getPlanDimensions(){
		if(dimensions != null) return dimensions;

		dimensions = Sets.newHashSet();		
		for(PlanPath p : paths){
			if(p.getExpressible() instanceof Dimension){
				dimensions.add((Dimension)p.getExpressible());
			}
		}
		
		return dimensions;
	}

	public HashSet<Measure> getPlanMeasures(){
		if(measures != null) return measures;

		measures = Sets.newHashSet();
	
		for(PlanPath p : paths){
			if(p.getExpressible() instanceof Measure){
				measures.add((Measure)p.getExpressible());
			}
		}

//		for(VirtualTree vt : virtualTrees){
//			ConsumedDescendantVisitor cdVisitor = new ConsumedDescendantVisitor(this);
//			vt.getRoot().accept(cdVisitor);
//			for(VirtualNode ve : cdVisitor.getConsumedNodes()){
//				// TODO fix virtual
////				measures.add(Measure.find((String)ve.getVeroNode().getProperty("id")));
//			}
//		}
		return measures;
	}

	// filter virual vs

//	public HashSet<Measure> getPlanSelectMeasures(){
//		if(selectMeasures != null) return selectMeasures;
//
//		selectMeasures = Sets.newHashSet(getPlanMeasures());
//		selectMeasures.retainAll(getSourceBlock().getSelectMeasures());
//
//		if(virtualTrees.size()==0) return selectMeasures;
//
//		Set<Measure> selm = getSourceBlock().getAllMeasures();
//
//		try{
//			EarliestConsumedDescendantVisitor ecdVisitor = new EarliestConsumedDescendantVisitor(this);
//			for(Measure sel : selm){
//				for(VirtualTree vt : virtualTrees){
//					VirtualNode found = (VirtualNode) vt.find(new VirtualNode(sel));
//
//					if(found==null) continue;
//
//					ecdVisitor.visit(found);
//					for(VirtualNode vf : ecdVisitor.getConsumedNodes()){
//						selectMeasures.add((Measure)vf.getVeroObj());
//					}
//				}
//			}
//		}catch( PlannerException e){
//			// FIXME  Need to refactor all of this error handling in QueryPlans
//			logger.error("Severe issue with processing virtual select measures.\n" +e.getMessage());
//		}
//
//		return selectMeasures;
//	}



	public FieldExpression getExpression(Expressible e) throws PlannerException{
		FieldExpression ee = expressibleExpressionMapping.get(e);
		if(ee == null) getJoinable(e);

		return expressibleExpressionMapping.get(e);
	}

	public IJoinable getJoinable(Expressible e) throws PlannerException{
		String expressibleName = "";
		IJoinable t = expressibleJoinableMapping.get(e);
		if(t != null) return t;

		try
		{
			Optional<PlanPath> res = paths.stream().filter((vp) ->
				vp.getExpressible().equals(e)
			 ).findFirst();

			// if no match was found and we are using disjoints then
			// find it in the disjoint collection
			FieldExpression ee = null;
			if(res.isPresent()){
				t = res.get().getSelectTable();
				ee = res.get().getExpression();

				if ( ee ==null ){
					throw new PlannerException("Couldnt not find expression in the database: "+ res.get().getExpression());
				}
			
			}else if(useDisjointedPlans()){
				for(DisjointPlan p : disjointedPlans){
					try{
						t = (Table) p.getJoinable(e);
						ee = p.getExpression(e);
						if(t!=null && ee!=null) break;
					}catch(PlannerException eop){
						continue;
					}
				}
			}else{
				throw new PlannerException("No joinable could be discovered.");
			}


			expressibleJoinableMapping.put(e,t);
			expressibleExpressionMapping.put(e, ee);

		} catch (Exception e1) {
			throw new PlannerException("Unable to find joinable/table for " + expressibleName,e1);
		}
		return t;
	}

	public List<DisjointPlan> getDisjointedPlans(){
		return disjointedPlans;
	}

	public void addDisjointedPlan(DisjointPlan dp) {
		this.disjointedPlans.add(dp);
	}

	public void addDisjointedPlans(List<DisjointPlan> dps) {
		this.disjointedPlans.addAll(dps);
	}

	/**
	 * Can only be set to true if there are are disjointed plans
	 * included in the collection.  You must first add disjointed plans
	 * before setting this to true.
	 *
	 * @param val
	 */
	public void setUseDisjointedPlans(boolean val){
		if(disjointedPlans.size()>0)
			this.useDisjointedPlans  = val;
	}

	/**
	 * The use of disjointed paths can only be determined in the
	 * context of all engineBlocks generated while processing a
	 * dataset.  Sometimes there is a potential to use disjoints
	 * but it is not required since some other engine block satisfies
	 * the input.
	 *
	 * @return
	 */
	public boolean useDisjointedPlans(){
		return this.useDisjointedPlans;
	}

	public void setAllDisjointsAsCrossJoin() {
		disjointedPlans.stream().forEach((dj) -> { dj.setAllDimensionsAsCrossJoin(); });
	}

}
