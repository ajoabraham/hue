package com.hue.planner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.hue.graph.GraphException;
import com.hue.graph.Path;
import com.hue.model.Datasource;
import com.hue.model.Dimension;
import com.hue.model.Expressible;
import com.hue.model.IJoinable;
import com.hue.model.Measure;
import com.hue.model.Table;


public class PlanGenerator {
	private static final Logger logger = LoggerFactory.getLogger(PlanGenerator.class.getName());

	protected List<PlanPath> paths;
	private CostMatrix cm;
	protected List<Expressible> selections;
	protected List<Expressible> filters;

	private IPathService pathSvc;

	private List<Table> hints;

	private Datasource ds;

	public PlanGenerator(IPathService pathSvc){ 
		this.pathSvc = pathSvc;
	}

	public List<OptimizedPlan> getPlans(Datasource ds, List<Expressible> selects,List<Expressible> filters, List<Table> hints) throws PlannerException{
		this.ds = ds;
		this.selections = selects;
		this.filters = filters;
		this.hints = hints;

		return process();
	}

	protected List<OptimizedPlan> process() throws PlannerException {
		try{
			List<Expressible> allExp = Lists.newArrayList();
			allExp.addAll(selections);
			allExp.addAll(filters);
			
			Expressible[] tgtArray = allExp.toArray(new Expressible[allExp.size()]);
			

			for(Path p : pathSvc.getBasePaths(tgtArray))
			{
				paths.add(new PlanPath(p));
			}

			cm = new CostMatrix(paths);
			cm.setHints(hints);
			logger.debug(cm.toString());

			if(hasDimensions() && !hasMeasures()){
				return getPlansForDimensions();
			}else{
				return getGeneralPlans();
			}
		}catch(Exception e){
			logger.error("Unable to process paths.", e);
			throw new PlannerException(e);
		}
	}

	private List<OptimizedPlan> getGeneralPlans() throws PlannerException {
		HashBasedTable<Table, Expressible, PlanPath> matrix = cm.getMatrix();
		ArrayList<Table> sortKeys = cm.getSortedKeys();

		// sort for cost then prune for virtuals
		cm.applyDefaultSort();
		logger.debug("Default sorted cost matrix at start of generating general plans: \n"+cm.toString());

		List<OptimizedPlan> pls = Lists.newArrayList();

		List<Expressible> measures = matrix.columnKeySet().stream().filter((vn) -> {
			return (vn instanceof Measure);
			
		}).collect(Collectors.toList());

		List<Expressible> dims = matrix.columnKeySet().stream().filter((vn) -> {
			return (vn instanceof Dimension);
		}).collect(Collectors.toList());

		int i=0;
		Map<Table, Map<Expressible, PlanPath>> map = matrix.rowMap();
		while(!measures.isEmpty()){
			if(i>sortKeys.size()-1)
				throw new PlannerException("Unresolvable measures: " +measures);

			Table row = Iterables.get(sortKeys,i);
			//skip if row has no measures.
			if(skipRow(row)){
				i++;
				continue;
			}

			OptimizedPlan op = new OptimizedPlan();
			int count = 0;
			double cost = 0;
			ArrayList<Expressible> dimStack = Lists.newArrayList(dims);

			// sometimes muliple roots can support a measure
			// if the measure is already resolved then we
			// dont need to add anymore plans that can support the same measure.
			boolean addPlan = false;
			for(Entry<Expressible, PlanPath> v : map.get(row).entrySet()){
				// set addPlan to true if we actually are able to
				// take a measure of the stack.
				if(measures.remove(v.getKey())) addPlan = true;
				dimStack.remove(v.getKey());
				op.addPath(v.getValue());
				cost += v.getValue().getCost();
				count++;
			}
			if(!addPlan){
				i++;
				continue;
			}

			op.setPlanCost(cost/count);
			op.addDisjointedPlans(getDisjoinPlans(dimStack,op));
			//cm.applyDefaultSort();
			pls.add(op);
			i++;
		}

		return processForCostOptimization(pls);
	}

	private boolean skipRow(Table row) throws PlannerException {
		boolean skip = true;
		for(PlanPath vp : cm.getMatrix().row(row).values()){
			if(vp.getExpressible() instanceof Measure)
				skip = false;
		};
		return skip;
	}


	private List<OptimizedPlan> processForCostOptimization(
			List<OptimizedPlan> pls) throws PlannerException {

		ArrayList<OptimizedPlan> pruned = Lists.newArrayList(pls);

		pls.sort((p1,p2) -> {
			return Double.compare(p1.getPlanCost(), p2.getPlanCost());
		});

		for(OptimizedPlan pl : pls){
			for(OptimizedPlan ipl : pls){
				if(pl==ipl || pl.getMeasureNodes().size()==0)
					continue;

				if(ipl.getMeasureNodes().size()>pl.getMeasureNodes().size()
						&& ipl.getMeasureNodes().containsAll(pl.getMeasureNodes())){
					pruned.remove(pl);
				}
			}
		}

		return pruned;
	}

	private List<OptimizedPlan> getPlansForDimensions() {
		cm.sortForDimensions();
		HashBasedTable<Table, Expressible, PlanPath> matrix = cm.getMatrix();
		ArrayList<Table> sk = cm.getSortedKeys();
		logger.debug(cm.toString());

		List<Expressible> dims = matrix.columnKeySet().stream().filter((vn) -> {
			return (vn instanceof Dimension);
		}).collect(Collectors.toList());

		OptimizedPlan op = new OptimizedPlan();
		int count = 0;
		double cost = 0;
		for(Entry<Expressible, PlanPath> v : matrix.row(sk.get(0)).entrySet()){
			dims.remove(v.getKey());
			op.addPath(v.getValue());
			cost += v.getValue().getCost();
			count++;
		}
		op.setPlanCost(cost/count);

		op.addDisjointedPlans(getDisjoinPlans(dims,op));

		return Lists.newArrayList(op);
	}

	private List<DisjointPlan> getDisjoinPlans(List<Expressible> dimStack2,OptimizedPlan originalPlan) {
		trySecondaryPlanning(dimStack2, originalPlan);

		List<DisjointPlan> dops = Lists.newArrayList();
		if(dimStack2.size()==0)
			return dops;

		List<Expressible> dimStack = Lists.newArrayList(dimStack2);
		cm.sortForDimensions();
		for(Table row : cm.getSortedKeys()){
			DisjointPlan op = new DisjointPlan();
			if(dimStack.size()==0) break;
			int count=0;
			//double cost =0;
			for(Iterator<Expressible> it = dimStack.iterator();it.hasNext();){
				PlanPath col = cm.getMatrix().row(row).get(it.next());
				if(col != null){
					//dimStack.remove(col.getExpressible());
					it.remove();
					op.addPath(col);
					count++;
					//cost += col.getCost();
				}
			}
			if(count>0){
				//op.setPlanCost(cost/count);
				dops.add(op);
			}

		}
		// have to restore sort since
		// there may be more measures waiting to process
		cm.applyDefaultSort();
		return dops;
	}

	/**
	 * vb-355 .  Looking for valid alternative paths or
	 * subquery strategy for unresolved dims.
	 *
	 * @param dimStack2
	 * @param op
	 */
	private void trySecondaryPlanning(List<Expressible> dimStack2,
			OptimizedPlan op) {
		if(dimStack2.size()==0) return;

		logger.info("Attempting secondary planning for disjoint dimensions");

		// collect filter only dims
		List<Expressible> filterOnlyDims = dimStack2.stream().filter((d) -> {
			try {
				return !selections.contains(d);
			} catch (Exception e) {
				return false;
			}
		}).collect(Collectors.toList());
		trySubqueryPlanning(filterOnlyDims,dimStack2,op);

		ArrayList<Expressible> projDims = Lists.newArrayList(dimStack2);
		projDims.removeAll(filterOnlyDims);

		// get table nodes for tables with projections
		List<Table> targetTables = selections.stream().map((d) ->{
			Table j = null;
			try {
				j = (Table) op.getJoinable(d);
			} catch (Exception e) {
				logger.debug("coudnt get joinable for " + d + " likely disjoint. safe to ignore.");
			}
			return j;
		}).filter( (j) -> {return j instanceof Table; })
		.collect(Collectors.toList());

		List<Expressible> newDimTargets = Lists.newArrayList(projDims);

		// generate new paths
		List<PlanPath> secPlans = null;
		try {
			secPlans = generateSecondaryPlan(newDimTargets, targetTables);
		} catch (GraphException | PlannerException e1) {
			// return empty if planning failed
			return;
		}
		
		if(secPlans.isEmpty() || (PathUtils.hasFactTables(secPlans) && PathUtils.hasFactTables(paths)))
			return;

		CostMatrix cm2 = null;
		try {
			cm2 = new CostMatrix(secPlans);
			cm2.setHints(hints);
		} catch (PlannerException e) {
			logger.error("unable to create cost matrix in secondary planning.", e);
		}
		cm2.sortForDimensions();
		logger.debug("Secondary cost matrix: \n" + cm2);
		ArrayList<Table> sortKeys = cm2.getSortedKeys();
		HashSet<PlanPath> altPaths = Sets.newHashSet();
		List<Expressible> solvedDims = Lists.newArrayList();
		for(Table d : sortKeys){
			Map<Expressible, PlanPath> row = cm2.getMatrix().row(d);
			projDims.removeAll(row.keySet());
			dimStack2.removeAll(row.keySet());
			altPaths.addAll(row.values());
			solvedDims.addAll(row.keySet());
			if(projDims.isEmpty()) break;
		}

		altPaths.stream().forEach((p) -> {
			if(p.userEnabledRollDown()){
				solvedDims.remove(p.getExpressible());
			}
		});

		if(solvedDims.size()>0){
			String msg = "This query plan will result in duplicate records caused by these dimensions: ";
			msg += solvedDims.stream().map(Expressible::getName).collect(Collectors.joining(", "));
			msg += ". This maybe okay or you need to adjust related table joins settings.";
			op.getPlanMessages().add(msg);
		}
		op.addPaths(altPaths);
	}

	private void trySubqueryPlanning(List<Expressible> filterOnlyDims,
			List<Expressible> dimStack2, OptimizedPlan op) {
		// TODO Auto-generated method stub
		logger.info("Attempting subquery planning for disjoint dimensions for: " + filterOnlyDims);
	}

	private List<PlanPath> generateSecondaryPlan(List<Expressible> start, List<Table> targetTables) throws GraphException, PlannerException{
		List<PlanPath> secPaths = Lists.newArrayList();
		ArrayList<Table> tables = Lists.newArrayList();
		for(IJoinable jn : targetTables){
			tables.add((Table) jn);
		}
		
		List<Path> paths = pathSvc.getSecondaryPaths(tables, start);

		for(Path p : paths){
			// filtering paths to include only current datasource.
			Datasource pathDs = ((Table)p.endNode()).getDatasource();
			if(ds.equals(pathDs)){
				secPaths.add(new PlanPath(p));
			}
		}
		return secPaths;
	}

	public boolean hasMeasures() throws PlannerException{
		return selections.stream()
				.anyMatch(s -> s instanceof Measure) 
				|| filters.stream()
				.anyMatch(s -> s instanceof Measure);	
	}

	public boolean hasDimensions(){
			return selections.stream()
					.anyMatch(s -> s instanceof Dimension) 
					|| filters.stream()
					.anyMatch(s -> s instanceof Dimension);	
	}

	//TODO: this should be refactored out of here. conflating
	//filter planning an other planning not good idea.
//	public SetBlock getSetBlock(QueryBlock sourceBlock, BlockFilter sf) throws PlannerException, PersistenceException {
//		this.sourceBlock = sourceBlock;
//		this.virtualPu = veroFactory.createVirtualProcessor();
//
//		selections = Lists.newArrayList();
//		for(Dimension d : sourceBlock.getSelectDimensions()){
//			selections.add(d);
//		}
//
//		for(Dimension d : sf.getDimensions()){
//			selections.add(d);
//		}
//
//		for(Measure m : sf.getMeasures()){
//			selections.add(m);
//		}
//
//		List<OptimizedPlan> pls = process();
//		SetBlock sb = new SetBlock(sourceBlock, pls.get(0));
//		sb.setBlockFilter(sf);
//		return sb;
//	}
}
