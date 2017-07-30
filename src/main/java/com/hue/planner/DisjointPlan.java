package com.hue.planner;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.hue.common.DisjointType;
import com.hue.graph.Path;
import com.hue.model.Dimension;
import com.hue.model.Expressible;
import com.hue.model.FieldExpression;
import com.hue.model.IJoinable;
import com.hue.model.Table;

/**
 * This is a special plan type to capture paths that cannot be resolved
 * based on the current knowledge graph (kg).  Currently, only paths with dimensions and
 * dimensions in general will be included in a disjoint scenario.
 *
 * Example:
 *
 * User wishes to get a result set with Customer Name, Product Name, and Revenue.
 * If there is no path available that unifies all three entities this gives us a disjoint
 * scenario.  Lets say, in this case Product Name is not reachable.  This will
 * be an error situation and the engine will output a cross join between the
 * selected root table and any table available for product name.
 *
 * Plan:
 * 	Customer Name -> TableA
 * 	Revenue		  -> TableA
 * 		CROSS JOINS
 * 	Product Name  -> ProductTable
 *
 * Normal Plan:
 * 	Customer Name -> TableA
 * 	Revenue		  -> TableA
 * 	Product Name  -> TableA -> ProductTable
 *
 * @author ajoabraham
 *
 */
public class DisjointPlan {
	private static final Logger logger = LoggerFactory.getLogger(DisjointPlan.class.getName());

	private List<PlanPath> veroPaths = Lists.newArrayList();
	private JoinTree joinTree;
	private Map<Dimension,DisjointType> disjointMap = Maps.newHashMap();
	HashMap<Expressible, IJoinable> expressibleJoinableMapping = Maps.newHashMap();
	HashMap<Expressible, FieldExpression> expressibleExpressionMapping = Maps.newHashMap();
	private HashSet<Dimension> dimensions;

	public DisjointPlan(){}


	public void setDisjointType(Dimension d, DisjointType dt){
		disjointMap.put(d, dt);
	}

	public DisjointType getDisjointType(Dimension d){
		DisjointType val = disjointMap.get(d);
		if(val == null){
			return DisjointType.SUPPRESSED;
		}
		return disjointMap.get(d);
	}

	/**
	 * Return neo4j paths from the current plan. Does not included paths
	 * from disjointed paths.
	 *
	 * @return
	 */
	public Set<Path> getPlanPaths() {
		Set<Path> paths = Sets.newLinkedHashSet();
		Collections.sort(veroPaths);
		if(paths.size()==0){
			for(PlanPath vp : veroPaths){
				paths.add(vp.getPath());
			}
		}
		return paths;
	}

	public void addPath(PlanPath path) {
		veroPaths.add(path);
	}

	public void addPaths(Set<PlanPath> veroPath) {

		Iterables.addAll(veroPaths,veroPath);
	}

	public Table getRootTable(){
		if(veroPaths.size()>0)
			return veroPaths.get(0).getRootTable();

		return null;
	}

	public JoinTree getJoinTree() throws PlannerException{

		if (joinTree != null) return joinTree;

		joinTree = new JoinTree(getPlanPaths());
		logger.debug("Disjoint paths: \n");
		logger.debug(getPlanPaths().toString());

		return joinTree;
	}

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
			Optional<PlanPath> res = veroPaths.stream().filter((vp) ->
				vp.getExpressible().equals(e)
			 ).findFirst();

			// if no match was found and we are using disjoints then
			// find it in the disjoint collection
			FieldExpression ee = null;
			if(res.isPresent()){
				t = res.get().getSelectTable();
				ee = res.get().getExpression();

				expressibleJoinableMapping.put(e,t);
				expressibleExpressionMapping.put(e, ee);
			}else{
				logger.info(e.getName() + " was not available in the disjointPlan with root: " + getRootTable().getName());
			}

		} catch (Exception e1) {
			throw new PlannerException("Unable to find joinable/table for " + expressibleName,e1);
		}
		return t;
	}

	public HashSet<Dimension> getPlanDimensions(){
		if(dimensions != null) return dimensions;

		dimensions = Sets.newHashSet();
		
		for(PlanPath p : veroPaths){
			if(p.getExpressible() instanceof Dimension){
				dimensions.add((Dimension) p.getExpressible());
			}
		}
		
		return dimensions;
	}

	public void setAllDimensionsAsCrossJoin() {
		getPlanDimensions().stream().forEach( (d) -> {
			disjointMap.put(d, DisjointType.CROSS_JOIN);
		});
	}

	/**
	 * This will mark the submitted dimensions as Cross_Join and all
	 * other dimensions as Suppressed.
	 *
	 * @param xj - Set of Dimensions
	 * @return List of Dimensions that are cross joined and can be planned
	 * 		by this disjointPlan.  The resolver can use this to pick a plan.
	 */
	public List<Dimension> setResolvedCrossJoins(SetView<Dimension> xj) {
		List<Dimension> plannedXj = Lists.newArrayList();
		getPlanDimensions().stream().forEach((d) -> {
			if(xj.contains(d)){
				disjointMap.put(d,DisjointType.CROSS_JOIN);
				plannedXj.add(d);
			}else{
				disjointMap.put(d,DisjointType.SUPPRESSED);
			}
		});
		return plannedXj;
	}
}
