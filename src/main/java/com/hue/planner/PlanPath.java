package com.hue.planner;

import com.google.common.primitives.Doubles;
import com.hue.graph.Graphable;
import com.hue.graph.Path;
import com.hue.model.Dimension;
import com.hue.model.Expressible;
import com.hue.model.FieldExpression;
import com.hue.model.Join;
import com.hue.model.Measure;
import com.hue.model.Table;

public class PlanPath implements Comparable<PlanPath> {
	private Path path;
	private Expressible expressible;
	private Table rootTable;
	private Table selectTable;
	private FieldExpression expression;
	private double cost = -999;
	private boolean hasFactOrAgg = false;

	protected PlanPath(Path p) throws PlannerException{
		validate(p);
		this.path = p;
		expressible = (Expressible) p.startNode();
		rootTable = (Table) p.endNode();
		expression = (FieldExpression)p.nodes().get(1);
		selectTable = (Table) p.nodes().get(2);

//		if(expression instanceof MeasureExpression &&
//				((MeasureExpression)expression).isVirtual()){
//			throw new PlannerException("Virtual expressions are not allowed in a VeroPath. Use VeroVirtualPath.");
//		}
		
		hasFactOrAgg = p.hasFactTable();
		boolean pathHasJoins = false;
		double cost = 0;
		for(Graphable vo : p.nodes()){
			if(vo instanceof Join){
				Join je = (Join) vo;
				pathHasJoins = true;
				cost += je.getCost();
			}
		}
		if(!pathHasJoins){
			cost += rootTable.getRowCount();
		}
		this.cost = cost;	
	}

	public Expressible getExpressible(){
		return expressible;
	}

	public FieldExpression getExpression(){
		return expression;
	}

	public Table getRootTable(){
		return rootTable;
	}

	public Table getSelectTable(){
		return selectTable;
	}

	/**
	 * Returns base cost.
	 *
	 * @return
	 */
	public double getCost(){
		return cost;
	}


	private void validate(Path p) throws PlannerException{
		if(!(p.startNode() instanceof Dimension ||
				p.startNode() instanceof Measure)){
			throw new PlannerException("The path should start with either Dimension or Measure.");
		}

		if(!(p.endNode() instanceof Table)){
			throw new PlannerException("The path should end with a Table.");
		}
		
		if(!(p.nodes().size()>2)){
			throw new PlannerException("A path should have at least 3 nodes.");
		}
	}
	@Override
	public String toString() {
		return this.path.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof PlanPath){
			return path.equals(((PlanPath) obj).getPath());
		}else{
			return super.equals(obj);
		}
	}

	public Path getPath() {
		return path;
	}

	public boolean contains(Graphable n) {
		boolean hasNode = false;
		
		for(Graphable vo : path.nodes()){
			if(vo.equals(n)){
				hasNode = true;
				break;
			}
		}
		
		return hasNode;
	}

	public boolean userEnabledRollDown() {
		int roll = -1;
		for(Graphable vo : path.nodes()){
			if(vo instanceof Join){
				Join je = (Join) vo;
				roll = je.getAllowRollDown();
			}
		}
		
		if(roll ==1){
			return true;
		}else{
			return false;
		}
	}
	
	public boolean hasFactTable(){
		return hasFactOrAgg;
	}

	@Override
	public int compareTo(PlanPath o) {
		int res = -1*Doubles.compare(getCost(), o.getCost());
		if(res==0)
			res = getSelectTable().getName().compareTo(o.getSelectTable().getName());
		
		return res;
	}
}
