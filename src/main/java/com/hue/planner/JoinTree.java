package com.hue.planner;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.hue.common.JoinType;
import com.hue.graph.Graphable;
import com.hue.graph.Path;
import com.hue.model.Join;
import com.hue.model.Table;

public class JoinTree {
	private static final Logger logger = LoggerFactory.getLogger(JoinTree.class.getName());

	private List<JoinPath> jp = Lists.newArrayList();
	private List<Join> jd = Lists.newArrayList();

	public JoinTree(Set<Path> set){
		for(Path p : set){
			Set<Table> potentialTables = Sets.newLinkedHashSet();
			// reverse sort join tree when it ends with a table
			// this ensures correct sorting of join tree in
			// dimension only and measures only cases
			Iterable<Graphable> nodes;
//			if(p.endNode() instanceof Table){
//				nodes = p.reverse();
//			}else{
//				nodes = p.nodes();
//			}
			nodes = p.reverse();
			for(Graphable n : nodes){
				if(n instanceof Table){
					potentialTables.add((Table)n);
				}
			}

			boolean tablesAlreadyPartOfaSet = false;
			for(JoinPath j : jp){
				if(j.containsTables(potentialTables)){
					tablesAlreadyPartOfaSet = true;
				}
			}

			// if the tables are not subset or whole set of an
			// existing join path then add it as new join path
			if(!tablesAlreadyPartOfaSet){
				ArrayList<Join> potentialJoins = Lists.newArrayList();
				for(Graphable r : nodes){
					if(r instanceof Join){
						potentialJoins.add((Join)r);
					}
				}
				jp.add(new JoinPath(potentialTables,potentialJoins));
			}
		}
	}

//	public JoinTree(List<EngineBlock> db) {
//		if(db.size()==1){
//			JoinDef jdf = new JoinDef(db.get(0),null,null,null);
//			jd.add(jdf);
//		}else{
//			for(int i=1;i<db.size();i++){
//				String exp = getBlockJoinExpression(db.get(0),db.get(i));
//				JoinDef jdf = new JoinDef(db.get(0),db.get(i),exp,JoinType.FULL_OUTER_JOIN);
//				if(exp.isEmpty())
//					jdf.setJoinType(JoinType.CROSS_JOIN);
//				jd.add(jdf);
//			}
//		}
//	}


	public List<Join> getJoinDefs() throws PlannerException{
		if(!jd.isEmpty()) return jd;

		// check if its multiblock or single block
		// joindef generation
		if(!jp.isEmpty()){
			for(JoinPath j : jp){
				jd.addAll(getJoinDefs(j));
			}
		}else{
			throw new PlannerException("JoinTree has no join paths.  "
					+ "You must first add paths to a join tree before calling getJoinDefs.");
		}

		return jd;
	}

	private List<Join> getJoinDefs(JoinPath jp) {
		long startTime = System.nanoTime();

		List<Join> jd = Lists.newArrayList();
		int i = 0;
		
		Table lastTableRight = null;
		for (Iterator<Table> it = jp.getTableNodes().iterator(); it.hasNext();) {
			Table tbl = it.next();
			if (it.hasNext() && lastTableRight == null) {
				lastTableRight = it.next();

				Join joins = jp.getRelationships().get(i);
				String formula = joins.getSql();
//					formula = switchSides(formula);
//					if(!joins.getStartNode().equals(tbl)){
//						formula = switchSides(formula);
//					}

				Join jdi = new Join(tbl, lastTableRight,
						formula, joins.getJoinType());
				jdi.setCardinalityType(joins.getCardinalityType());
				jd.add(jdi);
			}
			else if (!it.hasNext() && lastTableRight == null) {
				jd.add(new Join(tbl, null, null, null));
			}
			else {
				Join joins = jp.getRelationships().get(i);
				String formula = joins.getSql();
//					if(!joins.getStartNode().equals(tbl)){
//						formula = switchSides(formula);
//					}

				Join jdi = new Join(lastTableRight, tbl,
										formula, joins.getJoinType());
				jdi.setCardinalityType(joins.getCardinalityType());
				jd.add(jdi);
				lastTableRight = tbl;
			}
			i++;
		}
	
		// long startTime = System.nanoTime();
		System.out.println("getJoinDefs: " + (System.nanoTime() - startTime) / 1000000.0 + "md");
		return jd;
	}
	
	public void addCrossJoin(JoinTree joinTree) throws PlannerException {
		// must call getJoinDefs to make sure
		// join defs are processed.
		getJoinDefs();
		List<Join> crossjd = joinTree.getJoinDefs();

//		if(crossjd.size()>1){
//			JoinDef cj = new JoinDef(
//					jd.get(0).getLeft(),
//					crossjd.get(0).getLeft(),
//					null, JoinType.CROSS_JOIN);
//			crossjd.add(0, cj);
//			jd.addAll(crossjd);
//		}else{
//			JoinDef cjd = crossjd.get(0);
//			cjd.setRight(cjd.getLeft());
//			cjd.setLeft(jd.get(0).getLeft());
//			cjd.setJoinType(JoinType.CROSS_JOIN);
//			jd.add(cjd);
//		}


		Join cj = new Join(
				jd.get(0).getLeft(),
				crossjd.get(0).getLeft(),
				null, JoinType.CROSS_JOIN);
		crossjd.add(0, cj);
		jd.addAll(crossjd);

	}

	public String toString(){
		String output="";

		try {
			for(Join j : getJoinDefs()){
				output += j.toString() + "\n";
			}
		} catch (PlannerException e) {
			logger.error("No JoinDefs in this JoinTree." ,e);
			output += "No JoinDefs to print in this collection.";
		}

		return output;
	}

	public class JoinPath{
		private Set<Table> tables;
		private ArrayList<Join> joinDefs;

		public JoinPath(Set<Table> tables, ArrayList<Join> joinDefs){
			this.tables=tables;
			this.joinDefs=joinDefs;
		}

		public boolean containsTables(Set<Table> tables){
			return this.tables.containsAll(tables);
		}

		public Set<Table> getTableNodes(){
			return tables;
		}

		public ArrayList<Join> getRelationships(){
			return joinDefs;
		}

		public String toString() {
			String output = "";
			int i=0;
			for(Iterator<Table> it = tables.iterator();it.hasNext();){
				Table tbl = it.next();
				output += tbl.getName();
				if(it.hasNext()){
					output += "-[";
					output += joinDefs.get(i).getSql() + "]->";
				}
				i++;
			}
			return output;
		}

	}
}
