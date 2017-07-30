package com.hue.planner;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import com.google.common.collect.Maps;
import com.hue.common.TableType;
import com.hue.graph.Path;
import com.hue.model.Expressible;
import com.hue.model.Table;

public class PathUtils {

	public static PlanPath getOptimalDimVeroPath(Table table, Expressible expressible, List<PlanPath> vps) {
		Map<Table,Integer> selectTables = Maps.newHashMap();
		Map<Table,PlanPath> selectTableToPath = Maps.newHashMap();
		// find all paths that have same root and expressible but different select table.
		// ie year - [T:date] - [T:fact table]  - select = [T:date] != [T:fact table]
		//
		for(PlanPath vp : vps){
			if(vp.getRootTable().equals( table) && vp.getExpressible().equals(expressible)
					// && !vp.getSelectTable().equals(table) vb-411
					){
				int score = 0;
				if(vp.getSelectTable().getTableType() == TableType.FACT ){
					score = 16;
				}
				selectTables.put(vp.getSelectTable(),score);
				selectTableToPath.put(vp.getSelectTable(),vp);
			}
		}
		// find all paths that have the same select table and root table but diff expressible.
		for(PlanPath vp : vps){
			for(Table sel : selectTables.keySet()){
				if(vp.getSelectTable().equals(sel) && vp.getRootTable().equals(table)
						&& !vp.getExpressible().equals(expressible))
					selectTables.put(sel,selectTables.get(sel) + 1);
			}
		}
		Comparator<Entry<Table, Integer>> byValue = (entry1, entry2) -> entry1.getValue().compareTo(
	            entry2.getValue());

		Optional<Entry<Table, Integer>> val = selectTables
	            .entrySet()
	            .stream()
	            .sorted(byValue.reversed())
	            .findFirst();

		return selectTableToPath.get(val.get().getKey());
	}
	
	public static PlanPath createPlanPath(Path p) throws PlannerException{

		PlanPath vp = new PlanPath(p);

		return vp;
	}

	public static boolean hasFactTables(List<PlanPath> pls) {
		return pls.stream().anyMatch(p -> p.hasFactTable());
	}

}
