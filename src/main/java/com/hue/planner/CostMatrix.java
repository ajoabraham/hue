 package com.hue.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.hue.common.TableType;
import com.hue.model.Dimension;
import com.hue.model.Expressible;
import com.hue.model.Table;


public class CostMatrix {
	private List<PlanPath> vps;
	private ArrayList<Table> sortKeys = Lists.newArrayList();
	private HashBasedTable<Table, Expressible, PlanPath> matrix;
	private List<Table> hints = null;

	public CostMatrix(List<PlanPath> vps) throws PlannerException{
		this.vps = vps;
		matrix = HashBasedTable.create();
		for(PlanPath vp : vps){
			if(matrix.get(vp.getRootTable(), vp.getExpressible()) != null && vp.getExpressible() instanceof Dimension)
					vp = PathUtils.getOptimalDimVeroPath(vp.getRootTable(), vp.getExpressible(), vps);
			matrix.put(vp.getRootTable(), vp.getExpressible(), vp);
		}
		sortKeys = Lists.newArrayList(matrix.rowKeySet());
	}

	/**
	 * Call this after any table update, insert, or remove operation.
	 */
	public void refreshSortKeys(){
		sortKeys = Lists.newArrayList(matrix.rowKeySet());
	}

	public void setHints(List<Table> hints){
		this.hints = hints;
	}
	
	public void sortForDimensions(){
		sortKeys.sort((vn1,vn2) -> {
			int res =0;

			if(hints  != null){
				res = -1*Ints.compare(getRowHintCount(vn1), getRowHintCount(vn2));
			}

			if(res==0){
				res = -1*Ints.compare(getRowDimCount(vn1), getRowDimCount(vn2));
			}
			
			if(res==0){
				if(getRootSelectionCount(vn1)==0)
					res = 1;
				
				if(getRootSelectionCount(vn2)==0 && res==0)
					res = -1;
			}
			
			if(res==0){
				TableType vn1_type = vn1.getTableType();
				TableType vn2_type = vn2.getTableType();
				if(vn1_type == TableType.DIMENSION && vn2_type != TableType.DIMENSION){
					res = -1;
				}else if(vn1_type != TableType.DIMENSION && vn2_type == TableType.DIMENSION){
					res = 1;
				}
			}

			if(res==0)
				res = Double.compare(getRowCost(vn1), getRowCost(vn2));

			return res;
		});
	}

	public int getRootSelectionCount(Table rootTable) {
		int sels = 0;
		for(Entry<Expressible, PlanPath> e : matrix.rowMap().get(rootTable).entrySet()){
			if(e.getValue().getSelectTable() == rootTable)
				sels += 1;
		}
		return sels;
	}

	public void applyDefaultSort(){
		sortKeys.sort((a, b) -> {
			int res =0;

			if(hints  != null){
				res = -1*Ints.compare(getRowHintCount(a), getRowHintCount(b));
			}

			if(res==0)
				res = -1*Ints.compare(getRowDimCount(a), getRowDimCount(b));

			if(res==0)
				res = Doubles.compare(getRowCost(a), getRowCost(b));
			
			if(res==0){
				if(getRootSelectionCount(a)==0)
					res = 1;
				
				if(getRootSelectionCount(b)==0 && res==0)
					res = -1;
			}

			return res;
		});
	}

	public int getRowHintCount(Table rootTable) {

		if(hints.size()==0 ){
			return 0;
		}

		Map<Expressible, PlanPath> row = matrix.rowMap().get(rootTable);
		int hintCount = 0;
		for(Entry<Expressible, PlanPath> e : row.entrySet()){

			for(Table h : hints){
				if(e.getValue().contains(h))
					hintCount += 1;
			}
		}
		return hintCount;
	}

	public ArrayList<Table> getSortedKeys(){
		return sortKeys;
	}

	public HashBasedTable<Table, Expressible, PlanPath> getMatrix(){
		return matrix;
	}

	public int getRowDimCount(Table rootTable){
		int dims = 0;
		for(Entry<Expressible, PlanPath> e : matrix.rowMap().get(rootTable).entrySet()){
			if(e.getKey() instanceof Dimension)
				dims += 1;
		}
		return dims;
	}

	public Double getRowCost(Table rootTable){
		double cost = 0;
		for(Entry<Expressible, PlanPath> e : matrix.rowMap().get(rootTable).entrySet()){
			PlanPath vp = e.getValue();
			cost += vp.getCost();
		}
		return cost;
	}

	@Override
	public String toString() {
		String out = "\n";
		int maxColLength =4;
		int maxTableLength=4;
		List<Expressible> col = Lists.newArrayList();
		for(PlanPath vp : vps){
			if(!col.contains(vp.getExpressible())){
				col.add(vp.getExpressible());
				if(vp.getExpressible().toString().length()>maxColLength)
					maxColLength = vp.getExpressible().toString().length();
			}
			if(vp.getRootTable().toString().length()>maxTableLength)
				maxTableLength = vp.getRootTable().toString().length();
		}
		String tf = "%-" + (maxTableLength+1) + "s";
		String cf = "%-" + (maxColLength+1) + "s";
		out += String.format(tf, "ROOT");
		for(Expressible vn : col){
			out += "|" +String.format(cf, vn);
		}

		out += "\n";
		Map<Table, Map<Expressible, PlanPath>> map = matrix.rowMap();

		for (Table row : sortKeys) {
			out += String.format(tf,row);
		    for (Expressible vn : col) {
			PlanPath p = map.get(row).get(vn);
			Double cost = null;
			if(p != null)
				cost = p.getCost();
		        out += "|" + String.format("%-"+(maxColLength+1)+"s", "  "+cost);
		    }
		    out += "\n";
		}
		return out;
	}

}
