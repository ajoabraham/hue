package com.hue.graph;

import java.util.List;

import com.google.common.collect.Lists;
import com.hue.common.TableType;
import com.hue.model.Table;

public class Path {
	private int hasFactTable =-1;
	private List<Graphable> nodes = Lists.newArrayList();
	
	public Graphable startNode(){
		if(nodes.size()==0){
			return null;
		}
		return nodes.get(0);
	}
	
	public Graphable endNode(){
		if(nodes.size()==0){
			return null;
		}
		return nodes.get(nodes.size()-1);
	}
	
	public void add(Graphable node){
		nodes.add(node);
	}
	
	public List<Graphable> nodes(){
		return nodes;
	}
	
	public List<Graphable> tail(){
		if(nodes.size()>1){
			return Lists.newArrayList(nodes.subList(1, nodes.size()));
		}else{
			return nodes;
		}
	}
	
	public boolean isEmpty(){
		return nodes.isEmpty();
	}
	
	public List<Graphable> reverse(){
		return Lists.reverse(nodes);
	}
	
	public Path append(Path path) throws GraphException{
		if(!endNode().equals(path.startNode())){
			throw new GraphException("Cannot append path when the end node and start node does not match.");
		}
		Path np = clone();
		for(Graphable v : path.tail()){
			np.add(v);
		}
		return np;
	}
	
	@Override
	public String toString() {
		return nodes.toString()+"\n";
	}
	
	@Override
	protected Path clone() {
		Path p = new Path();
		for(Graphable v : nodes()){
			p.add(v);
		}
		return p;
	}

	public boolean contains(Object vo) {
		return nodes().contains(vo);
	}
	
	public boolean hasFactTable(){
		if(hasFactTable == -1){
			boolean f = nodes.stream()
			.anyMatch(k -> (k instanceof Table && ((Table)k).getTableType() == TableType.FACT));
			
			if(f){
				hasFactTable=1;
			}else{
				hasFactTable=0;
			}
		}
		
		if(hasFactTable==1){
			return true;
		}else{
			return false;
		}
	}
}
