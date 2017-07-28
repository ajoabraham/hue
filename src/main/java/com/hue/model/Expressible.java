package com.hue.model;

import java.util.Optional;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Sets;
import com.hue.graph.Graphable;

public class Expressible extends HueBase implements Graphable {
	private static final long serialVersionUID = 1L;	
	private final Set<FieldExpression> expressions = Sets.newHashSet();
	
	@JsonIgnore
	private Vertex v;
	
	public void addExpression(FieldExpression exp) {
		Optional<FieldExpression> res = expressions.stream().filter( exp1 -> exp1.equals(exp)).findFirst();
		if(res.isPresent()) {
			res.get().getTables().addAll(exp.getTables());
		}else {
			expressions.add(exp);
		}
	}
	
	public void removeExpression(FieldExpression exp) {
		expressions.remove(exp);
	}
	
	public Set<FieldExpression> getExpressions(){
		return expressions;
	}
	
	public Optional<FieldExpression> getExpression(Table t) {
		return getExpressions().stream()
			.filter(fe -> fe.hasTable(t))
			.findFirst();
	}
	
	@Override
	public Vertex v() {
		return v;
	}
	@Override
	public void v(Vertex v) {
		this.v = v;		
	}
}
