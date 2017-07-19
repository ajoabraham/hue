package com.hue.model;

import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Sets;

public class Expressible extends HueBase {
	private static final long serialVersionUID = 1L;	
	private final Set<FieldExpression> expressions = Sets.newHashSet();
		
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
}
