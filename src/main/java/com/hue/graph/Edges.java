package com.hue.graph;

public enum Edges {

	HAS_EXPRESSION("has_expression"),
	HAS_VIRTUAL_EXPRESSION("has_virtual_expression"),
	QUERIES_FROM("queries_from"),
	JOINS("joins"),
    COMPOSED_OF("composed_of");

	private String name = null;
	
	private Edges(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
}
