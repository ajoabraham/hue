package com.hue.graph;

import org.apache.tinkerpop.gremlin.structure.Vertex;

public interface Graphable {
	
	public Vertex v();
	public void v(Vertex v);
}
