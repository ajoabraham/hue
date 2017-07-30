package com.hue.model;

import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Sets;
import com.hue.graph.Graphable;

public class FieldExpression implements Graphable{
	private String sql;
	private Set<Table> tables = Sets.newHashSet();
	
	@JsonIgnore
	private Vertex v;
	
	public FieldExpression() {
	}
	
	public FieldExpression(String sql, Set
			<Table> tables) {
		setSql(sql);
		setTables(tables);
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		if(sql != null)
			this.sql = sql.trim();
	}
	
	@Override
	public Vertex v() {
		return v;
	}
	@Override
	public void v(Vertex v) {
		this.v = v;		
	}
	
	@Override
	public String getName() {
		return this.toString();
	}
	
	public Set<Table> getTables() {
		return tables;
	}

	public void setTables(Set<Table> tables) {
		this.tables = tables;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((sql == null) ? 0 : sql.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FieldExpression other = (FieldExpression) obj;
		if (sql == null) {
			if (other.sql != null)
				return false;
		} else if (!sql.equals(other.sql))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "exp:" + sql;
	}

	public boolean hasTable(Table t) {
		return getTables()
					.stream()
					.filter(tt -> tt.equals(t))
					.findFirst()
					.isPresent();
	}
}
