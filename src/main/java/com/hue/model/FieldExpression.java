package com.hue.model;

import java.util.Set;

import com.google.common.collect.Sets;

public class FieldExpression {
	private String sql;
	private Set<Table> tables = Sets.newHashSet();

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
		return sql+":"+tables.toString();
	}

	public boolean hasTable(Table t) {
		return getTables()
					.stream()
					.filter(tt -> tt.equals(t))
					.findFirst()
					.isPresent();
	}
}
