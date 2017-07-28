package com.hue.model;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.hue.common.ColumnKeyType;
import com.hue.common.TableType;
import com.hue.graph.Graphable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Table extends HueBase implements IJoinable,Graphable {
	private static final long serialVersionUID = 1L;

	private String schemaName;
	private String physicalName;
	private Integer rowCount = -1;
	private TableType tableType = TableType.DIMENSION;
	
    @JsonIgnore
	private Datasource datasource;
	private Set<Column> columns = Sets.newHashSet();
	
	@JsonIgnore
	private Vertex v;
	
	public Table() {}
	public Table(Datasource ds, String schema, String name, TableType type, int rowCount) {
		setDatasource(ds);
		setName(name);
		setSchemaName(schema);
		setPhysicalName(name);
		setTableType(type);
		setRowCount(rowCount);
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public String getPhysicalName() {
		return this.physicalName == null ? getName() : this.physicalName;
	}

	public void setPhysicalName(String physicalName) {
		this.physicalName = physicalName;
	}

	public Integer getRowCount() {
		return this.rowCount == null ? 0 : this.rowCount;
	}

	public void setRowCount(Integer rowCount) {
		this.rowCount = rowCount;
	}

	public TableType getTableType() {
		return this.tableType;
	}

    @JsonIgnore
	public List<Column> getPrimaryKeys(){
		return getKeys(ColumnKeyType.PRIMARY_KEY);
	}

    @JsonIgnore
    public List<Column> getForeignKeys() {
        return getKeys(ColumnKeyType.FOREIGN_KEY);
    }

	public void setTableType(TableType tableType) {
		this.tableType = tableType;
	}

	public void setColumns(Set<Column> columns) {
		this.columns = columns;
	}
	
	public Set<Column> getColumns() {
		return columns;
	}

	public Datasource getDatasource() {
		return datasource;
	}

	public void setDatasource(Datasource datasource) {
		this.datasource = datasource;
	}

	@Override
	public Object clone() {
		Table table = new Table();
		table.setDatasource(getDatasource());
		table.setDesc(getDesc());
		table.setName(getName());
		table.setPhysicalName(getPhysicalName());
		table.setSchemaName(getSchemaName());
		table.setRowCount(getRowCount());
		table.setTableType(getTableType());
		return table;
	}

    public List<Column> getKeys(ColumnKeyType keyType) {
        List<Column> l = Lists.newArrayList();
        for(int i=0;i<getColumns().size();i++){
            Column c = getColumns().toArray(new Column[l.size()])[i];
            if(keyType.equals(c.getKeyType())) {
                l.add(c);
            }
        }
        return l;
    }
    
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((datasource == null) ? 0 : datasource.hashCode());
		result = prime * result + ((physicalName == null) ? 0 : physicalName.hashCode());
		result = prime * result + ((schemaName == null) ? 0 : schemaName.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		Table other = (Table) obj;
		if (datasource == null) {
			if (other.datasource != null)
				return false;
		} else if (!datasource.equals(other.datasource))
			return false;
		if (physicalName == null) {
			if (other.physicalName != null)
				return false;
		} else if (!physicalName.equals(other.physicalName))
			return false;
		if (schemaName == null) {
			if (other.schemaName != null)
				return false;
		} else if (!schemaName.equals(other.schemaName))
			return false;
		return true;
	}
	
	@JsonIgnore
	@Override
	public List<String> getPhysicalNameSegments() {
		if(getSchemaName() == null){
			return Collections.singletonList(getPhysicalName());
		}
		return Arrays.asList(getSchemaName(), getPhysicalName());
	}
    
    @Override
    public String toString() {
    		return String.join(".", getPhysicalNameSegments());
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
