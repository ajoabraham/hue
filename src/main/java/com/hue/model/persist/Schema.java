package com.hue.model.persist;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hue.model.*;

public class Schema {
	private Project project;
	private final Set<Datasource> datasources = Sets.newConcurrentHashSet();	
	private final Set<Dimension> dimensions = Sets.newConcurrentHashSet();	
	private final Set<Measure> measures = Sets.newConcurrentHashSet();	
	private final Map<Datasource,Set<Table>> dsTables = Maps.newConcurrentMap();
	private final Map<Datasource,Set<Join>> dsJoins = Maps.newConcurrentMap();
	private final Map<Datasource,TinkerGraph> dsGraph = Maps.newConcurrentMap();
	
	public Schema(Project project) {
		this.project = project;
	}
	
	public Project getProject() {
		return project;
	}
	
	public Set<Datasource> getDatsources() {
		return Collections.unmodifiableSet(datasources);
	}
	
	public void addDatasource(Datasource ds) {
		ds.setProject(project);
		datasources.add(ds);
	}
	
	public Optional<Datasource> getDatasource(String name) {
		return datasources.stream().filter(d -> d.getName().equalsIgnoreCase(name)).findFirst();
	}

	public Set<Dimension> getDimensions() {
		return Collections.unmodifiableSet(dimensions);
	}
	
	public void addDimension(Dimension d) {
		d.setProject(project);
		dimensions.add(d);
	}
	
	public Optional<Dimension> getDimension(String name) {
		return dimensions.stream().filter(d -> d.getName().equalsIgnoreCase(name)).findFirst();
	}

	public Set<Measure> getMeasures() {
		return Collections.unmodifiableSet(measures);
	}
	
	public Optional<Measure> getMeasure(String name) {
		return measures.stream().filter(d -> d.getName().equalsIgnoreCase(name)).findFirst();
	}
	
	public void addMeasure(Measure m) {
		m.setProject(project);
		measures.add(m);
	}
	
	public Set<Table> getTables(Datasource ds) {
		return Collections.unmodifiableSet(dsTables.get(ds));
	}
	
	public Optional<Table> getTable(Datasource ds, String name) {
		Set<Table> tables = dsTables.get(ds);
		
		if(tables == null) return Optional.empty();
		
		return tables.stream().filter(d -> d.getName().equalsIgnoreCase(name)).findFirst();
	}
	
	public Optional<Table> getTable(Datasource ds, String tableName, String schemaName) {
		Set<Table> tables = dsTables.get(ds);
		
		if(tables == null) return Optional.empty();
		
		return tables.stream()
				.filter(d -> d.getName().equalsIgnoreCase(tableName) && 
						d.getSchemaName().equalsIgnoreCase(schemaName))
				.findFirst();
	}
	
	public void addTable(Datasource ds, Table t) {
		addDatasource(ds);
		t.setDatasource(ds);
		
		if(dsTables.containsKey(ds)) {
			dsTables.get(ds).add(t);
		}else {
			Set<Table> n = Sets.newConcurrentHashSet();
			n.add(t);
			dsTables.put(ds, n);
		}
	}
	
	public void addJoin(Datasource ds, Join j) {
		addDatasource(ds);
		j.setDatasource(ds);
		
		if( getTable(ds, j.getLeftTableName()).isPresent() &&
				getTable(ds,j.getRightTableName()).isPresent()){
		
			if(dsJoins.containsKey(ds)) {
				dsJoins.get(ds).add(j);
			}else {
				Set<Join> n = Sets.newConcurrentHashSet();
				n.add(j);
				dsJoins.put(ds, n);
			}
			
		}else {
			// TODO: error table in join doesnt exist for ds
		}		
		
	}
	
	public void buildGraphs() {
		datasources.stream().forEach(ds -> {
			TinkerGraph tg = TinkerGraph.open();
			dsGraph.put(ds, tg);
			getTables(ds).stream().forEach(t -> tg.addVertex("table", t));
		});
	}
}
