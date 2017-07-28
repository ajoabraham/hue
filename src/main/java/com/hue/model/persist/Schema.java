package com.hue.model.persist;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hue.common.CardinalityType;
import com.hue.model.*;

public class Schema {
	protected static final Logger logger = LoggerFactory.getLogger(Schema.class.getName());
	
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
		return Collections.unmodifiableSet(dsTables.getOrDefault(ds, Sets.newHashSet()));
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
		t.setProject(getProject());
		
		if(dsTables.containsKey(ds)) {
			dsTables.get(ds).add(t);
		}else {
			Set<Table> n = Sets.newConcurrentHashSet();
			n.add(t);
			dsTables.put(ds, n);
		}
	}
	
	public Set<Join> getJoins(Datasource ds) {
		return Collections.unmodifiableSet(dsJoins.getOrDefault(ds, Sets.newHashSet()));
	}
	
	public void addJoin(Datasource ds, Join j) {
		addDatasource(ds);
		j.setDatasource(ds);
		
		if( j.getLeft() != null  &&
				j.getRight() != null){
		
			if(dsJoins.containsKey(ds)) {
				dsJoins.get(ds).add(j);
			}else {
				Set<Join> n = Sets.newConcurrentHashSet();
				n.add(j);
				dsJoins.put(ds, n);
			}
			
		}		
		
	}
	
	public void buildGraphs() {
		datasources.stream().forEach(ds -> {
			TinkerGraph tg = TinkerGraph.open();
			dsGraph.put(ds, tg);
			getTables(ds).stream().forEach(t -> {
				t.v = tg.addVertex("table");
				t.v.property("object", t);
			});
			getJoins(ds).stream().forEach(j -> {
				j.v = tg.addVertex("join");
				j.v.property("object", j);
				
				if(j.getCardinalityType()==CardinalityType.ONE_TO_MANY) {
					((Table) j.getLeft()).v.addEdge("joins", j.v);
					j.v.addEdge("joins", ((Table) j.getRight()).v);
				}
				else if(j.getCardinalityType()==CardinalityType.MANY_TO_ONE) {					
					j.v.addEdge("joins", ((Table) j.getLeft()).v);
					((Table) j.getRight()).v.addEdge("joins", j.v);
				}else {
					((Table) j.getLeft()).v.addEdge("joins", j.v);
					j.v.addEdge("joins", ((Table) j.getRight()).v);
					
					j.v.addEdge("joins", ((Table) j.getLeft()).v);
					((Table) j.getRight()).v.addEdge("joins", j.v);
				}
			});
		});
	}
}
