package com.hue.model.persist;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Maps;
import com.hue.common.CardinalityType;
import com.hue.common.ColumnKeyType;
import com.hue.common.DBType;
import com.hue.common.DataType;
import com.hue.common.JoinType;
import com.hue.common.TableType;
import com.hue.model.Column;
import com.hue.model.Datasource;
import com.hue.model.Dimension;
import com.hue.model.FieldExpression;
import com.hue.model.Join;
import com.hue.model.Measure;
import com.hue.model.Project;
import com.hue.model.Table;

public class ProjectServices {
	private static final Logger logger = LoggerFactory.getLogger(ProjectServices.class.getName());
	
	private static ProjectServices instance = null;

	private static ObjectMapper mapper;
	private final HueConfig config = new HueConfig();
	
	private final Map<Project,Set<Datasource>> projDatasources = Maps.newHashMap();
	private final Map<Project,Set<Dimension>> projDims = Maps.newHashMap();
	private final Map<Project,Set<Measure>> projMeasures = Maps.newHashMap();
	private final Map<Project,Map<Datasource,Set<Join>>> projDJoins = Maps.newHashMap();
	private final Map<Project,Map<Datasource,Set<Table>>> projDTables = Maps.newHashMap();
	private final Map<Project,Map<Datasource,TinkerGraph>> projGraph = Maps.newHashMap();
	
	protected ProjectServices() {	}
	
	public static ProjectServices getInstance() {
      if(instance == null) {
         instance = new ProjectServices();
         mapper = new ObjectMapper();
         mapper.registerModule(new HueSerDeModule());
         
         mapper.enable(SerializationFeature.INDENT_OUTPUT);
      }
      return instance;
	}
	
	public HueConfig getConfig() {
		return config;
	}
	
	public Project getProject(String projectName) {
		return null;
	}
	
//	public void createNewProject(Project project) throws IOException {
//		if(PersistUtils.projectExists(project.getName())) {
//			String msg = String.format("Project with name %s already exists.", project.getName());
//			throw new IOException(msg);
//		}
//		
//		Path pr = FileSystems.getDefault().getPath(config.getProjectsDirectory(), project.getName());		
//		Files.createDirectories(pr);
//		Files.createDirectories(FileSystems.getDefault().getPath(config.getProjectsDirectory(), project.getName(), "dimensions"));
//		Files.createDirectories(FileSystems.getDefault().getPath(config.getProjectsDirectory(), project.getName(), "measures"));
//		
//		
//		mapper.writeValue(new File(pr.toString()+"/project.json"), project);
//	}
//	
//	public void createNewDS(Project project, Datasource ds) throws IOException {
//		if(PersistUtils.isValidDatasource(project.getName(), ds.getName())) {
//			String msg = String.format("Datasource with name %s already exists in project %s.", ds.getName(), project.getName());
//			throw new IOException(msg);
//		}
//		Path pr = FileSystems.getDefault().getPath(config.getProjectsDirectory(), project.getName(), ds.getName());
//		Files.createDirectories(pr);	
//		mapper.writeValue(new File(pr.toString()+"/datsource.json"), ds);
//		
//		Files.createDirectories(FileSystems.getDefault().getPath(config.getProjectsDirectory(), project.getName(), ds.getName(), "tables"));
//		Files.createDirectories(FileSystems.getDefault().getPath(config.getProjectsDirectory(), project.getName(), ds.getName(), "joins"));
//	}
//	
//	public void createProjectScaffold(String name) throws IOException {
//		Project pj = new Project();
//		if(name == null)
//			pj.setName("Example Project");
//		else
//			pj.setName(name);
//		
//		createNewProject(pj);
//		
//		Datasource ds = new Datasource();
//		ds.setDatabaseType(DBType.POSTGRESQL);
//		ds.setName("example ds");
//		
//		createNewDS(pj, ds);
//		
//		Table t = new Table(ds, "schema_name", "table_name", TableType.DIMENSION, 1000);
//		t.getColumns().add(new Column("col1", ColumnKeyType.NO_KEY_TYPE, DataType.INTEGER));
//		t.getColumns().add(new Column("col2", ColumnKeyType.PRIMARY_KEY, DataType.INTEGER));
//				
//		Path pt = FileSystems.getDefault().getPath(config.getProjectsDirectory(), pj.getName(), ds.getName(), "tables");
//		mapper.writeValue(new File(pt.toString()+"/"+String.join(".", t.getPhysicalNameSegments())+".json"), t);
//		
//		Table t2 = new Table(ds, "schema_name2", "table_name2", TableType.DIMENSION, 1000);
//		t2.getColumns().add(new Column("col1_t2", ColumnKeyType.NO_KEY_TYPE, DataType.INTEGER));
//		t2.getColumns().add(new Column("col2_t2", ColumnKeyType.PRIMARY_KEY, DataType.INTEGER));
//		mapper.writeValue(new File(pt.toString()+"/"+String.join(".", t.getPhysicalNameSegments())+".json"), t2);
//		
//		Join j = new Join(t, t2, "t.col1 = t2.col1_t2", JoinType.INNER_JOIN);
//		j.setCardinalityType(CardinalityType.ONE_TO_MANY);
//		pt = FileSystems.getDefault().getPath(config.getProjectsDirectory(), pj.getName(), ds.getName(), "joins");
//		mapper.writeValue(new File(pt.toString()+"/"+j.getLeftTableName()+"_"+ j.getRightTableName() +".json"), j);
//		
//		pt = FileSystems.getDefault().getPath(config.getProjectsDirectory(), pj.getName(), "dimensions");
//		Dimension d = new Dimension();
//		d.setName("dimension example");
//		FieldExpression fe = new FieldExpression();
//		fe.setSql("first_name || last_name");
//		fe.getTables().add(t2);
//		d.addExpression(fe);
//		
//		mapper.writeValue(new File(pt.toString()+"/"+d.getName()+".json"), d);
//	}
}
