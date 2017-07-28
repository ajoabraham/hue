package com.hue.model.persist;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Sets;
import com.hue.model.Datasource;
import com.hue.model.Dimension;
import com.hue.model.Join;
import com.hue.model.Measure;
import com.hue.model.Project;
import com.hue.model.Table;
import com.hue.services.ServiceException;

public class ProjectServices {
	private static final Logger logger = LoggerFactory.getLogger(ProjectServices.class.getName());
	
	private static ProjectServices instance = null;

	private static ObjectMapper mapper;
	private final HueConfig config = new HueConfig();
	private final Set<Schema> schemas = Sets.newConcurrentHashSet();
	
	protected ProjectServices() {	}
	
	public static ProjectServices getInstance() {
      if(instance == null) {
         instance = new ProjectServices();
         mapper = new ObjectMapper();
         mapper.registerModule(new HueSerDeModule());
         
         mapper.enable(SerializationFeature.INDENT_OUTPUT);
         mapper.enable(JsonParser.Feature.ALLOW_COMMENTS);
         mapper.enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);
         mapper.enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES);
      }
      return instance;
	}
	
	public HueConfig getConfig() {
		return config;
	}
	
	public Optional<Schema> getSchema(String projectName) {
		return schemas.stream().filter(s -> s.getProject().getName().equalsIgnoreCase(projectName)).findFirst();
	}
	
	public Set<Schema> getSchemas(){
		return schemas;
	}
	
	public void loadProjects() throws IOException {
		schemas.clear();
		Files.list(FileSystems.getDefault()
				.getPath(config.getProjectsDirectory()))
				.map(p ->  new File(p.toString()) )
				.filter(f -> f.isDirectory() && PersistUtils.isValidProject(f.getName()))
				.forEach(f -> {					
					try {
						logger.info("loading project " + f.getName());
						File pjJson = new File(f.getPath()+"/project.json");
						Project p = mapper.readValue(pjJson, Project.class);
						p.setFile(f);
						p.setName(f.getName());
						Schema s = new Schema(p);
						schemas.add(s);
						loadDatasources(s);
						loadExpressibles(s);
					} catch (IOException e) {
						logger.error("Could not load project in " + f.getPath() + " \n" + e.getMessage());
						e.printStackTrace();
					}
				});
	}

	private void loadDatasources(Schema s) throws IOException {
		logger.info("loading datasources from " + s.getProject().getFile().getPath());
		
		Files.list(s.getProject().getFile().toPath())
			.map(p ->  new File(p.toString()) )
			.filter(f -> {
				if(f.getName().equalsIgnoreCase("dimensions") || f.getName().equalsIgnoreCase("measures")
						|| f.getName().equalsIgnoreCase("project.json")) {
					return false;
				}
				
				if(!f.isDirectory()) {
					logger.info("skipping " + f.getName() + ". Not a datasource directory.");
					return false;
				}else if(!PersistUtils.isValidDatasource(s.getProject().getName(), f.getName())){
					logger.info("skipping " + f.getName() + ". Missing datasource.json file.");
					return false;
				}else {
					return true;
				}
			}).forEach(f -> {
				try {
					logger.info("loading datasource file " + f.getName() + " in project " + s.getProject().getName());
					File pjJson = new File(f.getPath()+"/datasource.json");
					Datasource ds = mapper.readValue(pjJson, Datasource.class);
					ds.setFile(f);
					ds.setName(f.getName());
					s.addDatasource(ds);
					loadTables(s,ds);
					loadJoins(s,ds);
					logger.info("finished loading datasource " + ds.getName());
				} catch (IOException e) {
					logger.error("Could not load project in " + f.getPath() + " \n" + e.getMessage());
				}
			});
	}

	private void loadTables(Schema s, Datasource ds) throws IOException {
		try {
			Files.list(FileSystems.getDefault().getPath(ds.getFile().toPath().toString(),"tables"))
				.map(p ->  new File(p.toString()) )
				.filter(p -> p.getName().toLowerCase().endsWith(".json"))
				.forEach(f -> {
					logger.info("\tloading table file " + f.getName() + " for ds " + ds.getName());
					try {
						Table t = mapper.readValue(f, Table.class);
						t.setFile(f);
						String[] parts = f.getName().split("\\.");
						
						if(parts.length==2) {
							t.setName(parts[0]);
						}else if(parts.length>2) {
							t.setName(parts[parts.length-2]);
						}
						logger.info("\t\t- table name: " + t.getName());
						s.addTable(ds, t);
						logger.info("\tsucceeded");
					} catch (IOException e) {
						logger.error("\tfailed - " + e.getMessage());
					}
				});
		} catch (IOException e) {
			logger.warn("no tables found for " + ds.getName() + "\n" + e.getMessage());
			Files.createDirectories(FileSystems.getDefault().getPath(ds.getFile().toPath().toString(),"tables"));
		}
	}	

	private void loadJoins(Schema s, Datasource ds) throws IOException {
		Std inj = new InjectableValues.Std().addValue("schema", s);
		inj.addValue("datasource", ds);
		
		try {
			Files.list(FileSystems.getDefault().getPath(ds.getFile().toPath().toString(),"joins"))
				.map(p ->  new File(p.toString()) )
				.filter(p -> p.getName().toLowerCase().endsWith(".json"))
				.forEach(f -> {
					logger.info("\tloading join file " + f.getName() + " for ds " + ds.getName());
					try {
						Join j = mapper.setInjectableValues(inj).readValue(f, Join.class);
						j.setFile(f);
						logger.info("\t\t- join: " + j.getName());
						s.addJoin(ds, j);
						logger.info("\tsucceeded");
					} catch (IOException e) {
						logger.error("\tfailed - " + e.getMessage());
					}
				});
		} catch (IOException e) {
			logger.warn("no joins found for " + ds.getName() + "\n" + e.getMessage());
			Files.createDirectories(FileSystems.getDefault().getPath(ds.getFile().toPath().toString(),"joins"));
		}		
	}
		
	private void loadExpressibles(Schema s) throws IOException {
		Std inj = new InjectableValues.Std().addValue("schema", s);
		
		logger.info("loading dimensions from " + s.getProject().getFile().getPath());		
		try {
			Files.list(FileSystems.getDefault().getPath(s.getProject().getFile().toPath().toString(),"dimensions"))
				.map(p ->  new File(p.toString()) )
				.filter(p -> p.getName().toLowerCase().endsWith(".json"))
				.forEach(f -> {
					logger.info("\tloading dimension file " + f.getName() + " for project " + s.getProject().getName());
					try {
						Dimension d = mapper.setInjectableValues(inj).readValue(f, Dimension.class);
						d.setFile(f);
						d.setName(f.getName().split("\\.")[0]);
						logger.info("\t\t- dimension: " + d.getName());
						s.addDimension(d);
						logger.info("\tsucceeded");
					} catch (IOException e) {
						logger.error("\tfailed - " + e.getMessage());
					}
				});
		} catch (IOException e) {
			logger.warn("no dimensions found for " + s.getProject().getName() + "\n" + e.getMessage());
			Files.createDirectories(FileSystems.getDefault().getPath(s.getProject().getFile().toPath().toString(),"dimensions"));
		}		
		logger.info("finished loading dimensions from " + s.getProject().getFile().getPath());
		
		logger.info("loading measures from " + s.getProject().getFile().getPath());		
		try {
			Files.list(FileSystems.getDefault().getPath(s.getProject().getFile().toPath().toString(),"measures"))
				.map(p ->  new File(p.toString()) )
				.filter(p -> p.getName().toLowerCase().endsWith(".json"))
				.forEach(f -> {
					logger.info("\tloading measure file " + f.getName() + " for project " + s.getProject().getName());
					try {
						Measure d = mapper.setInjectableValues(inj).readValue(f, Measure.class);
						d.setFile(f);
						d.setName(f.getName().split("\\.")[0]);
						logger.info("\t\t- meaure: " + d.getName());
						s.addMeasure(d);
						logger.info("\tsucceeded");
					} catch (IOException e) {
						logger.error("\tfailed - " + e.getMessage());
					}
				});
		} catch (IOException e) {
			logger.warn("no measures found for " + s.getProject().getName() + "\n" + e.getMessage());
			Files.createDirectories(FileSystems.getDefault().getPath(s.getProject().getFile().toPath().toString(),"measures"));
		}		
		logger.info("finished loading measures from " + s.getProject().getFile().getPath());
	}

	public void save(Schema s, Datasource datasource, Table t) throws ServiceException {
		
		try {
			if(t.getFile() == null) {
				t.setFile(new File(datasource.getFile().toString()+"/tables/"+ String.join(".", t.getPhysicalNameSegments()) +".json"));
			}
			mapper.writeValue(t.getFile(), t);
		} catch (IOException e) {
			throw new ServiceException("Unable to save: \n" + e.getMessage());
		}
		
	}

	public void save(Schema s, Datasource datasource, Join j) throws ServiceException {
		try {
			if(j.getFile() == null) {
				j.setFile(new File(datasource.getFile().toString()+"/joins/"+ j.getName() +".json"));
			}
			mapper.writeValue(j.getFile(), j);
		} catch (IOException e) {
			throw new ServiceException("Unable to save: \n" + e.getMessage());
		}
		
	}
	
	public void save(Schema s, Dimension d) throws ServiceException {
		try {
			if(d.getFile() == null) {
				d.setFile(new File(s.getProject().getFile().toString()+"/dimensions/"+ d.getName() +".json"));
			}
			mapper.writeValue(d.getFile(), d);
		} catch (IOException e) {
			throw new ServiceException("Unable to save: \n" + e.getMessage());
		}		
	}

	public void save(Schema s, Measure msr) throws ServiceException {
		try {
			if(msr.getFile() == null) {
				msr.setFile(new File(s.getProject().getFile().toString()+"/measures/"+ msr.getName() +".json"));
			}
			mapper.writeValue(msr.getFile(), msr);
		} catch (IOException e) {
			throw new ServiceException("Unable to save: \n" + e.getMessage());
		}		
	}
	
}
