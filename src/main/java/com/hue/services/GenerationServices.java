package com.hue.services;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.hue.common.DataType;
import com.hue.model.Column;
import com.hue.model.Datasource;
import com.hue.model.Dimension;
import com.hue.model.FieldExpression;
import com.hue.model.Join;
import com.hue.model.Measure;
import com.hue.model.Table;
import com.hue.model.persist.PersistUtils;
import com.hue.model.persist.ProjectServices;
import com.hue.model.persist.Schema;
import com.hue.utils.CommonUtils;

public class GenerationServices {
	private static final Logger logger = LoggerFactory.getLogger(GenerationServices.class.getName());
	private GenerationServices() {};
	
	public static void genSchema(
			Schema s, 
			Datasource datasource, 
			boolean updateTables, 
			boolean genDims, 
			boolean genMeasures,
			boolean skipRowCounts,
			boolean createJoins) throws ServiceException {
		
		List<Table> tables = SchemaImportService.importTablesForSchemas(datasource, 
				SchemaImportService.importSchemas(datasource));
		
		tables.parallelStream().forEach(t ->{
			try {
				SchemaImportService.importColumnsForTable(datasource, t);
				int row = -1;
				if(!skipRowCounts) {
					try {
						row = SchemaImportService.getRowCount(datasource, t.getSchemaName(), t.getName());
						t.setRowCount(row);
					}catch(Exception e) { logger.warn("failed to get row count for " + t.getName()); }					
				}
				
				// Find all tables with same physical name
				List<Table> tmatches = s.getTables(datasource).stream()
							.filter(tbl -> tbl.getPhysicalName().equalsIgnoreCase(t.getPhysicalName()) 
											&& tbl.getSchemaName().equalsIgnoreCase(t.getSchemaName()))
							.collect(Collectors.toList());
				
				for(Table orig : tmatches) {
					if( updateTables) {
						logger.debug("Updating table " + t.getName());
						List<Column> custCols = orig.getColumns().stream()
							.filter(c -> !CommonUtils.isBlank(c.getSql()))
							.collect(Collectors.toList());
						
						orig.getColumns().clear();
						orig.setColumns(t.getColumns());
						orig.getColumns().addAll(custCols);
						if(row != -1)
							orig.setRowCount(row);
						
						ProjectServices.getInstance().save(s,datasource, orig);
					}else {
						s.addTable(datasource, t);
						logger.debug("Generating table " + t.getName());
						ProjectServices.getInstance().save(s,datasource, t);
					}
				}			
				
			} catch (ServiceException e) {
				throw new RuntimeException(e);
			}
		});
		
		if(createJoins) {
			List<Join> joins = SchemaImportService.getJoinDefs(tables);
			joins.parallelStream().forEach(j -> {
				if(!s.getJoins(datasource).contains(j)) {
					s.addJoin(datasource, j);
					try {
						ProjectServices.getInstance().save(s,datasource, j);
					} catch (ServiceException e) {
						logger.error("could not save join " +j.getName() + e.getMessage());
					}
				};
			});
		}
		
		if(genDims) genDims(s, datasource);
		if(genDims) genMeasures(s, datasource);
	}
	
	public static void genDims(Schema s, Datasource ds) {
		logger.info("Generating dimensions...");
		s.getTables(ds).stream().forEach(t -> {
			t.getColumns().stream().forEach(c -> {
				
				if((c.getDataType() == DataType.INTEGER && !c.getName().toLowerCase().contains("id"))
						|| c.getDataType() == DataType.DECIMAL) {
					return ;
				}
				
				String nm = PersistUtils.deriveExpressibleName(c.getName(), t.getName(), true);
				logger.info("Pre check dimension " + nm );
				Optional<Dimension> dOpt = s.getDimension(nm);
				if(!dOpt.isPresent()) {
					logger.info("Creating new dimension " + nm);
					Dimension dim = new Dimension();
					dim.setName(nm);
					String sql = c.getSql();
					if(sql == null) sql = c.getName();
					
					Set<Table> st = Sets.newHashSet();
					st.add(t);
					FieldExpression fe = new FieldExpression(sql, st);
					dim.addExpression(fe);
					s.addDimension(dim);
					
					try {
						ProjectServices.getInstance().save(s, dim);
					} catch (ServiceException e) {
						throw new RuntimeException(e);
					}
				}else {
					logger.info("Dimension " + nm + " already exists");
					Dimension dim = dOpt.get();
					Optional<FieldExpression> fe = dim.getExpression(t);
					if(!fe.isPresent()) {
						logger.info("Updating expressions for " + nm );
						String sql = c.getSql();
						if(sql == null) sql = c.getName();
						
						Set<Table> st = Sets.newHashSet();
						st.add(t);
						FieldExpression fen = new FieldExpression(sql, st);
						dim.addExpression(fen);
						
						try {
							ProjectServices.getInstance().save(s, dim);
						} catch (ServiceException e) {
							throw new RuntimeException(e);
						}
					}else {
						logger.info("Expression for table " + t.getName() + " already exists in " + nm);
					}
				}
			});
		});	
		
		logger.info("Finished generating dimensions...");
	}
	
	public static void genMeasures(Schema s, Datasource ds) {
		logger.info("Generating measures...");
		s.getTables(ds).stream().forEach(t -> {
			t.getColumns().stream().forEach(c -> {
				String func = "sum";
				
				if(!c.getDataType().equals(DataType.INTEGER) && !c.getDataType().equals(DataType.DECIMAL) ) {
					return ;
				}
				
				String nm = PersistUtils.deriveExpressibleName(c.getName(), t.getName(), true);
				if(c.getName().toLowerCase().contains("id")) {
					func = "count";
					nm = "# " + nm;
				}				
				
				logger.info("Pre check measure " + nm );
				Optional<Measure> dOpt = s.getMeasure(nm);
				if(!dOpt.isPresent()) {
					logger.info("Creating new measure " + nm);
					Measure msr = new Measure();
					msr.setName(nm);
					String sql = c.getSql();
					if(sql == null) sql = c.getName();
					
					Set<Table> st = Sets.newHashSet();
					st.add(t);
					FieldExpression fe = new FieldExpression(func+"("+sql+")", st);
					msr.addExpression(fe);
					s.addMeasure(msr);
					
					try {
						ProjectServices.getInstance().save(s, msr);
					} catch (ServiceException e) {
						throw new RuntimeException(e);
					}
				}else {
					logger.info("Measure " + nm + " already exists");
					Measure msr = dOpt.get();
					Optional<FieldExpression> fe = msr.getExpression(t);
					if(!fe.isPresent()) {
						logger.info("Updating expressions for " + nm );
						String sql = c.getSql();
						if(sql == null) sql = c.getName();
						
						Set<Table> st = Sets.newHashSet();
						st.add(t);
						FieldExpression fen = new FieldExpression(func+"("+sql+")", st);
						msr.addExpression(fen);
						
						try {
							ProjectServices.getInstance().save(s, msr);
						} catch (ServiceException e) {
							throw new RuntimeException(e);
						}
					}else {
						logger.info("Expression for table " + t.getName() + " already exists in " + nm);
					}
				}
			});
		});	
		
		logger.info("Finished generating measures...");
	}

	public static void genSchema(
			Schema s, 
			String dsName, 
			boolean updateTables, 
			boolean genDims, 
			boolean genMeasures, 
			boolean skipRowCounts,
			boolean genJoins) throws ServiceException {
		
		Optional<Datasource> dsO = s.getDatasource(dsName);
		if(!dsO.isPresent()) {
			throw new ServiceException("Datasource " + dsName + " not found in project " + s.getProject().getName());
		}
		
		genSchema(s, s.getDatasource(dsName).get(), updateTables, genDims, genMeasures, skipRowCounts, genJoins);
	}
}
