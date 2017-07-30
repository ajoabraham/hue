package com.hue.translator.sql;

import static com.vero.server.engine.sql.SqlEngineUtils.emulateFullJoin;
import static com.vero.server.engine.sql.SqlEngineUtils.emulateWindowFunction;
import static com.vero.server.engine.sql.SqlEngineUtils.extractColumns;
import static com.vero.server.engine.sql.SqlEngineUtils.extractVeroIdents;
import static com.vero.server.engine.sql.SqlEngineUtils.findBlockDerivedDimension;
import static com.vero.server.engine.sql.SqlEngineUtils.findBlockDerivedMeasure;
import static com.vero.server.engine.sql.SqlEngineUtils.findExpressionReference;
import static com.vero.server.engine.sql.SqlEngineUtils.findSourceBlock;
import static com.vero.server.engine.sql.SqlEngineUtils.isDataBlockEmpty;
import static com.vero.server.engine.sql.SqlEngineUtils.isFullJoinPresent;
import static com.vero.server.engine.sql.SqlEngineUtils.joinTypeToString;
import static com.vero.server.engine.sql.SqlEngineUtils.patchJoinDef;
import static com.vero.server.engine.sql.SqlEngineUtils.patchWindowFunctionSumSum;
import static com.vero.server.engine.sql.SqlEngineUtils.replaceFirstAvoidOutsideQuotes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.antlr.runtime.tree.CommonTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.tree.Node;
import com.google.common.base.Joiner;
import com.vero.common.constant.DBType;
import com.vero.common.constant.JoinType;
import com.vero.common.constant.OrderByType;
import com.vero.common.constant.VeroType;
import com.vero.common.sql.DDLGenerator;
import com.vero.common.sql.DataType;
import com.vero.common.sql.SetBlockParser;
import com.vero.common.sql.SetBlockParser.SetBlockContent;
import com.vero.common.sql.parser.AggregateAnalyzer;
import com.vero.common.sql.parser.AggregateFunctions;
import com.vero.common.sql.parser.SemanticException;
import com.vero.common.sql.parser.VeroSqlParser;
import com.vero.common.sql.parser.nodeextractor.EnhancedNodeExtractor;
import com.vero.common.sql.parser.nodeextractor.ExtractResult;
import com.vero.common.util.CommonUtils;
import com.vero.model.Column;
import com.vero.model.Dimension;
import com.vero.model.Expressible;
import com.vero.model.Expression;
import com.vero.model.ExpressionRef;
import com.vero.model.IJoinable;
import com.vero.model.Measure;
import com.vero.model.Table;
import com.vero.model.VeroBase;
import com.vero.model.VeroObj;
import com.vero.model.filter.FilterNode;
import com.vero.model.graph.GraphException;
import com.vero.model.report.BlockDerivedDimension;
import com.vero.model.report.BlockDerivedEntity;
import com.vero.model.report.BlockDerivedMeasure;
import com.vero.model.report.BlockFilter;
import com.vero.model.report.BlockRef;
import com.vero.model.report.BlockToBlockJoin;
import com.vero.model.report.DataBlock;
import com.vero.model.report.DatasetException;
import com.vero.model.report.EngineBlock;
import com.vero.model.report.FilterExpression;
import com.vero.model.report.FinalBlock;
import com.vero.model.report.IGroupable;
import com.vero.model.report.IJoinExpression;
import com.vero.model.report.IQueryPlan;
import com.vero.model.report.LoadBlock;
import com.vero.model.report.QueryBlock;
import com.vero.model.report.Report;
import com.vero.model.report.ResultBlock;
import com.vero.model.report.SetBlock;
import com.vero.model.util.FormulaTokenizer;
import com.vero.model.util.FormulaUtils;
import com.vero.server.engine.CoalesceJoinable;
import com.vero.server.engine.MultiBlockPlan;
import com.vero.server.engine.OptimizedPlan;
import com.vero.server.engine.graph.QEException;
import com.vero.server.engine.graph.virtual.VirtualJoinable;
import com.vero.server.engine.sql.SqlEngineContext.BlockContext;
import com.vero.server.engine.sql.SqlEngineContext.FilterContext;
import com.vero.server.engine.sql.SqlEngineContext.SelectItem;
import com.vero.server.engine.sql.TreePatcher.PatchByAppendingFuncReturnType;
import com.vero.server.engine.sql.formatter.FormatterFactory;
import com.vero.server.engine.sql.vdb.VirtualDB;

public class SqlEngine {
    private static final Logger logger = LoggerFactory.getLogger(SqlEngine.class);
    
	public static class VeroItem {
		private VeroType _type;
		private VeroType _parentType;
		private String _name;
		private String _replacement = null;

		public VeroItem(VeroType type, VeroType parentType, String name) { _type = type; _parentType = parentType; _name = name; }
		public VeroType getType() { return _type; }
		public VeroType getParentType() { return _parentType; }
		public String getName() { return _name; }
		public void setReplacement(String replacement) { _replacement = replacement; }
		public String getReplacement() { return _replacement; }

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("VeroItem==> ").append("ParentType: " + _parentType).append(", Type: " + _type).append(", Name: " + _name);
			return sb.toString();
		}
	}
	
	/* now the key is a compound key built as "PhysicalName"_"Name"
	 */
    private class TableAlias {
        private Map<String, String> _aliasMap = new HashMap<String, String>();
        private final String _prefix = "t";
        private int _count = 0;

        public String assignAlias(IJoinable joinable) {
            String key = joinable.getPhysicalName().concat(":").concat(joinable.getName());
            
            String alias = _aliasMap.get(key);
            
            if (alias == null) {
                alias = new String(_prefix + _count);
                _aliasMap.put(key, alias);
                _count++;
            }
            
            return alias;
        }
                
        /*
         * if a single string is giving, we will use it twice to build the compound key
         */
        public String assignAlias(String key) {
            String compoundKey = key.concat(":").concat(key);
            
            String alias = _aliasMap.get(compoundKey);

            if (alias == null) {
                alias = new String(_prefix + _count);
                _aliasMap.put(compoundKey, alias);
                _count++;
            }

            // System.out.println("TableAlias.assignAlias: key = " + key + " value = " + value);

            return alias;
        }

        public Map<String, String> getAlias() {
            return _aliasMap;
        }
    }

    private class ColumnAlias {
        private Map<String, Integer> _aliasMap = new HashMap<String, Integer>();

        public String assignAlias(String key) {
            Integer value = _aliasMap.get(key);
            String retString = new String();

            if (value == null) {
                value = new Integer(0);
                _aliasMap.put(key, value);
                // when there is only one element, don't append serial
                retString = key;
            } else {
                int valueSize = value.toString().length();
                
                retString = key.substring(0, key.length()-valueSize).concat(value.toString());
                //retString = key.concat(value.toString());
            }

            value = new Integer(value.intValue()+1);
            _aliasMap.put(key, value);

            return retString;
        }
    }

    private class CoalesceMap {
        private class CoalesceInfo {
            LinkedHashSet<DataBlock> _blocks = null;
            LinkedHashSet<String> _names = null;
        }

        // this map maps from expressible to CoalesceInfo, this is main map that should be operated on
        private Map<Expressible, CoalesceInfo> _expMap = new HashMap<Expressible, CoalesceInfo>();
        // this map maps from expressible.getName() to CoalesceInfo, this is supplementary
        private Map<String, CoalesceInfo> _nameMap = new HashMap<String, CoalesceInfo>();

        public void addBlock(Expressible expressible, DataBlock block) {
            if (_expMap.containsKey(expressible)) {
                CoalesceInfo coalesceInfo = _expMap.get(expressible);
                coalesceInfo._blocks.add(block);
            } else {
                CoalesceInfo coalesceInfo = new CoalesceInfo();
                coalesceInfo._blocks = new LinkedHashSet<DataBlock>();
                coalesceInfo._blocks.add(block);
                coalesceInfo._names = new LinkedHashSet<String>();
                _expMap.put(expressible, coalesceInfo);
            }
            
            if (_nameMap.containsKey(expressible.getName())) {
                CoalesceInfo coalesceInfo = _nameMap.get(expressible.getName());
                coalesceInfo._blocks.add(block);
            } else {
                CoalesceInfo coalesceInfo = new CoalesceInfo();
                coalesceInfo._blocks = new LinkedHashSet<DataBlock>();
                coalesceInfo._blocks.add(block);
                coalesceInfo._names = new LinkedHashSet<String>();
                _nameMap.put(expressible.getName(), coalesceInfo);
            }
        }

        public void addName(Expressible expressible, String name) {
            if (_expMap.containsKey(expressible)) {
                CoalesceInfo coalesceInfo = _expMap.get(expressible);
                Boolean found = false;
                
                for (String item : coalesceInfo._names) {
                    if (item.replaceAll("\"", "").equals(name.replaceAll("\"", ""))) {
                        found = true;
                        break;
                    }
                }
                
                if (found == false) {
                    coalesceInfo._names.add(name);
                }
            }
            
            if (_nameMap.containsKey(expressible.getName())) {
                CoalesceInfo coalesceInfo = _nameMap.get(expressible.getName());
                Boolean found = false;
                
                for (String item : coalesceInfo._names) {
                    if (item.replaceAll("\"", "").equals(name.replaceAll("\"", ""))) {
                        found = true;
                        break;
                    }
                }
                
                if (found == false) {
                    coalesceInfo._names.add(name);
                }
            }
        }

        public LinkedHashSet<DataBlock> getBlocks(Expressible expressible) {
            if (_expMap.containsKey(expressible)) {
                CoalesceInfo coalesceInfo = _expMap.get(expressible);
                return coalesceInfo._blocks;
            } else {
                return null;
            }
        }

        public LinkedHashSet<DataBlock> getBlocks(String name) {
            if (_nameMap.containsKey(name)) {
                CoalesceInfo coalesceInfo = _nameMap.get(name);
                return coalesceInfo._blocks;
            } else {
                return null;
            }
        }
        
        public LinkedHashSet<String> getNames(Expressible expressible) {
            if (_expMap.containsKey(expressible)) {
                CoalesceInfo coalesceInfo = _expMap.get(expressible);
                return coalesceInfo._names;
            } else {
                return null;
            }
        }

        public LinkedHashSet<String> getNames(String name) {
            if (_nameMap.containsKey(name)) {
                CoalesceInfo coalesceInfo = _nameMap.get(name);
                return coalesceInfo._names;
            } else {
                return null;
            }
        }
        
        public Boolean containsExpressible(Expressible expressible) {
            return _expMap.containsKey(expressible);
        }
        
        public Boolean containsExpressibleName(String expressibleName) {
            return _nameMap.containsKey(expressibleName);
        }

        public Set<Expressible> keySet() {
            return _expMap.keySet();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("CoalesceMap ExpressibleMap==> ").append("# of sets: ").append(_expMap.size()).append("\n");
            for (Expressible expressible : _expMap.keySet()) {
                sb.append("  ExpressibleName==> ").append(expressible.getName()).append("\n");
                CoalesceInfo coalesceInfo = _expMap.get(expressible);
                for (DataBlock block : coalesceInfo._blocks) {
                    sb.append("    Blocks==> ").append(block.getPhysicalName()).append("\n");
                }

                for (String name : coalesceInfo._names) {
                    sb.append("    Names==> ").append(name).append("\n");
                }
            }

            sb.append("CoalesceMap NameMap==> ").append("# of sets: ").append(_nameMap.size()).append("\n");
            for (String name : _nameMap.keySet()) {
                sb.append("  Name==> ").append(name).append("\n");
                CoalesceInfo coalesceInfo = _nameMap.get(name);
                for (DataBlock block : coalesceInfo._blocks) {
                    sb.append("    Blocks==> ").append(block.getPhysicalName()).append("\n");
                }

                for (String itemName : coalesceInfo._names) {
                    sb.append("    Names==> ").append(itemName).append("\n");
                }
            }
            
            return sb.toString();
        }
    }

    // 20150217 ToDo: move global variables into SqlEngineContext
    private SqlEngineContext sqlEngineContext = new SqlEngineContext();

    private Map<String, Map<String, String>> blockAliasMap = new HashMap<String, Map<String, String>>();
    private Queue<VeroItem> engineBlockQueue = new LinkedList<VeroItem>();
    private Queue<VeroItem> queryBlockQueue = new LinkedList<VeroItem>();
    private List<String> blockTempName = new ArrayList<String>();
    
    private Boolean autoResetEngine = true;

    public SqlEngine(VirtualDB virtualDB) { sqlEngineContext.setVirtualDB(virtualDB); }
    public SqlEngine(DBType dbType) { sqlEngineContext.setVirtualDB(new VirtualDB(dbType)); }
	public SqlEngine() {}
	public SqlEngineContext getSqlEngineContext() { return sqlEngineContext; }
	public void disableAutoResetEngine() { autoResetEngine = false; }
	
	private void resetEngine() {
	    if (sqlEngineContext != null) {
	        sqlEngineContext.resetContext();
	    }
	    blockAliasMap.clear();
	    engineBlockQueue.clear();
	    queryBlockQueue.clear();
	    blockTempName.clear();
	}

	public void genSql(Report report) throws Exception {
	    for (QueryBlock queryBlock : report.getQueryBlocks()) {
	    	if(queryBlock.isFreeSql()) continue;

	        for (SetBlock setBlock : queryBlock.getSetBlocks()) {
	            genSqlForBlockRecursively(report, setBlock, queryBlock);
	        }

	        for (EngineBlock engineBlock : queryBlock.getEngineBlocks()) {
	            genSqlForBlockRecursively(report, engineBlock, queryBlock);
	        }

	        genSqlString(report, queryBlock);

	        for (LoadBlock loadBlock : queryBlock.getLoadBlocks()) {
	            /*
    			Map<String, String> selectAliases = blockAliasMap.get(loadBlock.getSource().getPhysicalName());
    			LinkedHashMap<String, DataType> tableItems = new LinkedHashMap<String, DataType>();
    			for (String key : selectAliases.keySet()) {
    				String alias = selectAliases.get(key);
    				tableItems.put(alias, new DataType("DYNAMIC"));
    			}
        		DDLGenerator ddlGen = new DDLGenerator();
        		DBType sourceType = loadBlock.getSource().getDatasource().getDatabaseType();
        		DBType targetType = loadBlock.getTargetDatasource().getDatabaseType();

        		// TODO: 20160616: need to fix the ddl part which uses physical name
        		String outSql = ddlGen.genSql(queryBlock.getPhysicalName(), tableItems, sourceType, targetType);
        		loadBlock.setTargetDDL(outSql);
        		*/
	            
                blockTempName.add(queryBlock.getPhysicalName());
	        }

	        // 20150213: drop this query block which has load block
	        FinalBlock finalBlock = null;
	        if (queryBlock.getFinalBlock() == null) {
	            finalBlock = new FinalBlock(queryBlock);
	        } else {
	            finalBlock = queryBlock.getFinalBlock();
	        }

	        if (queryBlock.getLoadBlocks().size() > 0) {
    			queryBlock.setFinalBlock(finalBlock);
    			Queue<VeroItem> queryBlockQueue = new LinkedList<VeroItem>();
    			queryBlockQueue.add(new VeroItem(VeroType.TABLE, null, queryBlock.getPhysicalName()));
                genSqlFinalBlock(queryBlockQueue, finalBlock);
    		}
	    }

	    ResultBlock resultBlock = report.getResultBlock();
	    if(resultBlock.isFreeSql()) return;
	    	
        for (SetBlock setBlock : resultBlock.getSetBlocks()) {
            genSqlForBlockRecursively(report, setBlock, resultBlock);
        }

        for (EngineBlock engineBlock : resultBlock.getEngineBlocks()) {
            genSqlForBlockRecursively(report, engineBlock, resultBlock);
        }

	    genSqlString(report, resultBlock);

	    if (autoResetEngine == true) {
	    	resetEngine();
	    }
	}

	private void genSqlForBlockRecursively(
        Report report,
        EngineBlock dataBlock,
        QueryBlock masterBlock) throws Exception {
		List<EngineBlock> childBlocks = dataBlock.getChildren();

		for (EngineBlock childBlock : childBlocks) {
		    // 20141120: currently, only SetBlock may have child blocks
			genSqlForBlockRecursively(report, childBlock, masterBlock);
		}

	    if (dataBlock instanceof SetBlock) {
	        patchSetFilterForMasterBlock(dataBlock, masterBlock);
	    }

	    genSqlString(report, dataBlock);
	}
	
    private void patchSetFilterForMasterBlock(
        EngineBlock filterBlock,
        QueryBlock masterBlock) throws GraphException, DatasetException {
        BlockFilter originalFilter = masterBlock.getBlockFilter();
        BlockFilter setFilter = filterBlock.getBlockFilter();
        int setFilterSerial = -1;

        search:
        for (FilterNode<Expression> childFilterNode : setFilter.getRoot().getChildren()) {
            if (childFilterNode.getData() != null) {
                for (ExpressionRef<? extends VeroBase> expRef : childFilterNode.getData().getReferences()) {
                    if (expRef.getReference() instanceof Measure) {
                        // match the first measure
                        setFilterSerial = childFilterNode.getSerial();
                        break search;
                    }
                }
            }
        }
        
        if (setFilterSerial != -1) {
            FilterNode<Expression> foundNode = originalFilter.findNode(setFilterSerial);

            if (foundNode != null) {
                int i = 0;
                FilterExpression expr = new FilterExpression();
                String pattern = "@[".concat(filterBlock.getPhysicalName()).concat("]");
                String formula = "include(".concat(pattern).concat(")");
                
                List<Dimension> selectDims = filterBlock.getSelectDimensions();
                for (Dimension curDim : selectDims) {
                    if (i>0) {
                        formula = formula.concat(",");
                    } else if (i==0) {
                        formula = formula.concat(" on ");
                    }
                    pattern = "@[".concat(curDim.getName()).concat("]");
                    formula = formula.concat(pattern);
                    i++;
                }

                expr.setFormula(formula);
                expr.setQueryBlock(masterBlock);
                FormulaTokenizer.tokenizeExpression(expr);

                Map<Integer, FilterExpression> patchedResult = originalFilter.getPatchedResult();
                Integer key = new Integer(setFilterSerial);
                FilterExpression mapExp = patchedResult.get(key);
                if (mapExp == null) {
                    patchedResult.put(key, expr);
                }
            }
        }
    }
	
	// generate sql for each block
    private void genSqlString(
        Report report,
        DataBlock dataBlock) throws Exception {
        // enter new block at first
        sqlEngineContext.enterNewBlock(dataBlock);

        // start using the sqlEngineContext
        VirtualDB virtualDB = sqlEngineContext.getVirtualDB();
        DBType dbType = virtualDB.getDbType();

	    TableAlias curTableAlias = new TableAlias();
	    String finalSql = "";
	    Boolean needReaggregate = false;
	    Boolean needGroupby = false;
	    List<LinkedHashSet<String>> listCoalesceSets = new ArrayList<LinkedHashSet<String>>();
	    String blockName = dataBlock.getPhysicalName();

	    String select = null;
	    String from = null;
	    String where = null;
	    String groupby = null;
	    String having = null;
	    String orderby = null;
    	List<String> allJoins = new ArrayList<String>();
    	List<String> hiddenSqls = new ArrayList<String>();

	    logger.debug("##### Generating sql for block: " + blockName + " " + " DBType: " + dbType + " " + dataBlock.getClass());
	    if (dataBlock.getPlan() == null) {
    		// no plan
    		return;
	    }

	    if (!blockAliasMap.containsKey(dataBlock.getPhysicalName())) {
	        blockAliasMap.put(dataBlock.getPhysicalName(), new LinkedHashMap<String, String>());
	    }
	    if (sqlEngineContext.getBlockContext(dataBlock.getPhysicalName()) == null) {
	        sqlEngineContext.addBlockContext(dataBlock.getPhysicalName());
	    }

	    // determine if re-aggregate is needed
        needReaggregate = needReaggregate(dataBlock);

        // generate selection order
        List<Expressible> selectProcessingOrder = SqlEngineUtils.genSelectionProcessingOrder(dataBlock.getSelections());
        
        select = genSelectString(dataBlock, selectProcessingOrder, curTableAlias, needReaggregate);
        from = genFromString(report, dataBlock, curTableAlias, allJoins, listCoalesceSets);
        StringBuilder fromBuilder = new StringBuilder(from);
        where = genWhereString(report, dataBlock, curTableAlias, needReaggregate, fromBuilder);
        from = fromBuilder.toString();
        having = genHavingString(dataBlock, curTableAlias, needReaggregate);
        
        needGroupby = needsGroupBy(dataBlock, having);

        BlockContext blockContext = sqlEngineContext.getBlockContext(sqlEngineContext.getCurrentDataBlock().getPhysicalName());

        // retouch phase: re-patch window function sum(sum()) situation (when there is group-by)
        if (needGroupby == true) {
    		// check if window function has nested agg functions
    		if (blockContext.isWindowFunctionPresent()) {
    			for (SelectItem selectItem : blockContext.getSelectItems()) {
    				if (selectItem.isWindowFunction()) {
    					System.out.println("Re-patch window function...");
    					String originalStr = selectItem.getKey();
    					String modifiedStr = patchWindowFunctionSumSum(originalStr);
    					if (!modifiedStr.equals(originalStr)) {
    						selectItem.setKey(modifiedStr);
    					}
    				}
    			}
    		}
        }
        
        // retouch phase: remove nested aggregate functions
        for (SelectItem selectItem : blockContext.getSelectItems()) {
            if (!selectItem.isWindowFunction()) {
                String originalStr = selectItem.getKey();
                CommonTree srcTree = VeroSqlParser.parseExpression(originalStr);
                originalStr = translateSql(null, VeroSqlParser.createExpression(srcTree));
                srcTree = TreePatcher.removeAggFunc(srcTree, AggregateFunctions.getAggregateFunctions(), 2);
                String patched = translateSql(null, VeroSqlParser.createExpression(srcTree));
                if (originalStr.equals(patched)) {
                    // do nothing
                } else {
                    String pattern = Pattern.quote(originalStr.replaceAll("\"", ""));
                    String replacement = patched.replaceAll("\"", "");

                    if (where != null) { where = where.replaceAll(pattern, replacement); }
                    if (having != null) { having = having.replaceAll(pattern, replacement); }
                    selectItem.setKey(patched);
                }
            }
        }
        
        // retouch phase: remove extra aggreagate functions
        if (needReaggregate == false) {
            for (SelectItem selectItem : blockContext.getSelectItems()) {
                VeroType originalType = selectItem.getOriginalType();
                VeroType type = selectItem.getType();
                if (!selectItem.isWindowFunction()) {
                    if ((originalType == VeroType.VIRTUAL_MEASURE) || (originalType == VeroType.BLOCK_DERIVED_MEASURE) || (originalType == VeroType.MEASURE)) {
                        if (type == VeroType.DIMENSION) {
                            String originalStr = selectItem.getKey();
                            CommonTree srcTree = VeroSqlParser.parseExpression(originalStr);
                            originalStr = translateSql(null, VeroSqlParser.createExpression(srcTree));
                            srcTree = TreePatcher.removeAggFunc(srcTree, AggregateFunctions.getAggregateFunctions(), 1);
                            String patched = translateSql(null, VeroSqlParser.createExpression(srcTree));
                            if (originalStr.equals(patched)) {
                                // do nothing
                            } else {
                                String pattern = Pattern.quote(originalStr.replaceAll("\"", ""));
                                String replacement = patched.replaceAll("\"", "");

                                if (where != null) { where = where.replaceAll(pattern, replacement); }
                                if (having != null) { having = having.replaceAll(pattern, replacement); }
                                selectItem.setKey(patched);
                            }
                        }
                    }
                }
            }
        }

        // retouch phase: for hive, add aggreagates from window function into select list
        if (dbType == DBType.HIVE) {
            // check if window function has nested agg functions
            if (blockContext.isWindowFunctionPresent()) {
                List<SelectItem> newAddonSeleteItems = new ArrayList<SelectItem>();
                List<String> newAddonStrings = new ArrayList<String>();

                for (SelectItem selectItem : blockContext.getSelectItems()) {
                    if (selectItem.isWindowFunction()) {
                        System.out.println("Examine window function...");
                        String originalStr = selectItem.getKey();
                        ExtractResult extractResult = EnhancedNodeExtractor.extract(originalStr);
                        List<String> items = new ArrayList<String>(extractResult.getOrderByKeysOnlyNames());
                        items.addAll(extractResult.getPartitionByKeysOnlyNames());
                        AggregateAnalyzer aggAnalyzer = new AggregateAnalyzer();
                        for (String item : items) {
                            try {
                                aggAnalyzer.visitTree(item);
                                if (aggAnalyzer.getAggregates().size() > 0) {
                                    //System.out.println("Need to add this to select list==> " + item);
                                    //SelectItem si = sqlEngineContext.new SelectItem("0");
                                    SelectItem si = sqlEngineContext.new SelectItem(null);
                                    si.setKey(item);
                                    si.setType(VeroType.MEASURE);
                                    newAddonSeleteItems.add(si);
                                    newAddonStrings.add(item);
                                }
                            } catch (SemanticException se) {
                                // can't happen here
                                System.out.println(se);
                            }
                        }
                    }
                }
                blockContext.getSelectItems().addAll(newAddonSeleteItems);
            }
        }

        // retouch phase: patch coalesce from block join
        if (listCoalesceSets.size() > 0) {
            System.out.println("Re-patch coalesce...");

            for (SelectItem selectItem : blockContext.getSelectItems()) {
                Object obj = sqlEngineContext.findObjectById(selectItem.getId());
                if (obj instanceof Expressible) {
                    String selectString = selectItem.getKey();
                    System.out.println("Original selectItem==> " + selectString);
                    CommonTree srcTree = VeroSqlParser.parseExpression(selectString);
                    srcTree = TreePatcher.recursivelyPatchCoalesce(srcTree, listCoalesceSets);
                    String patched = translateSql(null, VeroSqlParser.createExpression(srcTree));
                    System.out.println("Patched selectItem==> " + patched);
                    selectItem.setKey(patched);
                }
            }
        }

        // generate select from SqlEngineContext
        // the selectItems are ordered by the time of processing by vero which may be different from user's preference so we need to order it again
        List<SelectItem> selectItems = new ArrayList<SelectItem>();
        for (Expressible userExpressible : dataBlock.getSelections()) {
            for (Expressible veroExpressible :  selectProcessingOrder) {
                if (userExpressible == veroExpressible) {
                    // since the order of selectProcessingOrder will be the same as that of SelectItem, we can just get index of it
                    int index = selectProcessingOrder.indexOf(veroExpressible);
                    selectItems.add(blockContext.getSelectItems().get(index));
                    break;
                }
            }
        }
        
        List<String> selectStrings = new ArrayList<String>();
        Joiner selectJoiner = Joiner.on(", ").skipNulls();

        selectItems.forEach(selectItem -> selectStrings.add(selectItem.getKey().concat(" ").concat(selectItem.getAlias())));
        select = genCustomString("SELECT", selectJoiner.join(selectStrings));

        // generate groupby
        if (needGroupby == false) {
            groupby =  "";
        } else {
            groupby = genGroupByString(dataBlock);
        }
        // generate orderby
        orderby = genOrderByString(dataBlock);
        String firstPassSqlString = select.concat(" ").concat(from).concat(" ").concat(where).concat(" ").concat(groupby).concat(" ").concat(having).concat(orderby);

        sqlEngineContext.printContent(dataBlock.getPhysicalName());

        String tempSqlString = firstPassSqlString;
        String curSqlString = null;
        List<VeroItem> cleanupItems = new ArrayList<VeroItem>();
        List<String> allSqls = dataBlock.getSqls();
        Boolean isEmulationPerformed = false;

        if (isFullJoinPresent(dataBlock) && (virtualDB.supportsFullOuterJoins() == false)) {
            isEmulationPerformed = emulateFullJoin(virtualDB, blockName, firstPassSqlString, 
                select, from, where, groupby, having, orderby, allJoins, allSqls, cleanupItems);
            if (isEmulationPerformed == true) {
                tempSqlString = allSqls.get(allSqls.size()-1);
            }
        }

        // 20150218: Emulate Window Function
        if (isEmulationPerformed == false) {
    		if (virtualDB.supportsWindowFunctions() == false) {
    			Boolean isWindowFunctionPresent = blockContext.isWindowFunctionPresent();
    
    			if (isWindowFunctionPresent == true) {
    				System.out.println("Emulate Window Function...");
    				isEmulationPerformed = emulateWindowFunction(dbType, blockContext, blockName, 
				        firstPassSqlString, select, from, where, groupby, having, orderby, allJoins, allSqls, cleanupItems);
    				if (isEmulationPerformed == true) {
    					tempSqlString = allSqls.get(allSqls.size()-1);
    				} else {
    					tempSqlString = firstPassSqlString;
    				}
    			} else {
    				tempSqlString = firstPassSqlString;
    			}
    		}
        }

        // 20141209: only do the processing when there is something in the sql
        if (tempSqlString.length() > 0) {
    		if (dbType == DBType.MSSQL || dbType == DBType.AZURE) {
    			blockTempName.add(blockName);
    		}

	        // 20140910: output temp table for engine block and query block only.
	        if ((dataBlock instanceof EngineBlock) || ((dataBlock instanceof QueryBlock) && !(dataBlock instanceof ResultBlock))) {
    			// cleanup items generated from emulation of full join and window function should be treated as engine block because they are temp to the current block
    			engineBlockQueue.addAll(cleanupItems);
	            if (dataBlock instanceof EngineBlock) {
	                engineBlockQueue.add(new VeroItem(VeroType.TABLE, null, blockName));
	            } else {
	                queryBlockQueue.add(new VeroItem(VeroType.TABLE, null, blockName));
	            }
	            
	            if (virtualDB.supportsCreateTemporaryTable() == false) {
                   curSqlString = "".concat("CREATE TABLE " + blockName).concat(" AS (").concat("\n")
                       .concat(tempSqlString).concat("\n")
                       .concat(")");
	            } else {
	                curSqlString = "".concat("CREATE TEMP TABLE " + blockName).concat(" AS (").concat("\n")
                       .concat(tempSqlString).concat("\n")
                       .concat(")");
	            }

	            if (isEmulationPerformed == true) {
        			// patch back
        			allSqls.set(allSqls.size()-1, curSqlString);
	            } else {
	                allSqls.add(curSqlString);
	            }

	            if (dbType == DBType.DERBY_LOCAL) {
	                allSqls.add("insert into " + blockName + " " + tempSqlString);
	            }
            } else {
    			engineBlockQueue.addAll(cleanupItems);
    			if (isEmulationPerformed != true) {
    				curSqlString = tempSqlString;
    				allSqls.add(curSqlString);
    			}

    			// 20150115: hidden sqls
    			String hiddenResultSql = null;
                if (virtualDB.supportsCreateTemporaryTable() == false) {
                    hiddenResultSql = "".concat("CREATE TABLE " + blockName).concat(" AS (").concat("\n")
                        .concat(tempSqlString).concat("\n")
                        .concat(")");
                } else {
                    hiddenResultSql = "".concat("CREATE TEMP TABLE " + blockName).concat(" AS (").concat("\n")
                        .concat(tempSqlString).concat("\n")
                        .concat(")");
                }

    			hiddenSqls.add(hiddenResultSql);

    			if (dbType == DBType.DERBY_LOCAL) {
    				hiddenSqls.add("insert into " + blockName + " " + tempSqlString);
    			}

    			hiddenSqls.add("select * from " + blockName);
            }

	        try {
    			for (int i=0; i<allSqls.size(); i++) {
    				finalSql = translateSql(dbType, VeroSqlParser.createStatementSemantics(
				        allSqls.get(i), null, dbType));
    
    				// MSSQL needs to prepend temp table with '#'
    				// doing this here because parser doesn't like # in the name
    				if ((dbType== DBType.MSSQL) || (dbType== DBType.AZURE)){
    					for (String tempBlock : blockTempName) {
    					    finalSql = finalSql.replaceAll(tempBlock, "#".concat(tempBlock));
    					}
    				}
    
    				allSqls.set(i, finalSql);
    			}

    			for (int i=0; i<hiddenSqls.size(); i++) {
    				finalSql = translateSql(dbType, VeroSqlParser.createStatement(hiddenSqls.get(i)));
    
    				// MSSQL needs to prepend temp table with '#'
                    // doing this here because parser doesn't like # in the name
                    if ((dbType== DBType.MSSQL) || (dbType== DBType.AZURE)){
                        for (String tempBlock : blockTempName) {
                            finalSql = finalSql.replaceAll(tempBlock, "#".concat(tempBlock));
                        }
    				}
    
    				hiddenSqls.set(i, finalSql);
    			}

	            dataBlock.setTableAliasMap(curTableAlias.getAlias());
	        } catch (ParsingException e) {
	            System.out.println("Exception on createStatement: " + curSqlString);
	            System.out.println("Exception on createStatement: " + e.getLocalizedMessage());
	            throw e;
	        }
        }

        // prepend initial sql
        // 20150610 ToDo: need a place to insert these sqls
        /*
        if (needHiveConfig(sqlEngineContext.getBlockSerial(), dataBlock) == true) {
            allSqls.addAll(0, HiveConfig.getConfigurations());
        }
        */

        // create final block for dropping tables
        if (dataBlock instanceof ResultBlock) {
            ResultBlock curRB = (ResultBlock) dataBlock;
            if ((engineBlockQueue.size() > 0) || (queryBlockQueue.size() > 0) || hiddenSqls.size() > 0) {
                FinalBlock finalBlock = new FinalBlock(curRB);
                curRB.setFinalBlock(finalBlock);

                genSqlFinalBlock(engineBlockQueue, finalBlock);
                engineBlockQueue.clear();

                genSqlFinalBlock(queryBlockQueue, finalBlock);
                queryBlockQueue.clear();

                // add hidden sqls
                finalBlock.getSqls().add("#");
                finalBlock.getSqls().addAll(hiddenSqls);
                queryBlockQueue.add(new VeroItem(VeroType.TABLE, null, blockName));
                genSqlFinalBlock(queryBlockQueue, finalBlock);
                queryBlockQueue.clear();
            }
        } else if (dataBlock instanceof QueryBlock) {
            QueryBlock curQB = (QueryBlock) dataBlock;
            FinalBlock finalBlock = new FinalBlock(curQB);
            curQB.setFinalBlock(finalBlock);

            if (engineBlockQueue.size() > 0) {
                genSqlFinalBlock(engineBlockQueue, finalBlock);
                engineBlockQueue.clear();
            }
        }

        System.out.println("SQL==> ");
        int sqlCount = 0;
        for (String sql : dataBlock.getSqls()) {
            System.out.println(sqlCount + ": " + sql);
            sqlCount++;
        }
        
        if (dataBlock instanceof QueryBlock) {
    		FinalBlock finalBlock = ((QueryBlock) dataBlock).getFinalBlock();
    		if (finalBlock != null) {
    			for (String sql : finalBlock.getSqls()) {
    	                System.out.println(sqlCount + ": " + sql);
    	                sqlCount++;
    			}
    		}
        }
    }

	private String genSelectString(
        DataBlock dataBlock,
        List<Expressible> selectProcessingOrder,
        TableAlias tableAlias,
        Boolean needReaggregate) throws Exception {

		if (isDataBlockEmpty(dataBlock)) {
			return "";
		}

	    IQueryPlan queryPlan = dataBlock.getPlan();
	    List<String> selectList = new ArrayList<String>();
	    ColumnAlias columnAlias = new ColumnAlias();
	    Map<String, String> representNameMap = blockAliasMap.get(dataBlock.getPhysicalName());
	    Map<String, Expressible> objectMap = new LinkedHashMap<String, Expressible>();
	    sqlEngineContext.setObjectMap(objectMap);
	    BlockContext blockContext = sqlEngineContext.getBlockContext(dataBlock.getPhysicalName());
	    List<SelectItem> selectItems = blockContext.getSelectItems();

	    String itemToSql = null;
	    String retStr = "";

	    List<VeroType> joinableTypes = new ArrayList<VeroType>();

        System.out.println("Re-aggregate = " + needReaggregate);

        // populate object map
        dataBlock.getSelections().forEach(e -> objectMap.put(e.getRID(), e));

    	// loop ordered select items
    	for (Expressible expressible : selectProcessingOrder) {
    		SelectItem selectItem = sqlEngineContext.new SelectItem(expressible);
    		selectItems.add(selectItem);
    
    		if (expressible instanceof Dimension) {
    			Dimension curDim = (Dimension) expressible;
    
                itemToSql = genStringForExpressible(
                    queryPlan,
                    (Dimension) expressible,
                    tableAlias,
                    columnAlias,
                    representNameMap,
                    joinableTypes,
                    selectItem,
                    true,
                    false);
    
                selectList.add(itemToSql);
    
                if (curDim.isConstant()) {
                    // we set constant to constant instead of dimension
                    // is because later in the group generation
                    // it only pikcs up dimensions and we don't want
                    // constant dimension in the group by.
                    selectItem.setType(VeroType.CONSTANT);
                    selectItem.setOriginalType(VeroType.CONSTANT);
                } else {
                    selectItem.setType(VeroType.DIMENSION);
                    selectItem.setOriginalType(VeroType.DIMENSION);
                }
    		} else if (expressible instanceof Measure) {
    			Measure curMea = (Measure) expressible;
    
                itemToSql = genStringForExpressible(
                    queryPlan,
                    curMea,
                    tableAlias,
                    columnAlias,
                    representNameMap,
                    joinableTypes,
                    selectItem,
                    true,
                    needReaggregate);
    
                selectList.add(itemToSql);
    
                if (curMea.isConstant()) {
                    // here we don't set constant measure to constant is
                    // because we need to cause group by generation by counting
                    // if there is any measure
                    selectItem.setType(VeroType.MEASURE);
                    selectItem.setOriginalType(VeroType.CONSTANT);
                } else {
                    IJoinable curJoinable = queryPlan.getJoinable(curMea);
    
                    if (curJoinable instanceof DataBlock) {
                        if (curMea instanceof Measure) {
                            if (needReaggregate == true) {
                                selectItem.setType(VeroType.MEASURE);
                            } else {
                                selectItem.setType(VeroType.DIMENSION);
                            }
                            selectItem.setOriginalType(VeroType.VIRTUAL_MEASURE);
                        } else {
                            selectItem.setType(VeroType.DIMENSION);
                            selectItem.setOriginalType(VeroType.MEASURE);
                        }
                    } else if ((curJoinable instanceof VirtualJoinable) && (needReaggregate == false)) {
                        Boolean allFromTable = true;
            			for (VeroType veroType : joinableTypes) {
            				if (veroType != VeroType.TABLE) { allFromTable = false; break; }
            			}
            			if (allFromTable == true) {
            				// local, treat it as measure
            				selectItem.setType(VeroType.MEASURE);
            			} else {
            				selectItem.setType(VeroType.DIMENSION);
            			}
            			selectItem.setOriginalType(VeroType.MEASURE);
                    } else {
                        selectItem.setType(VeroType.MEASURE);
                        selectItem.setOriginalType(VeroType.MEASURE);
                    }
                }
    		} else if (expressible instanceof BlockDerivedDimension) {
    			itemToSql = genStringForBlockDerivedEntity(
    	            queryPlan,
    	            (BlockDerivedDimension) expressible,
    	            tableAlias,
    	            columnAlias,
    	            representNameMap,
                    joinableTypes,
                    selectItem,
    	            true,
    	            false);
    
			    selectList.add(itemToSql);
                selectItem.setType(VeroType.DIMENSION);
                selectItem.setOriginalType(VeroType.BLOCK_DERIVED_DIMENSION);
    		} else if (expressible instanceof BlockDerivedMeasure) {
    			itemToSql = genStringForBlockDerivedEntity(
    	            queryPlan,
    	            (BlockDerivedMeasure) expressible,
    	            tableAlias,
                    columnAlias,
                    representNameMap,
    	            joinableTypes,
    	            selectItem,
    	            true,
    	            needReaggregate);
    
    			selectList.add(itemToSql);
    			if (needReaggregate == false) {
    				selectItem.setType(VeroType.DIMENSION);
    			} else {
    				selectItem.setType(VeroType.MEASURE);
    			}
    			selectItem.setOriginalType(VeroType.BLOCK_DERIVED_MEASURE);
    		}
    	}

        if (selectList.size() > 0) {
            Joiner joiner = Joiner.on(", ").skipNulls();
            retStr = genCustomString("SELECT", joiner.join(selectList));
		}

	    return retStr;
	}

	private String genStringForConstant(
        Expressible expressible) {
        String itemToSql = "";
        String formula = expressible.getExpressions().get(0).getFormula();

        if (expressible instanceof Dimension) {
            /*
    		if (formula.startsWith("'")) {
    			// assume it's enclosed by user with "'"
    			itemToSql = formula;
    		} else {
    			itemToSql = itemToSql.concat("\'").concat(formula).concat("\'");
    		}
    		*/
            
            // 20160201: fix VB-457: user needs to be responsible for the input
            // to solve the constant case: dateadd('XXXX', 1)
            itemToSql = itemToSql.concat(formula);
        } else {
            itemToSql = itemToSql.concat(formula);
        }

        return itemToSql;
	}

	private String genStringForCoalesceJoinable(
        Expressible expressible,
        TableAlias tableAlias,
        CoalesceJoinable coalesceJoinable) {
        // 20141106: CoalesceJoinable will only be associated with dimensions, not measures
        int count = 0;
        String itemToSql = null;
        for (IJoinable joinable : coalesceJoinable.getJoinables()) {
            String joinableId = joinable.getPhysicalName();
            Map<String, String> curRepresentNameMap = blockAliasMap.get(joinableId);
            String representName = curRepresentNameMap.get(expressible.getName());
            // 20160215: tableAlias fix
            //String tableAliasName = tableAlias.assignAlias(joinableId);
            String tableAliasName = tableAlias.assignAlias(joinable);

            if (count == 0) {
                itemToSql = "".concat("coalesce(");
            } else {
                itemToSql = itemToSql.concat(", ");
            }

            itemToSql = itemToSql.concat(tableAliasName).concat(".").concat(representName);
            count++;
        }

        if (count != 0) {
            itemToSql = itemToSql.concat(")");
        }

        return itemToSql;
	}

	private String genStringForBlockRef(
        IQueryPlan queryPlan,
	    String originalFormula,
        TableAlias tableAlias,
	    BlockRef<? extends Expressible> blockRef,
	    SelectItem selectItem,
	    Boolean doneReaggregate,
	    Boolean needReaggregate) throws Exception {
	    String itemToSql = "";
        String split[] = FormulaUtils.parseBlockDerivedName(originalFormula);
        String blockName = "";

        System.out.println("BLOCKREF, FORMULA==>" + originalFormula);
        blockName = split[0];

        Boolean localMeasure = false;
        Boolean found = false;
        IJoinable joinable = null;
        Expressible expressible = null;
        
        if (blockRef != null) {
            found = true;

            expressible = blockRef.getReference();

            if ((blockName.equals(sqlEngineContext.getCurrentDataBlock().getName())) || blockName.equals("this")) {
                // refer to some engine block
                // 20141130: or potentially local table
                if (expressible instanceof BlockDerivedEntity) {
                    joinable = blockRef.getJoinable();
                } else {
                    joinable = queryPlan.getJoinable(expressible);
                }
            } else {
                // refer to some explicitly stated block
        		joinable = blockRef.getJoinable();
            }
        } else {
            found = false;
        }

        if (!found) {
            // 20140910 ToDo: throw exception
            return "";
        }

	    if (joinable instanceof Table) {
    		// reference to local dim/mea
	        if ((blockName.equals(sqlEngineContext.getCurrentDataBlock().getName())) || blockName.equals("this")) {
    			Map<String, String> representNameMap = blockAliasMap.get(queryPlan.getPlanBlock().getPhysicalName());
    			String representName = representNameMap.get(expressible.getName());
    			BlockContext bc = sqlEngineContext.getBlockContext(queryPlan.getPlanBlock().getPhysicalName());
    			String key = null;
    			VeroType type = null;
    			for (SelectItem curSelectItem : bc.getSelectItems()) {
    			    if (CommonUtils.endsWithIgnoreCaseQuotes(curSelectItem.getAlias(), representName)) {
    					//System.out.println("Found!!");
    					key = curSelectItem.getKey();
    					type = curSelectItem.getType();
    					if (type == VeroType.MEASURE) {
    						localMeasure = true;
    					}
    					break;
    				}
    			}
    			if (key != null) {
    				itemToSql = key;
    			}
    		} else {
    		    // 20160215: tableAlias fix
    			//String tableAliasName = tableAlias.assignAlias(joinable.getPhysicalName());
    		    String tableAliasName = tableAlias.assignAlias(joinable);
    			Map<String, String> representNameMap = blockAliasMap.get(queryPlan.getPlanBlock().getPhysicalName());
    			String representName = representNameMap.get(expressible.getName());
    			itemToSql = tableAliasName.concat(".").concat(representName);
    		}
	    } else if (joinable instanceof VirtualJoinable) {
	        String formula = ((VirtualJoinable) joinable).getExpression().getFormula();

	        itemToSql = genStringForVirtualJoinable(queryPlan, tableAlias, formula, null, (VirtualJoinable) joinable, false);
	    } else if (joinable instanceof CoalesceJoinable) {
	        itemToSql = genStringForCoalesceJoinable(
                expressible,
                tableAlias,
                (CoalesceJoinable) joinable);
	    } else {
    		// block
            if ((blockName.equals(sqlEngineContext.getCurrentDataBlock().getName()) || blockName.equals("this")) && (joinable.getPhysicalName().equals(sqlEngineContext.getCurrentDataBlock().getPhysicalName()))) {
    		//if (blockName.equals("this") && (joinable.getPhysicalName().equals(sqlEngineContext.getCurrentDataBlock().getPhysicalName()))) {
    			// 20150417: even when blockName is "this", it can mean a derived engine block (and not this query block)
    			// which shouldn't be taken care of here.
    			// here it takes care of the situation where the joinable is the current block so the item needs to be
    			// searched from sqlEngineContext of current block
    			BlockContext bc = sqlEngineContext.getBlockContext(joinable.getPhysicalName());
    			Map<String, String> representNameMap = blockAliasMap.get(joinable.getPhysicalName());
    			String representName = representNameMap.get(expressible.getName());
    
    			List<SelectItem> selectItems = bc.getSelectItems();
    			System.out.println(selectItems);
    
    			String foundSelectItemName = "";
    			for (SelectItem curSelectItem : selectItems) {
    				String alias = curSelectItem.getAlias();
    				if (alias == null) { continue; }
    
    				if (CommonUtils.equalsWithIgnoreCaseQuotes(alias, representName)) {
    					foundSelectItemName = curSelectItem.getKey();
    					// 20150422: these stored select items are already re-aggregated
    					doneReaggregate = true;
    					break;
    				}
    			}
    
    			itemToSql = foundSelectItemName;
    		} else {
    		    // 20160215: tableAlias fix
    			//String blockAlias = tableAlias.assignAlias(joinable.getPhysicalName());
    		    String blockAlias = tableAlias.assignAlias(joinable);
    
			    Map<String, String> representNameMap = blockAliasMap.get(joinable.getPhysicalName());

			    if (representNameMap == null) {
    				// 20141130 ToDo: throws exception
    				System.out.println("representNameMap == null...");
    				System.out.println("Expressible = " + expressible.getName());
    				System.out.println("Joinable physical name = " +joinable.getPhysicalName());
    				System.out.println("Joinable name = " +joinable.getName());
    				// dump blockAliasMap
    				for (String key1 : blockAliasMap.keySet()) {
    					System.out.println("key1: " + key1);
    					Map<String, String> value = blockAliasMap.get(key1);
    					for (String key2 : value.keySet()) {
    						System.out.println("key2: " + key2);
    						System.out.println("value2: " + value.get(key2));
    					}
    				}
			    }

			    String representName = representNameMap.get(expressible.getName());

		        itemToSql = "\"" + blockAlias.concat("\"").concat(".").concat("\"").concat(representName).concat("\"");
    		}
	    }

	    if (itemToSql.length() != 0) {
    		// append aggregation function if needed
    		if ((needReaggregate == true) && (doneReaggregate == false)) {
    			PatchByAppendingFuncReturnType funcType;
    			
    			if (blockName.equals(sqlEngineContext.getCurrentDataBlock().getName()) || blockName.equals("this")) {
    				funcType = PatchByAppendingFuncReturnType.REGEN_TREE_FUNC_MAX;
    			} else {
    				funcType = PatchByAppendingFuncReturnType.REGEN_TREE_FUNC_SUM;
    			}
    
    			// Window Function is not possible here
    		    CommonTree patchedTree = TreePatcher.appendAggregateFunc(VeroSqlParser.parseExpression(itemToSql), null, sqlEngineContext.getCurrentDataBlock().getName(), funcType);
    		    itemToSql = translateSql(null, VeroSqlParser.createExpression(patchedTree));
    		}
	    }

	    if (selectItem != null) {
            if (needReaggregate == false) {
                if (localMeasure == false) {
                    selectItem.setType(VeroType.DIMENSION);
                } else {
                    selectItem.setType(VeroType.MEASURE);
                }
            }
	    }

	    return itemToSql;
	}

	private List<VeroItem> generateVeroItems(
        List<ExpressionRef<? extends VeroBase>> references,
        Expressible parent,
		List<String> matchedNames) {
		List<VeroItem> matchedVeroItems = new ArrayList<VeroItem>();

		if (references.size() == 0) {
		    return matchedVeroItems;
		}

		VeroType parentType = null;
		
		if (parent != null) {
			if (parent instanceof BlockDerivedMeasure) {
				parentType = VeroType.BLOCK_DERIVED_MEASURE;
			} else if (parent instanceof BlockDerivedDimension) {
				parentType = VeroType.BLOCK_DERIVED_DIMENSION;
			} else {
				// unlikely
			}
		}
		
		for (int i=0; i<matchedNames.size(); i++) {
	        String matchedName = matchedNames.get(i);
			ExpressionRef<? extends VeroBase> curExpRef = findExpressionReference(references, i);
			VeroObj veroObj = curExpRef.getReference();
			VeroType resultType = VeroType.DIMENSION;
			
			if (curExpRef instanceof BlockRef) {
			    Expressible expressible = (Expressible) curExpRef.getReference();
			    
			    if (expressible instanceof BlockDerivedDimension) {
			        resultType = VeroType.DIMENSION;
			    } else if (expressible instanceof BlockDerivedMeasure) {
			        resultType = VeroType.MEASURE;
			    } else if (expressible instanceof Dimension) {
			        resultType = VeroType.DIMENSION;
			    } else {
			        // Measure
			        resultType = VeroType.MEASURE;
			    }
    		} else if (veroObj instanceof Dimension) {
    			resultType = VeroType.DIMENSION;
    		} else if (veroObj instanceof Measure) {
    			resultType = VeroType.MEASURE;
    		} else if (veroObj instanceof BlockDerivedMeasure) {
    			resultType = VeroType.MEASURE;
    		} else if (veroObj instanceof BlockDerivedDimension) {
    			resultType = VeroType.DIMENSION;
    		}

    		matchedVeroItems.add(new VeroItem(resultType, parentType, matchedName));
		}

		return matchedVeroItems;
	}

	private String genStringForBlockDerivedEntity(
        IQueryPlan queryPlan,
        BlockDerivedEntity blockDerivedEntity,
        TableAlias tableAlias,
        ColumnAlias columnAlias,
        Map<String, String> representNameMap,
        List<VeroType> joinableTypes,
        SelectItem selectItem,
        Boolean assignAlias,
        Boolean needReaggregate) throws Exception {
        // place of window function appearance (3/3)

	    if (blockDerivedEntity  == null) {
	        return null;
	    }

    	String itemToSql = null;
    	Boolean isFailed = false;
    
    	// 20150709: parse it to let pass() fix work (to be able to separate vero idents from others)
    	//String curFormula = blockDerivedEntity.getFormula();
    	String curFormula = FormatterFactory.getSqlFormatter().formatSql(VeroSqlParser.createExpression(blockDerivedEntity.getFormula()));
    	
    	// 20150820: ToDo
    	List<BlockRef<? extends Expressible>> blockRefsInBDE = blockDerivedEntity.getBlockRefs();
    	List<ExpressionRef<? extends VeroBase>> references = new ArrayList<ExpressionRef<? extends VeroBase>>();
    	for (BlockRef<? extends Expressible> blockRefInBDE : blockRefsInBDE) {
    	    references.add(blockRefInBDE);
    	}
    
    	System.out.println("BLOCKDERIVEDENTITY==> " + blockDerivedEntity.getName());
        System.out.println("FORMULA==> " + curFormula);

        List<String> matchedNames = extractVeroIdents(curFormula);

        // gather type info with matched names
        List<VeroItem> matchedVeroItems = generateVeroItems(references, blockDerivedEntity, matchedNames);
    
        // collect and generate replacement
    	for (int i=0; i<matchedVeroItems.size(); i++) {
    	    VeroItem curVeroItem = matchedVeroItems.get(i);
    	    ExpressionRef<? extends VeroBase> curExpRef = findExpressionReference(references, i);
    	    String matchedString = curVeroItem.getName();

    		/*
            System.out.println("matchedString==> " + matchedString);
            System.out.println("ExpressionProp@index " + index + ":"
    			+ " Type: " + curExpProp.getType()
    			+ " Position: " + curExpProp.getPositionInFormula()
    			+ " Id: " + curExpProp.getTargetObjectId());
    		*/

            VeroObj veroObj = curExpRef.getReference();

            if (curExpRef instanceof BlockRef) {
                itemToSql = genStringForBlockRef(
                    queryPlan,
                    matchedString,
                    tableAlias,
                    (BlockRef<? extends Expressible>) curExpRef,
                    selectItem,
                    true,
                    needReaggregate);

                System.out.println("BLOCK_REF itemToSql==> " + itemToSql);
            } else if ((veroObj instanceof Dimension) || (veroObj instanceof Measure)) {
                itemToSql = genStringForExpressible(
                    queryPlan,
                    (Expressible) veroObj,
                    tableAlias,
                    columnAlias,
                    representNameMap,
                    joinableTypes,
                    null,
                    false,
                    needReaggregate);

                System.out.println("EXPRESSIBLE itemToSql==> " + itemToSql);
            } else if (veroObj instanceof BlockDerivedEntity) {
                // 20141107 ToDo: can have BlockDerivedMeasre and BlockDerivedDimension here, not done yet!!!
                /*
                BlockDerivedEntity curBlockDerivedEntity = findBlockDerivedEntity((QueryBlock) dataBlock, curExpProp.getTargetObjectId());

                String itemToSql = genStringForBlockDerivedEntity(
                    queryPlan,
                    curBlockDerivedEntity,
                    tableAlias,
                    null,
                    null,
                    null,
                    blockRefs,
                    false,
                    needReaggregate);
                */
            } else {
                // 20140910 ToDo: throw exception
                isFailed = true;
                break;
            }

            curVeroItem.setReplacement(itemToSql);
        }

        if (needReaggregate == true) {
            CommonTree patchedTree = TreePatcher.appendAggregateFunc(VeroSqlParser.parseExpression(curFormula), matchedVeroItems, sqlEngineContext.getCurrentDataBlock().getName(), PatchByAppendingFuncReturnType.NONE);
            curFormula = translateSql(null, VeroSqlParser.createExpression(patchedTree));
        }

        // replace the formula
        for (int i=0; i<matchedVeroItems.size(); i++) {
            VeroItem curVeroItem = matchedVeroItems.get(i);
            String matchedString = curVeroItem.getName();
            String replacement = curVeroItem.getReplacement();
            curFormula = replaceFirstAvoidOutsideQuotes(curFormula, Pattern.quote(matchedString), replacement);
        }

        if (isFailed != true) {
            Boolean isWindowingFunction = EnhancedNodeExtractor.extract(curFormula).isWindowFunction();

            if (assignAlias == true) {
                String representName = retrieveRepresentNameWithAlias(
                        blockDerivedEntity.getName(), blockDerivedEntity.getSlug(), columnAlias, representNameMap);

        		if (selectItem != null) {
        			selectItem.setKey(curFormula);
        			selectItem.setAlias(representName);
        			selectItem.setWindowFunction(isWindowingFunction);
        		}

                curFormula = curFormula.concat(" ").concat(representName);
            } else {
        		if (selectItem != null) {
        			selectItem.setKey(curFormula);
        			selectItem.setWindowFunction(isWindowingFunction);
        		}
            }
        }

        System.out.println("FINALFORMULA_BLOCKDERIVEDENTITY==> " + curFormula);
        return curFormula;
	}

    private String genStringForExpressible(
        IQueryPlan queryPlan,
        Expressible expressible,
        TableAlias tableAlias,
        ColumnAlias columnAlias,
        Map<String, String> representNameMap,
        List<VeroType> joinableTypes,
        SelectItem selectItem,
        Boolean assignAlias,
        Boolean needReaggregate) throws Exception {

        String itemToSql = null;

        System.out.println("EXPRESSIBLE==> " + expressible.getName() + ", Obj==> " + expressible.getClass());

        if (expressible.isConstant()) {
            // 20141106: constant can't getJoinable()
            itemToSql = genStringForConstant(expressible);

            if (assignAlias == true) {
                String representNameWithAlias = retrieveRepresentNameWithAlias(
                        expressible.getName(), expressible.getSlug(), columnAlias, representNameMap);

                if (selectItem != null) {
        			selectItem.setKey(itemToSql);
        			//selectItem.setAlias(representNameWithAlias.replaceAll("\"", ""));
        			selectItem.setAlias(representNameWithAlias);
                }

                itemToSql = itemToSql.concat(" ").concat(representNameWithAlias);
            } else {
        		if (selectItem != null) {
        			selectItem.setKey(itemToSql);
        		}
            }

            System.out.println("CONSTANT itemToSql==> " + itemToSql);
            return itemToSql;
        }

        IJoinable curJoinable = queryPlan.getJoinable(expressible);

        if (curJoinable instanceof CoalesceJoinable) {
    		itemToSql = genStringForCoalesceJoinable(
    			expressible,
    			tableAlias,
    			(CoalesceJoinable) curJoinable);

    		if (itemToSql != null) {
                if (assignAlias == true) {
                    String representNameWithAlias = retrieveRepresentNameWithAlias(
                            expressible.getName(), expressible.getSlug(), columnAlias, representNameMap);

                    if (selectItem != null) {
                        selectItem.setKey(itemToSql);
                        selectItem.setAlias(representNameWithAlias);
                    }

                    itemToSql = itemToSql.concat(" ").concat(representNameWithAlias);
                } else {
                    if (selectItem != null) {
                        selectItem.setKey(itemToSql);
                    }
                }
    		}

    		System.out.println("COALESCEJOINABLE itemToSql==> " + itemToSql);
            return itemToSql;
        }

        // 20141017: don't use it now. the mini-ide should have covered this
        //String curFormula = patchFormulaWithExpressionForColumn(queryPlan.getExpression(expressible).getFormula(), curExp);
        //String curFormula = queryPlan.getExpression(expressible).getFormula();

        Expression curExp = queryPlan.getExpression(expressible);

        String curFormula = "";
        String curOriginalFormula = "";
        if (curExp != null) {
            //curFormula = curExp.getFormula();
        	curOriginalFormula = curExp.getFormula();
            curFormula = FormatterFactory.getSqlFormatter().formatSql(VeroSqlParser.createExpression(curOriginalFormula));
        }

        System.out.println("ORIGINAL FORMULA==> " + curOriginalFormula);
        System.out.println("FORMATTED FORMULA==> " + curFormula);

        if (curJoinable instanceof VirtualJoinable) {
    		// one entry point for window function. (1/3)
    		Boolean isWindowingFunction = EnhancedNodeExtractor.extract(curOriginalFormula).isWindowFunction();

            // 20150130: fix VB-141
            if (expressible instanceof Measure) {
                if (needReaggregate) {
                    //System.out.println("preparing for re-aggre1..." + " curFormula = " + curFormula);
                    if (isWindowingFunction == false) {
	                    // for the current block measures wrap them in a MAX function
                        curFormula = "max(" + curFormula + ")";
                    } else {
            			// 20150312: Window Function should first remove all the unreachable items then apply re-aggregation if needed
            			// so taken care in genStringForVirtualJoinable()
        			    //CommonTree patchedTree = TreePatcher.appendAggregateFunc(VeroSqlParser.parseExpression(curFormula), PatchByAppendingFuncReturnType.NONE);
        			    //curFormula = translateSql(null, VeroSqlParser.createExpression(patchedTree));
                    }
                }
            }

            itemToSql = genStringForVirtualJoinable(queryPlan, tableAlias, curOriginalFormula, joinableTypes, (VirtualJoinable) curJoinable, needReaggregate);

            if (assignAlias == true) {
                String representNameWithAlias = retrieveRepresentNameWithAlias(
                        expressible.getName(), expressible.getSlug(), columnAlias, representNameMap);

                if (selectItem != null) {
        			selectItem.setKey(itemToSql);
        			selectItem.setAlias(representNameWithAlias);
        			selectItem.setWindowFunction(isWindowingFunction);
                }

                itemToSql = itemToSql.concat(" ").concat(representNameWithAlias);
            } else {
        		if (selectItem != null) {
        			selectItem.setKey(itemToSql);
        			selectItem.setWindowFunction(isWindowingFunction);
        		}
            }

            System.out.println("VIRTUALJOINABLE itemtoSql==> " + itemToSql);

            return itemToSql;
        }

        // 20160215: tableAlias fix
        String joinableId = curJoinable.getPhysicalName();
        //String tableAliasName = tableAlias.assignAlias(joinableId);
        String tableAliasName = tableAlias.assignAlias(curJoinable);

        if (curJoinable instanceof DataBlock) {
            Map<String, String> curRepresentNameMap = blockAliasMap.get(joinableId);

            // 20141106 Comments:
            // measure/dimension name is enclosed in the form of @ABC
            // a normal dim or measure exp can’t be more complicated than @ABC when going against a data block
            // for complicated formula like @ABC+1 or @ABC+@DEF, it will be either virtual or block derived stuff
            String veroIdent = FormulaUtils.parseBlockDerivedName(curFormula)[1];
            String representName = curRepresentNameMap.get(veroIdent);

            itemToSql = "\"" + tableAliasName.concat("\"").concat(".").concat("\"").concat(representName).concat("\"");
            //itemToSql = patchFormulaWithAlias(representName, tableAliasName);

            if (expressible instanceof Measure) {
                if (needReaggregate) {
                    System.out.println("preparing for re-aggre2..." + " itemToSql = " + itemToSql);
                    // for the current block measures wrap them in a MAX function
                    itemToSql = "max(" + itemToSql + ")";
                } else {
                    // treat it as dimension
                }
            }

            if (assignAlias == true) {
                String representNameWithAlias = retrieveRepresentNameWithAlias(
                        expressible.getName(), expressible.getSlug(), columnAlias, representNameMap);

                if (selectItem != null) {
        			selectItem.setKey(itemToSql);
        			selectItem.setAlias(representNameWithAlias);
                }

                itemToSql = itemToSql.concat(" ").concat(representNameWithAlias);
            } else {
        		if (selectItem != null) {
        			selectItem.setKey(itemToSql);
        		}
            }

            System.out.println("DATABLOCK itemToSql==> " + itemToSql);
        } else if (curJoinable instanceof Table) {
            ExtractResult extractResult = EnhancedNodeExtractor.extract(curOriginalFormula);
            if (extractResult.isWindowFunction() == true) {
                // the second place that can have window function (2/3)
                itemToSql = genStringForWindowFunction(
                    queryPlan,
    				tableAlias,
    				curOriginalFormula,
    				queryPlan.getExpression(expressible),
    				joinableTypes,
    				needReaggregate);
        		if (selectItem != null) {
        			selectItem.setWindowFunction(true);
        		}
    		} else {
    			itemToSql = patchFormulaWithAlias(curOriginalFormula, tableAliasName);
    		}

            if (assignAlias == true) {
                String representNameWithAlias = retrieveRepresentNameWithAlias(
                        expressible.getName(), expressible.getSlug(), columnAlias, representNameMap);

                if (selectItem != null) {
                    selectItem.setKey(itemToSql);
                    selectItem.setAlias(representNameWithAlias);
                }

                itemToSql = itemToSql.concat(" ").concat(representNameWithAlias);
            } else {
        		if (selectItem != null) {
        			selectItem.setKey(itemToSql);
        		}
            }

            System.out.println("TABLE itemToSql==> " + itemToSql);
        } else {
            System.out.println("ERROR..... unknown type for joinable...");
        }

        return itemToSql;
    }

    private String genStringForVirtualJoinable(
        IQueryPlan queryPlan,
        TableAlias tableAlias,
        String formula,
        List<VeroType> joinableTypes,
        VirtualJoinable joinable,
        Boolean needReaggregate) throws Exception {
        String itemToSql = "";
        Boolean isWindowingFunction = false;
        Expression expression = joinable.getExpression();

        CommonTree commonTree = VeroSqlParser.parseExpression(formula);
        List<String> matchedNames = extractVeroIdents(formula);

        isWindowingFunction = EnhancedNodeExtractor.extract(commonTree).isWindowFunction();

        if (isWindowingFunction == false) {
            for (int i=0; i<expression.getReferences().size(); i++) {
                ExpressionRef<? extends VeroBase> expRef = findExpressionReference(expression.getReferences(), i);

                if ((expRef.getReference() instanceof Measure) || (expRef.getReference() instanceof Dimension)) {
                    Expressible expressible = (Expressible) expRef.getReference();

                    if (expressible != null) {
                        if (expressible.isConstant()) {
                            itemToSql = genStringForConstant(expressible);
                        } else {
                            IJoinable epJoinable = queryPlan.getJoinable(expressible);
                            Expression epExpression = queryPlan.getExpression(expressible);

                            if (epJoinable instanceof DataBlock) {
                                Map<String, String> representNameMap = blockAliasMap.get(epJoinable.getPhysicalName());
                                String veroIdent = FormulaUtils.parseBlockDerivedName(matchedNames.get(i))[1];
                                String representName = representNameMap.get(veroIdent);
                                String tableAliasName = tableAlias.assignAlias(epJoinable);
                                itemToSql = "\"" + tableAliasName.concat("\"").concat(".").concat("\"").concat(representName).concat("\"");
                                if (joinableTypes != null) { joinableTypes.add(VeroType.ENGINE_BLOCK); }
                            } else if (epJoinable instanceof VirtualJoinable) {
                                itemToSql = genStringForVirtualJoinable(queryPlan, tableAlias, epExpression.getFormula(), joinableTypes,
                                    (VirtualJoinable) epJoinable, false);
                            } else {
                                // Table
                                String tableAliasName = tableAlias.assignAlias(epJoinable);
                                itemToSql = patchFormulaWithAlias(epExpression.getFormula(), tableAliasName);
                                if (joinableTypes != null) { joinableTypes.add(VeroType.TABLE); }
                            }
                        }

                        //formula = formula.replaceFirst(Pattern.quote(matchedNames.get(i)), itemToSql);
                        formula = replaceFirstAvoidOutsideQuotes(formula, Pattern.quote(matchedNames.get(i)), itemToSql);
                    }
                } else {
                    // 20140812 ToDo: is it possible to have other types here?
                }
            }
        } else {
            formula = genStringForWindowFunction(
                queryPlan,
    			tableAlias,
    			formula,
    			expression,
    			joinableTypes,
    			needReaggregate);
        }

        return formula;
    }

    private String genFromString(
        Report report,
		DataBlock dataBlock,
		TableAlias tableAlias,
		List<String> allJoins,
		List<LinkedHashSet<String>> listCoalesceSets /* INOUT */) throws Exception {
        IQueryPlan queryPlan = dataBlock.getPlan();
        String retStr = "";
        String fromList = "";
        Boolean emptyPlan = false;
        List<IJoinExpression> joinDefs = new ArrayList<IJoinExpression>();

        if (dataBlock instanceof ResultBlock) {
            // debug
            //logger.debug("genFromString() for ResultBlock...");
        }
        
		if (isDataBlockEmpty(dataBlock)) {
			return "";
		}

        if (queryPlan instanceof OptimizedPlan) {
            OptimizedPlan optPlan = ((OptimizedPlan) queryPlan);
            if (optPlan.isEmpty()) {
                emptyPlan = true;
            } else if (optPlan.isConstantOnlyPlan() == true) {
                return retStr;
            } else {
                joinDefs.addAll(optPlan.getJoinTree().getJoinDefs());
                /*
                if (optPlan.getDisjointedPlans().size() > 0) {
                    System.out.println("Adding disjoint plan joindefs...");
                    for (DisjointPlan disjointPlan : optPlan.getDisjointedPlans()) {
                        JoinTree joinTree = disjointPlan.getJoinTree();
                        List<IJoinDef> jDefs = joinTree.getJoinDefs();
                        joinDefs.addAll(jDefs);
                    }
                }
                */
            }
        } else {
            joinDefs.addAll(queryPlan.getJoinTree().getJoinDefs());
        }

        Map<IJoinable, List<IJoinable>> joinMap = new HashMap<IJoinable, List<IJoinable>>();
        if (emptyPlan == false) {
            while (!joinDefs.isEmpty()) {
                String oneJoin = null;

                // search for regular join
                for (IJoinExpression jDef : joinDefs) {
        			//System.out.println("Processing regular join: " + jDef);
        			//System.out.println("Current from list: " + fromList);
        		    IJoinable leftJoinable = jDef.getLeft();
        		    IJoinable rightJoinable = jDef.getRight();

        		    // schema + table name
        		    String leftName = getJoinableNameWithSchema(sqlEngineContext.getVirtualDB().getAppendSchema(), leftJoinable);
        		    String leftJoinableAlias = tableAlias.assignAlias(leftJoinable);

                    if (rightJoinable != null) {
                        String rightName = getJoinableNameWithSchema(sqlEngineContext.getVirtualDB().getAppendSchema(), rightJoinable);
                        String rightJoinableAlias = tableAlias.assignAlias(rightJoinable);
                        
                        JoinType joinType = jDef.getJoinType();
                        String joinDefExp = null;

                        if (joinMap.isEmpty()) {
                            // first entry
                            joinMap.put(rightJoinable, new ArrayList<IJoinable>());
                            joinMap.put(leftJoinable, new ArrayList<IJoinable>(Arrays.asList(rightJoinable)));
                            System.out.println("JD1 ==> " + jDef);

                            if (joinType == JoinType.CROSS_JOIN) {
                                oneJoin = "".concat(leftName).concat(" AS ").concat(leftJoinableAlias)
                                    .concat(" ").concat(joinTypeToString(joinType))
                                    .concat(" ").concat(rightName).concat(" AS ").concat(rightJoinableAlias);
                                allJoins.add(oneJoin);
                                fromList = fromList.concat(oneJoin);
                            } else {
                                if ((jDef.getLeft() instanceof Table) && (jDef.getRight() instanceof Table)) {
                                    joinDefExp = patchJoinDef(jDef.getJoinFormula(), leftJoinable.getName(), leftJoinableAlias, 
                                        rightJoinable.getName(), rightJoinableAlias);
                                } else {
                                    joinDefExp = genFromStringForOneItemMergeBlock(jDef, dataBlock, tableAlias);
                                }

                                oneJoin = "".concat(leftName).concat(" AS ").concat(leftJoinableAlias)
                                    .concat(" ").concat(joinTypeToString(joinType))
                                    .concat(" ").concat(rightName).concat(" AS ").concat(rightJoinableAlias)
                                    .concat(" ON ").concat(joinDefExp);
                                allJoins.add(oneJoin);
                                fromList = fromList.concat(oneJoin);
                            }
                        } else {
                            Boolean repeatedEntry = false;
                            if (joinMap.containsKey(leftJoinable)) {
                                List<IJoinable> joinables = joinMap.get(leftJoinable);
                                if (joinables.contains(rightJoinable)) {
                                    // repeated entry
                                    repeatedEntry = true;
                                } else {
                                    joinables.add(rightJoinable);
                                    joinMap.put(rightJoinable, new ArrayList<IJoinable>());
                                    System.out.println("JD2 ==> " + jDef);
                                }
                            } else {
                                // the join order is out of order, need to find one through all the remaining joins
                                for (IJoinExpression innerJDef : joinDefs) {
                                    IJoinable innerLeftJoinable = innerJDef.getLeft();
                                    if (joinMap.containsKey(innerLeftJoinable)) {
                                        jDef = innerJDef;
                                        leftJoinable = innerLeftJoinable;
                                        rightJoinable = jDef.getRight();
                                        leftName = getJoinableNameWithSchema(sqlEngineContext.getVirtualDB().getAppendSchema(), leftJoinable);
                                        rightName = getJoinableNameWithSchema(sqlEngineContext.getVirtualDB().getAppendSchema(), rightJoinable);
                                        leftJoinableAlias = tableAlias.assignAlias(leftJoinable);
                                        rightJoinableAlias = tableAlias.assignAlias(rightJoinable);
                                        joinType = jDef.getJoinType();
                                        break;
                                    }
                                }

                                List<IJoinable> joinables = joinMap.get(leftJoinable);

                                if (joinables == null) {
                                    System.out.println("DEBUG THIS");
                                }

                                if (joinables.contains(rightJoinable)) {
                                    // repeated entry
                                    repeatedEntry = true;
                                } else {
                                    joinables.add(rightJoinable);
                                    joinMap.put(rightJoinable, new ArrayList<IJoinable>());
                                    System.out.println("JD3 OUT OF ORDER ==> " + jDef);
                                }
                            }

                            if (repeatedEntry == false) {
                                if (joinType == JoinType.CROSS_JOIN) {
                                    oneJoin = "".concat(" ").concat(joinTypeToString(joinType))
                                        .concat(" ").concat(rightName).concat(" AS ").concat(rightJoinableAlias);
                                    allJoins.add(oneJoin);
                                    fromList = fromList.concat(oneJoin);
                                } else {
                					if ((jDef.getLeft() instanceof Table) && (jDef.getRight() instanceof Table)) {
                						joinDefExp = patchJoinDef(jDef.getJoinFormula(), leftJoinable.getName(), leftJoinableAlias, 
            						        rightJoinable.getName(), rightJoinableAlias);
                					} else {
                						joinDefExp = genFromStringForOneItemMergeBlock(jDef, dataBlock, tableAlias);
                					}

                                    oneJoin = "".concat(" ").concat(joinTypeToString(joinType))
                                        .concat(" ").concat(rightName).concat(" AS ").concat(rightJoinableAlias)
                                        .concat(" ON ").concat(joinDefExp);
                                    allJoins.add(oneJoin);
                                    fromList = fromList.concat(oneJoin);
                                }
                            }
                        }
                    } else {
                        // single table
                        System.out.println("JDSingleTable==> " + jDef);
                        if (joinMap.isEmpty()) {
                            // first entry
                            joinMap.put(leftJoinable, new ArrayList<IJoinable>());
                            oneJoin = "".concat(leftName).concat(" AS ").concat(leftJoinableAlias);
                            allJoins.add(oneJoin);
                            fromList = fromList.concat(oneJoin);
                        } else {
                            if (!joinMap.containsKey(leftJoinable)) {
                                joinMap.put(leftJoinable, new ArrayList<IJoinable>());
                                oneJoin = "".concat(leftName).concat(" AS ").concat(leftJoinableAlias);
                                allJoins.add(oneJoin);
                                fromList = fromList.concat(oneJoin);
                            }
                        }
                    }
                    joinDefs.remove(jDef);
                    break;
                }
            }
        }

        // debug
        // print all regular join
        System.out.println("Debug reg join:");
        for (IJoinable joinable : joinMap.keySet()) {
            List<IJoinable> joinables = joinMap.get(joinable);
            System.out.println("regJoin: " + joinable);
            for (IJoinable oneJoinable : joinables) {
                System.out.println("regJoin target: " + oneJoinable);
            }
        }
        
        // debug
        // print all b2bjoin
        if (dataBlock instanceof QueryBlock) {
    		QueryBlock thisBlock = (QueryBlock) dataBlock;
    		System.out.println("Debug b2b join:");
    		for (BlockToBlockJoin b2bJoin : thisBlock.getQueryBlockJoins()) {
    			System.out.println("b2bJoin: " + b2bJoin.getJoinType());
    			System.out.println("b2bJoin: " + b2bJoin.getJoinFormula());
    			System.out.println("b2bJoin target: " + b2bJoin.getTarget().getPhysicalName());
                for (IGroupable iGroupable : b2bJoin.getJoinKeys()) {
			        System.out.println("iGroupable: " + iGroupable);
                }
    		}
        }
        
        // 20141216: resolve b2b join, not done yet
        // process block join if any in result block
        if (dataBlock instanceof QueryBlock) {
    		QueryBlock thisBlock = (QueryBlock) dataBlock;
            CoalesceMap coalesceMap = new CoalesceMap();
            
            ArrayList<BlockToBlockJoin> b2bJoins = new ArrayList<BlockToBlockJoin>(thisBlock.getQueryBlockJoins());
            Collections.sort(b2bJoins, Collections.reverseOrder());
            
            // allBlocksInJoins keeps track of all blocks that are included from joins, it's incremental
            LinkedHashSet<DataBlock> allBlocksInJoins = new LinkedHashSet<DataBlock>();
    		
            // first, gather common keys between hosting block and target block
            for (BlockToBlockJoin b2bJoin : b2bJoins) {
                for (IGroupable iGroupable : b2bJoin.getJoinKeys()) {
                    if (iGroupable instanceof Dimension) {
                        Dimension curDim = (Dimension) iGroupable;
                        if (isExpressibleReachableByPlan(queryPlan, curDim)) {
                            QueryBlock targetBlock = b2bJoin.getTarget();
                            if (isExpressibleReachableByPlan(targetBlock.getPlan(), curDim)) {                        
                                coalesceMap.addBlock(curDim, thisBlock);
                                coalesceMap.addBlock(curDim, targetBlock);
                            }
                        }
                    } else if (iGroupable instanceof BlockDerivedDimension) { 
                        BlockDerivedDimension curBdd = (BlockDerivedDimension) iGroupable;
                        
                        // 20160216: remove such check to allow regular dim join with bdd
                        if (SqlEngineUtils.findBlockDerivedDimension(thisBlock, curBdd.getRID()) != null) {
                            coalesceMap.addBlock(curBdd, thisBlock);
                            coalesceMap.addBlock(curBdd, b2bJoin.getTarget());
                        } else {
                            Boolean addedBlock = false;
                            
                            /*
                            // the bdd can't be found in current block, let's check if we can find it from block's been joined to this block
                            //System.out.println("bdd dim==> " + curBdd.getName());
                            for (IJoinable joinable : joinMap.keySet()) {
                                String blockName = joinable.getPhysicalName();
                                //System.out.println("joinable name==> " + blockName);
                                DataBlock innerDataBlock = SqlEngineUtils.findDataBlock(report, blockName);
                                if (innerDataBlock != null) {
                                    HashSet<Dimension> dimensions = innerDataBlock.getAllDimensions();
                                    for (Dimension dim : dimensions) {
                                        //System.out.println("looping dims==> " + dim.getName());
                                        if (dim.getName().equals(curBdd.getName())) {
                                            allBlocksInJoins.add(innerDataBlock);
                                            coalesceMap.addBlock(curBdd, innerDataBlock); 
                                            addedBlock = true;
                                        }
                                    }
                                }
                            }
                            */
                            
                            // the bdd can't be found in current block, let's check if we can find it from select list of this block
                            //System.out.println("bdd dim==> " + curBdd.getName());
                            BlockContext blockContext = sqlEngineContext.getBlockContext(dataBlock.getPhysicalName());
                            for (SelectItem selectItem : blockContext.getSelectItems()) {
                                Expressible curExp = selectItem.getExpressible();
                                if (curExp != null) {
                                    if ((curExp instanceof Dimension) || (curExp instanceof BlockDerivedDimension)) {                             
                                        if (curExp.getName().equals(curBdd.getName())) {
                                            allBlocksInJoins.add(dataBlock);
                                            coalesceMap.addBlock(curBdd, dataBlock); 
                                            addedBlock = true;
                                            coalesceMap.addBlock(curBdd, b2bJoin.getTarget());
                                        }  
                                    }
                                }
                            }
                        }
                    }
                }
            }
    		
    		// second, construct joins
    		allBlocksInJoins.add(thisBlock);
    		for (BlockToBlockJoin b2bJoin : b2bJoins) {
    			QueryBlock targetBlock = b2bJoin.getTarget();
                String targetBlockId = targetBlock.getPhysicalName();
                // 20160215: tableAlias fix
                //String targetBlockAlias = tableAlias.assignAlias(targetBlockId);
                String targetBlockAlias = tableAlias.assignAlias(targetBlock);
                String oneJoin = "";
                int numCommonKeys = 0;
    
                allBlocksInJoins.add(targetBlock);
                    
                for (IGroupable iGroupable : b2bJoin.getJoinKeys()) {
                    Expressible curExp = (Expressible) iGroupable;
                    if (coalesceMap.containsExpressibleName(curExp.getName())) {
                        numCommonKeys++;
                    }
                }
    
                if (numCommonKeys == 0) {
                    if (fromList.length() == 0) {
                        // there is no join yet, use single table scenario
                        oneJoin = "".concat(targetBlock.getPhysicalName()).concat(" AS ").concat(targetBlockAlias);
                    } else {
                        // cross join
                        oneJoin = "".concat(" ").concat(joinTypeToString(JoinType.CROSS_JOIN))
                            .concat(" ").concat(targetBlock.getPhysicalName()).concat(" AS ").concat(targetBlockAlias);
                    }
    
                    // update join map
                    for (IGroupable iGroupable : b2bJoin.getJoinKeys()) {
                        Expressible curExp = (Expressible) iGroupable;
                        coalesceMap.addBlock(curExp, targetBlock);
                    }
                } else {
                    JoinType joinType = b2bJoin.getJoinType();
                    Map<String, String> targetBlockRepresentNameMap = blockAliasMap.get(targetBlockId);
                    int onCount = 0;
    
                    if (fromList.length() == 0) {
                        oneJoin = "".concat(" ").concat(b2bJoin.getTarget().getPhysicalName()).concat(" AS ").concat(targetBlockAlias);                                    
                    } else {
                        oneJoin = "".concat(" ").concat(joinTypeToString(joinType))
                            .concat(" ").concat(b2bJoin.getTarget().getPhysicalName()).concat(" AS ").concat(targetBlockAlias).concat(" ON ");                                       
                    
                        for (IGroupable iGroupable : b2bJoin.getJoinKeys()) {
                            // 20160203: TODO missing BDD here
                            Expressible curExp = (Expressible) iGroupable;
                            
                            LinkedHashSet<DataBlock> joinKeyMapBlocks = null;
        
                            if (coalesceMap.containsExpressibleName(curExp.getName())) {
                                joinKeyMapBlocks = coalesceMap.getBlocks(curExp.getName());
                                Boolean selfGen = false;
                                String selfGenName = "";
    
                				System.out.println("All blocks that share the same expressible: " + curExp);
                				for (DataBlock db : joinKeyMapBlocks) {
                					System.out.println("db==> " + db.getPhysicalName());
                				}
    
                				String coalesceSql = null;
                				String leftRepresentName = null;
                				String rightRepresentName = null;
                				String rightBlockAlias = null;
                                String name = null;
        
                                if ((joinKeyMapBlocks.size() > 1) && (b2bJoin.getJoinType() == JoinType.FULL_OUTER_JOIN)) {
                                    coalesceSql = "Coalesce(";
                                    // need to generate coalesce sql on common keys                                    
                                    // the reason of using hashset is to ignore duplicates that may appear in result block and its engine block
                                    Set<String> joinStuff = new LinkedHashSet<String>();
        
                                    for (DataBlock fromBlock : joinKeyMapBlocks) {
                                        Map<String, String> rightRepresentNameMap = null;
        
                                        /*
        						        System.out.println("fromBlock.getPhysicalName() = " + fromBlock.getPhysicalName());
        						        System.out.println("dataBlock.getPhysicalName() = " + dataBlock.getPhysicalName());
                                        */
        
                						if (fromBlock.getPhysicalName().equals(targetBlockId)) {
                							continue;
                						}
        
                						if (!allBlocksInJoins.contains(fromBlock)) {
                							// this block hasn't been added
                							continue;
                						}
        
                    					if (fromBlock.getPhysicalName().equals(dataBlock.getPhysicalName())) {
                    					    IJoinable targetJoinable = null;
                    					    if (curExp instanceof Dimension) {
                    					        targetJoinable = queryPlan.getJoinable(curExp);
                    					    }
        
                    						// from this block join query block
                                            if (targetJoinable instanceof CoalesceJoinable) {
                        						for (IJoinable curJoinable : ((CoalesceJoinable) targetJoinable).getJoinables()) {
                        							//System.out.println("Joinable: " + curJoinable.getPhysicalName());
                        							rightRepresentNameMap = blockAliasMap.get(curJoinable.getPhysicalName());
                        							rightRepresentName = rightRepresentNameMap.get(curExp.getName());
                        							rightRepresentName = SqlEngineUtils.getQuotedString(rightRepresentName);
                        							// check if key is present in this block
                        							if (rightRepresentNameMap.containsKey(curExp.getName())) {
                        							    rightBlockAlias = tableAlias.assignAlias(curJoinable);
                        							}
                        
                        							name = rightBlockAlias + "." + rightRepresentName;
                        							SqlEngineUtils.QuotedNameAwareSetInsertion(joinStuff, name);
                        							coalesceMap.addName(curExp, name);
                        						}
                                            } else if (targetJoinable != null) {
                                                rightRepresentNameMap = blockAliasMap.get(targetJoinable.getPhysicalName());
                                                rightRepresentName = rightRepresentNameMap.get(curExp.getName());
                                                rightRepresentName = SqlEngineUtils.getQuotedString(rightRepresentName);
                                                rightBlockAlias = tableAlias.assignAlias(targetJoinable);
                                                name = rightBlockAlias + "." + rightRepresentName;
                                                SqlEngineUtils.QuotedNameAwareSetInsertion(joinStuff, name);
                                                coalesceMap.addName(curExp, name);
                                            } else {
                                                // curExp is a BlockDerivedDimension
                                                // 20160623: hack to bdd
                                                BlockContext blockContext = sqlEngineContext.getBlockContext(dataBlock.getPhysicalName());
                                                for (SelectItem selectItem : blockContext.getSelectItems()) {
                                                    Expressible innerCurExp = selectItem.getExpressible();
                                                    if (innerCurExp != null) {
                                                        if ((innerCurExp instanceof Dimension) || (innerCurExp instanceof BlockDerivedDimension)) {
                                                            if (innerCurExp.getName().equals(curExp.getName())) {
                                                                selfGen = true;
                                                                selfGenName = selectItem.getKey();
                                                                
                                                                // 20160710: here we getKey() from a selectItem. it can already be a coalesce() expression
                                                                // and if we don't extract the elements out, then later when we try to patchCoalesce outside,
                                                                // it will fail. we're supposed to addName() into coalesceMap for plain names,
                                                                // not those complicated names (ex. coalesce)                                                                
                                                                if (selfGenName.startsWith("coalesce")) {
                                                                    List<String> elements = SqlEngineUtils.extractQualifiedNames(selfGenName);
                                                                    for (String element : elements) {
                                                                        coalesceMap.addName(curExp, element);
                                                                    }
                                                                }
                                                                SqlEngineUtils.QuotedNameAwareSetInsertion(joinStuff, selfGenName);
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }

                                                if (selfGen == false) {
                                                    rightRepresentNameMap = blockAliasMap.get(fromBlock.getPhysicalName());
                                                    rightRepresentName = rightRepresentNameMap.get(curExp.getName());
                                                    rightRepresentName = SqlEngineUtils.getQuotedString(rightRepresentName);
                                                    rightBlockAlias = tableAlias.assignAlias(fromBlock);
                                                    name = rightBlockAlias + "." + rightRepresentName;
                                                    SqlEngineUtils.QuotedNameAwareSetInsertion(joinStuff, name);
                                                    coalesceMap.addName(curExp, name);
                                                }
                                            }
    
                                            leftRepresentName = targetBlockRepresentNameMap.get(curExp.getName());
                                            leftRepresentName = SqlEngineUtils.getQuotedString(leftRepresentName);
                    					} else {
                    						// from query block join query block
                    						rightRepresentNameMap = blockAliasMap.get(fromBlock.getPhysicalName());
                    						rightRepresentName = rightRepresentNameMap.get(curExp.getName());
                    						rightRepresentName = SqlEngineUtils.getQuotedString(rightRepresentName);
                    						rightBlockAlias = tableAlias.assignAlias(fromBlock);
                    						leftRepresentName = targetBlockRepresentNameMap.get(curExp.getName());
                    						leftRepresentName = SqlEngineUtils.getQuotedString(leftRepresentName);
                    						name = rightBlockAlias + "." + rightRepresentName;
                    						SqlEngineUtils.QuotedNameAwareSetInsertion(joinStuff, name);
                    						coalesceMap.addName(curExp, name);
                    					}
                                    }
        
                                    if (joinStuff.size() > 1) {
                                        Joiner joiner = Joiner.on(", ").skipNulls();
                                        coalesceSql = coalesceSql.concat(joiner.join(joinStuff)).concat(")");
                                    } else {
                                        coalesceSql = null;
                                    }
                                } else {
                                    // 20141218: always join on the first element in the block list
                                    DataBlock fromBlock = joinKeyMapBlocks.iterator().next();
                                    Map<String, String> rightRepresentNameMap = null;
        
                                    if (fromBlock.getPhysicalName().equals(dataBlock.getPhysicalName())) {
                                        IJoinable targetJoinable = null;
                                        if (curExp instanceof Dimension) {
                                            targetJoinable = queryPlan.getJoinable(curExp);
                                        }
                                        
                                        // from this block join query block
                                        if (targetJoinable instanceof CoalesceJoinable) {
                        					for (IJoinable curJoinable : ((CoalesceJoinable) targetJoinable).getJoinables()) {
                        						//System.out.println("Joinable: " + curJoinable.getPhysicalName());
                        						rightRepresentNameMap = blockAliasMap.get(curJoinable.getPhysicalName());
                        						// check if key is present in this block
                        						if (rightRepresentNameMap.containsKey(curExp.getName())) {
                        							// first match
                        							rightBlockAlias = tableAlias.assignAlias(curJoinable);
                        							break;
                        						}
                        					}
                                        } else if (targetJoinable != null) {
                                            rightRepresentNameMap = blockAliasMap.get(targetJoinable.getPhysicalName());
                                            rightBlockAlias = tableAlias.assignAlias(targetJoinable);
                                        } else {
                                            // targetJoinable == null
                                            if (curExp instanceof BlockDerivedEntity) {
                                                // curExp is a BlockDerivedDimension
                                                // 20160623: quick hack to bdd
                                                String bddSlug = curExp.getSlug();
                                                BlockContext blockContext = sqlEngineContext.getBlockContext(dataBlock.getPhysicalName());
                                                for (SelectItem selectItem : blockContext.getSelectItems()) {
                                                    if (bddSlug.equals(selectItem.getAlias().replaceAll("\"", ""))) {
                                                        selfGen = true;
                                                        selfGenName = selectItem.getKey();
                                                        break;
                                                    }
                                                }
                                                                     
                                                if (selfGen == false) {
                                                    rightRepresentNameMap = blockAliasMap.get(fromBlock.getPhysicalName());
                                                    rightBlockAlias = tableAlias.assignAlias(fromBlock);
                                                }
                                            } else {
                                                rightRepresentNameMap = blockAliasMap.get(fromBlock.getPhysicalName());
                                                rightBlockAlias = tableAlias.assignAlias(fromBlock);
                                            }                                        
                                        }
        
                                        leftRepresentName = targetBlockRepresentNameMap.get(curExp.getName());
                                        leftRepresentName = SqlEngineUtils.getQuotedString(leftRepresentName);
                                        if (selfGen == false) {
                                            rightRepresentName = rightRepresentNameMap.get(curExp.getName());
                                            rightRepresentName = SqlEngineUtils.getQuotedString(rightRepresentName);
                                        }
                					} else {
                						// from query block join query block
                						rightRepresentNameMap = blockAliasMap.get(fromBlock.getPhysicalName());
                						rightRepresentName = rightRepresentNameMap.get(curExp.getName());
                						rightRepresentName = SqlEngineUtils.getQuotedString(rightRepresentName);
                						rightBlockAlias = tableAlias.assignAlias(fromBlock);
                						leftRepresentName = targetBlockRepresentNameMap.get(curExp.getName());
                						leftRepresentName = SqlEngineUtils.getQuotedString(leftRepresentName);
                					}
                                    if (selfGen == false) {
                                        // 20160617: fix VB-545
                                        coalesceMap.addName(curExp,  rightBlockAlias.concat(".".concat(rightRepresentName)));
                                    } else {
                                        // 20160617: fix VB-545
                                        coalesceMap.addName(curExp,  selfGenName);                                    
                                    }
                                }
        
                				if (onCount > 0) {
                					oneJoin = oneJoin.concat(" AND ");
                				}
        
                				name = targetBlockAlias.concat(".").concat(leftRepresentName);
                				coalesceMap.addName(curExp, name);
                				if (coalesceSql == null) {
                				    if (selfGen == false) {
                				        oneJoin = oneJoin.concat(name).concat("=").concat(rightBlockAlias).concat(".").concat(rightRepresentName);
                				    } else {
                				        oneJoin = oneJoin.concat(name).concat("=").concat(selfGenName);
                				    }
                				} else {
                					oneJoin = oneJoin.concat(name).concat("=").concat(coalesceSql);
                				}
        
                				onCount++;
        
                				joinKeyMapBlocks.add(targetBlock);
                            } else {
                				// missing
                				coalesceMap.addBlock(curExp, targetBlock);
                			}
                        }
                    }
                }

                allJoins.add(oneJoin);
                fromList = fromList.concat(oneJoin);
		    }

		    System.out.println(coalesceMap);

		    coalesceMap.keySet().forEach(expressible -> listCoalesceSets.add(coalesceMap.getNames(expressible)));
        }

        if (fromList.length() > 0) {
            retStr = genCustomString("FROM", fromList);
        }

        return retStr;
    }

    // 20150203 ToDo: bug here table join, the representNameMap doesn't have the join item because the item is not in the select list.
    private String genFromStringForOneItemMergeBlock(
        IJoinExpression jDef,
        DataBlock dataBlock,
        TableAlias tableAlias) {
        String joinExpression = jDef.getJoinFormula();
        String leftJoinableAlias = tableAlias.assignAlias(jDef.getLeft());
        String rightJoinableAlias = tableAlias.assignAlias(jDef.getRight());

        IJoinable leftJoinable = jDef.getLeft();
        IJoinable rightJoinable = jDef.getRight();
        Map<String, String> leftRepresentNameMap = blockAliasMap.get(jDef.getLeft().getPhysicalName());
        Map<String, String> rightRepresentNameMap = blockAliasMap.get(jDef.getRight().getPhysicalName());
        if (leftJoinable instanceof Table) {
            leftRepresentNameMap = blockAliasMap.get(dataBlock.getPhysicalName());
        } else {
            leftRepresentNameMap = blockAliasMap.get(jDef.getLeft().getPhysicalName());
        }

        if (rightJoinable instanceof Table) {
            rightRepresentNameMap = blockAliasMap.get(dataBlock.getPhysicalName());
        } else {
            rightRepresentNameMap = blockAliasMap.get(jDef.getRight().getPhysicalName());
        }

        List<String> matchedNames = extractVeroIdents(joinExpression);

        String newJoinExpression = new String(joinExpression);

        assert(matchedNames.size()%2 == 0);

        for (int i=0; i<matchedNames.size(); i++) {
    		String virtualName = FormulaUtils.parseBlockDerivedName(matchedNames.get(i))[1];
    		String replaceName = null;
    
    		if (i%2 == 0) {
    			// left
    			replaceName = leftJoinableAlias.concat(".").concat(leftRepresentNameMap.get(virtualName));
    		} else {
    			// 1%2 == 1, right
    			replaceName = rightJoinableAlias.concat(".").concat(rightRepresentNameMap.get(virtualName));
    		}
    		newJoinExpression = newJoinExpression.replaceFirst(Pattern.quote(matchedNames.get(i)), replaceName);
        }

        return newJoinExpression;
    }

    private Boolean needProcessMeasureInsideWhere(DataBlock dataBlock) {
        Boolean process = false;

        if (dataBlock instanceof ResultBlock) {
            process = true;
        } else if (dataBlock instanceof SetBlock) {
            if (!((SetBlock) dataBlock).getChildren().isEmpty()) {
                process = true;
            }
        }

        return process;
    }

    private String genWhereString(
        Report report,
        DataBlock dataBlock,
        TableAlias tableAlias,
        Boolean needReaggregate,
        StringBuilder fromBuilder) throws Exception {
        BlockFilter originalFilter = dataBlock.getBlockFilter();
        String retStr = "";

		if (isDataBlockEmpty(dataBlock)) {
			return "";
		}

        if (originalFilter == null) {
            return "";
        }

        if (ignoreFilter(dataBlock) == true) {
            return "";
        }

        BlockFilter filterForWhere = originalFilter.duplicateFilter();
        filterForWhere.preprocessFilter();
        Map<Integer, FilterExpression> patchedResult = originalFilter.getPatchedResult();
        IQueryPlan queryPlan = dataBlock.getPlan();

        // take care of where - all existed dimensions
        Iterator<FilterNode<Expression>> it = filterForWhere.getRoot().iterator();
        Set<Dimension> planDimensions = queryPlan.getPlanDimensions();

        // first pass: remove unnecessary nodes
        while (it.hasNext()) {
            FilterNode<Expression> curNode = it.next();
            if (!curNode.isOperator()) {
                Expression curFilterExp = curNode.getData();
                Boolean remove = true;
                Integer key = new Integer(curNode.getSerial());
                Expression patchedExp = patchedResult.get(key);

                if (patchedExp != null) {
                    // patched by SetBlock or Segment
                    for (ExpressionRef<? extends VeroBase> curExpRef : patchedExp.getReferences()) {
                        // 20140904: check if there is any dimension that can be reached by the plan.
                        VeroObj veroObj = curExpRef.getReference();
                        if (veroObj instanceof Dimension) {
                            Dimension curDim = (Dimension) veroObj;
                            if (planDimensions.contains(curDim)) {
                                // found at least one
                                remove = false;
                                break;
                            }
                        }
                    }
                } else {
        			if (ignoreFilterNode(dataBlock, 0, curNode.getSerial()) == true) {
        				System.out.println("Ignore node for where: " + curNode.getSerial());
        				remove = true;
        			} else {        			    
	                    for (ExpressionRef<? extends VeroBase> curExpRef : curFilterExp.getReferences()) {
	                        Boolean isBlockRef = false;
	                        if (curExpRef instanceof BlockRef) {
	                            isBlockRef = true;
	                        }
	                        
	                        VeroObj veroObj = curExpRef.getReference();
	                        
                            if (!isBlockRef && veroObj instanceof Measure) {
                                if (needProcessMeasureInsideWhere(dataBlock)) {
                                    Measure curMea = (Measure) veroObj;
                                    if (isExpressibleReachableByPlan(queryPlan, curMea)) {
                                        IJoinable curJoinable = queryPlan.getJoinable(curMea);
                                        // 20140810 in result block, treat measure from other blocks as dimensions when there is no re-aggregate.
                                        if (((curJoinable instanceof DataBlock) || (curJoinable instanceof VirtualJoinable)) && (needReaggregate == false)) {
                                            // good
                                            remove = false;
                                        } else {
                                            // bad
                                            remove = true;
                                            break;
                                        }
                                    } else {
                                        // bad
                                        remove = true;
                                        break;
                                    }
                                } else {
                                    // bad
                                    remove = true;
                                    break;
                                }
                            } else if (!isBlockRef && veroObj instanceof Dimension) {
                                Dimension curDim = (Dimension) veroObj;
                                if (isExpressibleReachableByPlan(queryPlan, curDim)) {
                                    if (dataBlock instanceof ResultBlock) {
                                        IJoinable curJoinable = queryPlan.getJoinable(curDim);
                                        if (curJoinable instanceof CoalesceJoinable) {
                                            // 20141018: VB-5 CoalesceJoinable is taken care of in other blocks, ignore it here
                                            System.out.println("Where node removed due to ResultBlock + CoalesceJoinable");
                                            remove = true;
                                            break;
                                        }
                                    }

                                    // good
                                    remove = false;
                                } else {
                                    // bad
                                    remove = true;
                                    break;
                                }
                            } else if (!isBlockRef && (veroObj instanceof BlockDerivedEntity)) {
                                if (dataBlock instanceof QueryBlock) {
                                    BlockDerivedEntity curBlockDerivedEntity = null;
                                    if (veroObj instanceof BlockDerivedMeasure) {
                                        if (needReaggregate == true) {
                                            // check if re-aggregate is needed, if needed, treated as a measure; otherwise, treat it as a dimension
                                            remove = true;
                                            break;
                                        } else {
                                            curBlockDerivedEntity = findBlockDerivedMeasure((QueryBlock) dataBlock, veroObj.getRID());
                                        }
                                    } else {
                                        // BLOCK_DERIVED_DIMENSION
                                        curBlockDerivedEntity = findBlockDerivedDimension((QueryBlock) dataBlock, veroObj.getRID());
                                    }
                                    if (curBlockDerivedEntity != null) {
                                        System.out.println("genWhere BlockDerivedEntity found for id: " + veroObj.getRID());
                                        remove = false;
                                        break;
                                    } else {
                                        remove = true;
                                        break;
                                    }
                                } else {
                                    remove = true;
                                    break;
                                }                          
                            } else if (curExpRef instanceof BlockRef) {
                                System.out.println("genWhere BlockRef searching for id: " + veroObj.getRID());
                                if (dataBlock instanceof QueryBlock) {
                                    Expressible curExpressible = (Expressible) curExpRef.getReference();
                                    if ((curExpressible instanceof Measure) || (curExpressible instanceof BlockDerivedMeasure)) {
                                        if (needReaggregate == true) {
                                            remove = true;
                                            break;
                                        } else {
                                            remove = false;
                                            break;
                                        }
                                    } else {
                                        remove = false;
                                        break;
                                    }
                                } else {
                                    remove = true;
                                    break;
                                }
                            } else if (veroObj instanceof QueryBlock) {
                                // 20141003 ToDo: review the logic that removes QUERYBLOCK node when it's result block with more than 1 engine block
                                System.out.println("genWhere QueryBlock searching for id: " + veroObj.getRID());
                                if ((dataBlock instanceof ResultBlock) && ((ResultBlock) dataBlock).getEngineBlocks().size() != 0) {
                                    remove = true;
                                    break;
                                }

                                // check all dimensions in it and remove if all keys are not in the plan
                                for (ExpressionRef<? extends VeroBase> curInnerExpRef : curFilterExp.getReferences()) {
                                    VeroObj curInnerVeroObj = curInnerExpRef.getReference();
                                    
                                    if (curInnerVeroObj instanceof Dimension) {
                                        if (planDimensions.contains(curInnerVeroObj)) {
                                            // found at least one
                                            remove = false;
                                            break;
                                        }
                                    } else if (curInnerVeroObj instanceof QueryBlock) {
                                        continue;
                                    } else {
                                        remove = true;
                                        break;
                                    }
                                }

                                break;
                            }
	                    }
        			}
                }

                if (remove == true) {
                    filterForWhere.removeNode(curNode);
                }
            }
        }

        // second pass: process the nodes that are left
        genFilter(
            report,
            dataBlock,
            filterForWhere,
            patchedResult,
            tableAlias,
            false, // doens't need to re-aggreagate because they are all dimensions
            fromBuilder);

        // third pass: gather items for where
        dataBlock.setWhereMap(gatherItemsInFilter(filterForWhere));

		Iterator<FilterNode<Expression>> itt = filterForWhere.getRoot().iterator();
        while (itt.hasNext()) {
            FilterNode<Expression> curNode = itt.next();
            System.out.println("serial = " + curNode.getSerial());
        }

        // fourth pass: gather node usage info
        BlockFilter masterFilter = findSourceBlock(dataBlock).getBlockFilter();
        FilterContext filterContext = sqlEngineContext.getFilterContext(masterFilter.getRID());
        if (filterContext == null) {
    		sqlEngineContext.addFilterContext(masterFilter);
    		filterContext = sqlEngineContext.getFilterContext(masterFilter.getRID());
        }
        filterContext.calUsageCountForWhere(filterForWhere);


        //System.out.println("B====> " + filterForWhere.toString());
        //System.out.println("B-toSql==> " + genFilterStringForClause(filterForWhere.getRoot()));
        filterForWhere.preprocessFilter();
        String stringForWhere = genStringForFilter(filterForWhere.getRoot());

        if (stringForWhere.length() > 0) {
            retStr = genCustomString("WHERE", stringForWhere);
        }
        return retStr;
    }

    private String genHavingString(
        DataBlock dataBlock,
        TableAlias tableAlias,
        Boolean needReaggregate) throws Exception {
        BlockFilter originalFilter = dataBlock.getBlockFilter();
        String retStr = "";

		if (isDataBlockEmpty(dataBlock)) {
			return "";
		}

        if (originalFilter == null) {
            return "";
        }

        if (ignoreFilter(dataBlock) == true) {
            return "";
        }

        BlockFilter filterForHaving = originalFilter.duplicateFilter();
        filterForHaving.preprocessFilter();
        Map<Integer, FilterExpression> patchedResult = originalFilter.getPatchedResult();
        IQueryPlan queryPlan = dataBlock.getPlan();

        // take care of having - all existed measures or mixed measure and dimension
        Iterator<FilterNode<Expression>> it = filterForHaving.getRoot().iterator();
        Set<Measure> planMeasures = queryPlan.getPlanMeasures();

        // first pass: remove unnecessary nodes
        while (it.hasNext()) {
            FilterNode<Expression> curNode = it.next();
            if (!curNode.isOperator()) {
                Expression curFilterExp = curNode.getData();
                Boolean remove = true;
                Integer key = new Integer(curNode.getSerial());
                Expression patchedExp = patchedResult.get(key);

                //20140905: if found the patched result, it means it is a set or segment filter which we can remove here.
                if (patchedExp != null) {
                    remove = true;
                } else {
        			if (ignoreFilterNode(dataBlock, 1, curNode.getSerial()) == true) {
        				System.out.println("Ignore node for having: " + curNode.getSerial());
        				remove = true;
        			} else {
                        for (ExpressionRef<? extends VeroBase> curExpRef : curFilterExp.getReferences()) {
                            Boolean isBlockRef = false;
                            if (curExpRef instanceof BlockRef) {
                                isBlockRef = true;
                            }
                            
                            VeroObj veroObj = curExpRef.getReference();
                            
                            // curExRef is ExpressionRef
                            if (!isBlockRef && veroObj instanceof Measure) {
                                Measure curMea = (Measure) veroObj;
                                if (planMeasures.contains(curMea)) {
                                    IJoinable curJoinable = queryPlan.getJoinable(curMea);

                                    // 20140904 ToDo: need to review the following logic
                                    if (((curJoinable instanceof DataBlock) || (curJoinable instanceof VirtualJoinable)) && (needReaggregate == false)) {
                                        if (needProcessMeasureInsideWhere(dataBlock) == true) {
                                            // bad
                                            remove = true;
                                            break;
                                        } else {
                                            // good
                                            remove = false;
                                        }
                                    } else {
                                        // good
                                        remove = false;
                                    }
                                } else {
                                    // bad
                                    remove = true;
                                    break;
                                }
                            } else if (!isBlockRef && veroObj instanceof Dimension) {
                                // bad
                                remove = true;
                                break;
                            } else if (!isBlockRef && veroObj instanceof BlockDerivedEntity) {
                                System.out.println("genHaving BlockDerivedEntity searching for id: " + veroObj.getRID());
                                if (dataBlock instanceof QueryBlock) {
                                    BlockDerivedEntity curBlockDerivedEntity = null;
                                    if (veroObj instanceof BlockDerivedMeasure) {
                                        if (needReaggregate == false) {
                                            // check if re-aggregate is needed, if needed, treated as a measure; otherwise, treat it as a dimension
                                            remove = true;
                                            break;
                                        } else {
                                            curBlockDerivedEntity = findBlockDerivedMeasure((QueryBlock) dataBlock, veroObj.getRID());
                                        }
                                    } else {
                                        // BLOCK_DERIVED_DIMENSION
                                        remove = true;
                                        break;
                                    }

                                    if (curBlockDerivedEntity != null) {
                                        System.out.println("genHaving BlockDerivedEntity found for id: " + veroObj.getRID());
                                        remove = false;
                                        break;
                                    } else {
                                        remove = true;
                                        break;
                                    }
                                } else {
                                    remove = true;
                                    break;
                                }
                            } else if (curExpRef instanceof BlockRef) {
                                // 20141209 ToDo: not sure if the logic of when to remove it is correct
                                System.out.println("genHaving BlockRef searching for id: " + veroObj.getRID());
                                if (dataBlock instanceof QueryBlock) {
                                    Expressible curExpressible = (Expressible) curExpRef.getReference();
                                    if ((curExpressible instanceof Measure) || (curExpressible instanceof BlockDerivedMeasure)) {
                                        if (needReaggregate == false) {
                                            // check if re-aggregate is needed, if needed, treated as a measure; otherwise, treat it as a dimension
                                            remove = true;
                                            break;
                                        } else {
                                            remove = false;
                                            break;
                                        }
                                    } else {
                                        remove = true;
                                        break;
                                    }
                                } else {
                                    remove = true;
                                    break;
                                }
                            } else if (veroObj instanceof QueryBlock) {
                                // QUERYBLOCK is ignored in HAVING
                                remove = true;
                                break;
                            }
	                    }
        			}
                }

                if (remove == true) {
                    filterForHaving.removeNode(curNode);
                }
            }
        }

        // second pass: process the nodes that are left
        genFilter(
            null,
            dataBlock,
            filterForHaving,
            patchedResult,
            tableAlias,
            needReaggregate,
            null);

        // third pass: gather items for having
        dataBlock.setHavingMap(gatherItemsInFilter(filterForHaving));

        // forth pass: gather node usage info
        BlockFilter masterFilter = findSourceBlock(dataBlock).getBlockFilter();
        FilterContext filterContext = sqlEngineContext.getFilterContext(masterFilter.getRID());
        if (filterContext == null) {
            sqlEngineContext.addFilterContext(masterFilter);
            filterContext = sqlEngineContext.getFilterContext(masterFilter.getRID());
        }
        filterContext.calUsageCountForWhere(filterForHaving);

        //System.out.println("B====> " + filterForHaving.toString());
        //System.out.println("B-toSql==> " + genFilterStringForClause(filterForHaving.getRoot()));
        filterForHaving.preprocessFilter();
        //System.out.println("A====> " + filterForHaving.toString());
        String stringForHaving = genStringForFilter(filterForHaving.getRoot());

        if (stringForHaving.length() > 0) {
            retStr = genCustomString("HAVING", stringForHaving);
        }

        return retStr;
    }

    private String genStringForFilter(FilterNode<Expression> node) {
        String retString = "";

        String operator = node.getOperatorName();
        int childrenSize = node.getChildren().size();

        //System.out.println("node = " + node + "childrensize = " + childrenSize);
        for (int i=0; i<childrenSize; i++) {
            FilterNode<Expression> curNode = node.getChildren().get(i);

            if (i == 0) {
                //System.out.print("(");
                retString = retString.concat("(");
            }

            if ((i != 0) && (!curNode.isOperator() || (curNode.isOperator() && (curNode.hasChildren())))) {
                //System.out.print(" " + operator + " ");
                retString = retString.concat(" " + operator + " ");
            }

            if (curNode.hasChildren()) {
                // non-leaf
            } else {
                // can be an empty operator node
                if (curNode.isOperator()) {
                    // do nothing
                } else {
                    Expression curExp = curNode.getData();
                    retString = retString.concat(curExp.getPatchedFormula());
                }
            }

            retString = retString.concat(genStringForFilter(curNode));

            if (i == childrenSize-1) {
                //System.out.print(")");
                retString = retString.concat(")");
            }
        }

        return retString;
    }

    private String genGroupByString(
		DataBlock dataBlock) {
        String retStr = "";

        BlockContext bc = sqlEngineContext.getBlockContext(dataBlock.getPhysicalName());
        List<String> groupBys = bc.getSelectItems().stream()
            .filter(selectItem -> selectItem.getType() == VeroType.DIMENSION).map(selectItem -> selectItem.getKey()).collect(Collectors.toList());

        if (groupBys.size() > 0) {
            Joiner joiner = Joiner.on(", ").skipNulls();
            retStr = genCustomString("GROUP BY", joiner.join(groupBys));
        }

        return retStr;
    }
    
    private String genOrderByString(DataBlock dataBlock) {
        String retStr = "";

        Map<Expressible, OrderByType> orderBys = dataBlock.getOrderByMap();
        Joiner orderByJoiner = Joiner.on(", ").skipNulls();
        
        if (dataBlock instanceof QueryBlock) {
            BlockContext bc = sqlEngineContext.getBlockContext(dataBlock.getPhysicalName());
            List<SelectItem> selectItems = bc.getSelectItems();
            List<String> orderByStrings = new ArrayList<String>();
            for (Expressible expressible : orderBys.keySet()) {
            	String id = expressible.getRID();
                for (SelectItem selectItem : selectItems) {
                    if (id.equals(selectItem.getId())) {
                    	OrderByType type = orderBys.get(expressible);
                        if (type == OrderByType.DESC) {
                            orderByStrings.add(selectItem.getKey().concat(" ").concat("DESC"));    
                        } else {
                        	// default
                            orderByStrings.add(selectItem.getKey().concat(" ").concat("ASC"));
                        }
                    }
                }
            }

            if (orderByStrings.size() > 0) {
                retStr = genCustomString("ORDER BY", orderByJoiner.join(orderByStrings));
            }
        }
        
        return retStr;
    }

    private Boolean needsGroupBy(
        DataBlock dataBlock,
        String having) {
        Boolean needsGroupBy = true;

        if (dataBlock.isForceGroupBy() == true) {
            return true;
        }
        
        BlockContext bc = sqlEngineContext.getBlockContext(dataBlock.getPhysicalName());
        int dimCount = 0;
        int meaCount = 0;
        for (SelectItem selectItem : bc.getSelectItems()) {
            if (selectItem.getType() == VeroType.DIMENSION) { dimCount++; }
            else if (selectItem.getType() == VeroType.MEASURE) { meaCount++; }
        }

        if (!((meaCount != 0) && (dimCount != 0))) {
            if (dataBlock instanceof SetBlock) {
                // 20140904: for SetBlock, all dimensions need to be in the groupby regardless of measure existence.
            } else {
                if (having.length() != 0) {
                    // 20150227: there is having, implying that there is aggregation, implying that it is a measure and the groupby should be kept
                } else {
                    needsGroupBy = false;
                }
            }
        }

        return needsGroupBy;
    }

    private String genCustomString(
        String prefix,
        String inplace) {
        return "".concat(prefix).concat(" ").concat(inplace).concat("\n");
    }

    private String translateSql(DBType dbType, Node sqlTree) {
        if (dbType == null) { return FormatterFactory.getSqlFormatter().formatSql(sqlTree); }
        else { return FormatterFactory.getSqlFormatter(dbType).formatSql(sqlTree); }
    }

    private void genFilter(
        Report report,
        DataBlock dataBlock,
        BlockFilter blockFilter,
        Map<Integer, FilterExpression> patchedResult,
        TableAlias tableAlias,
        Boolean needReaggregate,
        StringBuilder fromBuilder) throws Exception {
        IQueryPlan queryPlan = dataBlock.getPlan();
        Iterator<FilterNode<Expression>> it = blockFilter.getRoot().iterator();
        while (it.hasNext()) {
            FilterNode<Expression> curNode = it.next();
            if (!curNode.isOperator()) {
                Expression patchedExp = patchedResult.get(new Integer(curNode.getSerial()));
                Expression curFilterExp = null;

                // switch between exp
                if (patchedExp != null) {
                    curFilterExp = patchedExp;
                } else {
                    curFilterExp = curNode.getData();

                }

                VeroObj veroObj = curFilterExp.getReferences().get(0).getReference();
                if ((patchedExp != null) || (veroObj instanceof QueryBlock)) {
                    SetBlockContent setBlockContent = SetBlockParser.parse(curFilterExp.getFormula());

                    if (setBlockContent == null) {
            			// parsing has failed
            			continue;
                    }

                    SetBlockContent.Type type = setBlockContent.getType();
                    String setBlockName = setBlockContent.getBlock();
                    String includeExcludBlockId = null;

                    if (veroObj instanceof SetBlock) {
                        includeExcludBlockId = ((SetBlock) veroObj).getPhysicalName();
                    } else {
                        // QUERYBLOCK
                        includeExcludBlockId = ((QueryBlock) veroObj).getPhysicalName();
                        // find the block name
                        for (QueryBlock curQueryBlock : report.getQueryBlocks()) {
                            if (curQueryBlock.getPhysicalName().equals(includeExcludBlockId)) {
                                // found
                                setBlockName = curQueryBlock.getPhysicalName();
                                break;
                            }
                        }
                    }

                    System.out.println("SetBlock or QuertBlock: " + curFilterExp.getFormula() + " type= " + type + " block= " + setBlockName);

                    String finalPatchedString = new String();
                    String replaceSubqueryJoinString = new String();
                    String replaceSubqueryWhereString = new String();
                    String blockAlias = tableAlias.assignAlias(setBlockName);

                    if (type == SetBlockContent.Type.INCLUDE) {
                        finalPatchedString = finalPatchedString.concat("exists ");
                    } else if (type == SetBlockContent.Type.EXCLUDE) {
                        finalPatchedString = finalPatchedString.concat("not exists ");
                    }

                    finalPatchedString = finalPatchedString.concat("(select * from ").concat(setBlockName).concat(" ").concat(blockAlias).concat(" where ");
                    replaceSubqueryJoinString = replaceSubqueryJoinString.concat(" LEFT OUTER JOIN ").concat(setBlockName).concat(" ").concat(blockAlias).concat(" ON ");

                    int count = 0;
                    String whereString = new String();
                    
                    for (ExpressionRef<? extends VeroBase> curExpRef : curFilterExp.getReferences()) {
                        veroObj = curExpRef.getReference();
                        
                        if ((veroObj instanceof Dimension) || (veroObj instanceof Measure)) {
                            if (isExpressibleReachableByPlan(queryPlan, (Expressible) veroObj)) {
                                if (count > 0) {
                                    whereString = whereString.concat(" and ");
                                }

                                /*
                                Joinable curJoinable = queryPlan.getJoinable(curExpressible);
                                String tableAliasName = tableAlias.getAlias().get(curJoinable.getId());
                                System.out.println("Table alias==> " + tableAliasName);
                                */

                                String itemToSql = genStringForExpressible(
                                    queryPlan,
                                    (Expressible) veroObj,
                                    tableAlias,
                                    null,
                                    null,
                                    null,
                                    null,
                                    false,
                                    false);

                                whereString = whereString.concat(itemToSql).concat("=");

                                // the include/exclude block
                                Map<String, String> representNameMap = blockAliasMap.get(includeExcludBlockId);
                                String expression = representNameMap.get(((Expressible) veroObj).getName());
                                //System.out.println("Current Expression==> " + expression);

                                itemToSql = patchFormulaWithAlias(expression, blockAlias);
                                //System.out.println("itemToSql==> " + itemToSql);

                                whereString = whereString.concat(itemToSql);

                                if (count == 0) {
                                    replaceSubqueryWhereString = replaceSubqueryWhereString.concat(itemToSql);
                                }

                                count++;
                            }
                        }
                    }
                    if (type == SetBlockContent.Type.INCLUDE) {
                        replaceSubqueryWhereString = replaceSubqueryWhereString.concat(" is not null");
                    } else if (type == SetBlockContent.Type.EXCLUDE) {
                        replaceSubqueryWhereString = replaceSubqueryWhereString.concat(" is null");
                    }

                    replaceSubqueryJoinString = replaceSubqueryJoinString.concat(whereString);

                    if (count > 0) {
                        whereString = whereString.concat(")");
                    }

                    finalPatchedString = finalPatchedString.concat(whereString);
                    System.out.println("finalPatchedString==>" + finalPatchedString);
                    System.out.println("replaceSubqueryJoinString==>" + replaceSubqueryJoinString);
                    System.out.println("replaceSubqueryWhereString==>" + replaceSubqueryWhereString);

                    // update patchedFormula from patchedResult to original tree
                    if (sqlEngineContext.getVirtualDB().supportsSubqueriesInExist() == false) {
                        curNode.getData().setPatchedFormula(replaceSubqueryWhereString);
                        if (fromBuilder != null) {
                            fromBuilder = fromBuilder.append(" ").append(replaceSubqueryJoinString);
                        }
                    } else {
                        curNode.getData().setPatchedFormula(finalPatchedString);
                    }
                } else {
                    // not from SETBLOCK or QUERY_BLOCK

                    // 20150709: parse it to let pass() fix work (to be able to separate vero idents from others)
                    //String patchedFormula = curFilterExp.getFormula();
                    String patchedFormula = FormatterFactory.getSqlFormatter().formatSql(VeroSqlParser.createExpression(curFilterExp.getFormula()));

                    List<String> matched = extractVeroIdents(patchedFormula);

                    for (int i=0; i<curFilterExp.getReferences().size(); i++) {
                        ExpressionRef<? extends VeroBase> curExpRef = findExpressionReference(curFilterExp.getReferences(), i);
                        Boolean isBlockRef = false;
                        if (curExpRef instanceof BlockRef) {
                            isBlockRef = true;
                        }
                        
                        veroObj = curExpRef.getReference();
                        
                        // curExpRef is ExpressionRef
                        if (!isBlockRef && ((veroObj instanceof Dimension) || (veroObj instanceof Measure))) {
                            if (!isExpressibleReachableByPlan(queryPlan, (Expressible) veroObj)) {
                                String pattern = ",".concat(matched.get(i));
                                patchedFormula = patchedFormula.replaceFirst(Pattern.quote(pattern), "");
                                pattern = "".concat(matched.get(i));
                                patchedFormula = patchedFormula.replaceFirst(Pattern.quote(pattern), "");
                            } else {
                                Expression curUsedExp = queryPlan.getExpression((Expressible) veroObj);
                                IJoinable curJoinable = queryPlan.getJoinable((Expressible) veroObj);

                                String itemToSql = genStringForExpressible(
                                    queryPlan,
                                    (Expressible) veroObj,
                                    tableAlias,
                                    null,
                                    null,
                                    null,
                                    null,
                                    false,
                                    false);

                                String replace;
                                if (curJoinable instanceof DataBlock) {
                                    replace = curUsedExp.getFormula();
                                } else {
                                    // table
                                    replace = matched.get(i);
                                }
                                // 20150709: since formula is parsed, when we replace, we need to ignore outside quotes
                                //patchedFormula = patchedFormula.replaceFirst(Pattern.quote(replace), itemToSql);
                                patchedFormula = replaceFirstAvoidOutsideQuotes(patchedFormula, Pattern.quote(replace), itemToSql);
                            }

                            curFilterExp.setPatchedFormula(patchedFormula);
                            System.out.println("Patched formula==> " + patchedFormula);
                        } else if (!isBlockRef && veroObj instanceof BlockDerivedEntity) {
                            System.out.println("genFilter processing for BLOCK_DERIVED_ENTITY..." + patchedFormula);

                            String itemToSql = genStringForBlockDerivedEntity(
                                queryPlan,
                                (BlockDerivedEntity) veroObj,
                                tableAlias,
                                null,
                                null,
                                null,
                                null,
                                false,
                                needReaggregate);

                            String blockRefName = new String().concat(matched.get(i));

                            // 20150709: since formula is parsed, when we replace, we need to ignore outside quotes
                            //patchedFormula = patchedFormula.replaceFirst(Pattern.quote(blockRefName), itemToSql);
                            patchedFormula = replaceFirstAvoidOutsideQuotes(patchedFormula, Pattern.quote(blockRefName), itemToSql);
                            curFilterExp.setPatchedFormula(patchedFormula);
                        } else if (curExpRef instanceof BlockRef) {
                            System.out.println("genFilter processing for BLOCK_REF..." + patchedFormula);
                            String itemToSql = genStringForBlockRef(
                                queryPlan,
                                matched.get(i),
                                tableAlias,
                                (BlockRef<? extends Expressible>) curExpRef,
                                null,
                                false,
                                needReaggregate);

                            // 20150709: since formula is parsed, when we replace, we need to ignore outside quotes
                            //patchedFormula = patchedFormula.replaceFirst(Pattern.quote(bRefPattern), itemToSql);
                            patchedFormula = replaceFirstAvoidOutsideQuotes(patchedFormula, Pattern.quote(matched.get(i)), itemToSql);
                            curFilterExp.setPatchedFormula(patchedFormula);
                        } else {
                            // 20141107 ToDo: more types
                            System.out.println("Type not supported!!!");
                        }
                    }
                }
            }
        }
    }

    private Boolean ignoreFilter(DataBlock dataBlock) {
        // 20141001: for ResultBlock that is used to resolve SetBlocks and having EngineBlocks, ignore filter
        // 20141202: VB-80: for QueryBlock that is used to resolve SetBlocks and having EngineBlocks, ignore filter
        if (dataBlock instanceof QueryBlock) {
            QueryBlock queryBlock = (QueryBlock) dataBlock;

            if ((queryBlock.getEngineBlocks().size() != 0) && (queryBlock.getSetBlocks().size() != 0)) {
                System.out.println("Note: filter has been ignored...");
                return true;
            }
        }

        return false;
    }

    private Boolean needReaggregate(
        DataBlock sourceBlock) {
        // determine if re-aggregate is needed
        if (sourceBlock instanceof QueryBlock) {
            if (((QueryBlock) sourceBlock).getQueryBlockJoins() != null) {
                Set<String> sourceDimIds = new HashSet<String>();

                for (BlockToBlockJoin curB2BJoin : ((QueryBlock) sourceBlock).getQueryBlockJoins()) {
                    DataBlock targetBlock = curB2BJoin.getTarget();

                    List<Dimension> sourceDims = sourceBlock.getSelectDimensions();
                    List<BlockDerivedDimension> srcDerivedDims = null;
                    if (sourceBlock instanceof QueryBlock) {
                        srcDerivedDims = ((QueryBlock) sourceBlock).getBlockDerivedDimensions();
                    }

                    List<Dimension> targetDims = targetBlock.getSelectDimensions();
                    List<BlockDerivedDimension> tgtDerivedDims = null;
                    if (targetBlock instanceof QueryBlock) {
                        tgtDerivedDims = ((QueryBlock) targetBlock).getBlockDerivedDimensions();
                    }
                    Set<String> targetDimIds = new HashSet<String>();

                    targetDims.forEach(targetDim -> targetDimIds.add(targetDim.getName()));
                    sourceDims.forEach(sourceDim -> sourceDimIds.add(sourceDim.getName()));

                    // 20160202: adding dimension from target bdd
                    for (BlockDerivedDimension tgtSrcDerivedDim : tgtDerivedDims) {
                        //System.out.println("Adding bdd: " + curDerivedDim.getFormula());
                        List<String> uniqueIds = new ArrayList<String>();
                        
                        List<BlockRef<? extends Expressible>> innerBlockRefs = tgtSrcDerivedDim.getBlockRefs();
                        
                        if (innerBlockRefs.size() == 1) {
                            // TODO: 20160202: only take care of simple bdd. complex one (ie. x+y) not supported yet
                            for (BlockRef<? extends Expressible> innerBlockRef : innerBlockRefs) {                            
                                VeroObj veroObj = innerBlockRef.getReference();
    
                                //System.out.println("Looping expprop: " + curExpProp.getType() + " " + curExpProp.getTargetObjectId());
                                if (veroObj instanceof Dimension) {
                                    //System.out.println("Added expprop: " + curExpProp.getType() + " " + curExpProp.getTargetObjectId());
                                    uniqueIds.add(((Dimension) veroObj).getName());
                                //} else if (veroObj instanceof BlockRef) {
                                } else if (innerBlockRef instanceof BlockRef) {
                                    Expressible curExpressible = innerBlockRef.getReference();
                                    
                                    if (curExpressible instanceof Dimension) {
                                        uniqueIds.add(curExpressible.getName());
                                    }
                                }
                            }

                            targetDimIds.addAll(uniqueIds);
                        }
                    }
                    
                    // 20150224: Fix VB-171 & VB-186: adding dimension from source bdd
                    for (BlockDerivedDimension curSrcDerivedDim : srcDerivedDims) {
                        //System.out.println("Adding bdd: " + curDerivedDim.getFormula());
                        int numTargetMatch = 0;
                        List<String> uniqueIds = new ArrayList<String>();
                        
                        List<BlockRef<? extends Expressible>> innerBlockRefs = curSrcDerivedDim.getBlockRefs();
                        for (BlockRef<? extends Expressible> innerBlockRef : innerBlockRefs) {
                            VeroObj veroObj = innerBlockRef.getReference();

                            //System.out.println("Looping expprop: " + curExpProp.getType() + " " + curExpProp.getTargetObjectId());
                            if (veroObj instanceof Dimension) {
                                //System.out.println("Added expprop: " + curExpProp.getType() + " " + curExpProp.getTargetObjectId());
                                String id = ((Dimension) veroObj).getName();
                                if (targetDimIds.contains(id)) {
                                    numTargetMatch++;
                                }
                                uniqueIds.add(id);
                            //} else if (veroObj instanceof BlockRef) {
                            } else if (innerBlockRef instanceof BlockRef) {
                                Expressible curExpressible = innerBlockRef.getReference();
                                
                                if (curExpressible instanceof Dimension) {
                                    String id = curExpressible.getName();
                                    if (targetDimIds.contains(id)) {
                                        numTargetMatch++;
                                    }
                                    uniqueIds.add(id);
                                }
                            }
                        }

                        if (numTargetMatch > 1) {
                            // discard this new dimension, the new dimension is created with multiple target dims
                        } else {
                            sourceDimIds.addAll(uniqueIds);
                        }
                    }

                    for (String curTargetDimId : targetDimIds) {
                        if (!sourceDimIds.contains(curTargetDimId)) {
                            //System.out.println("Looped targetDim that doesn't match: " + curTargetDim.getName() + " " + curTargetDim.getId());
                            return true;
                        }
                    }
                }
            } else {
                System.out.println("needReaggregate() QueryBlockJoins is null");
            }
        }

        return false;
    }

    private String patchFormulaWithAlias(String formula, String alias) {
        String retStr = null;

        CommonTree curItemTree = VeroSqlParser.parseExpression(formula);
        List<String> columns = SqlEngineUtils.extractColumns(formula);
        TreePatcher.patchTreeByString(curItemTree, columns, alias);
        retStr = VeroSqlParser.createExpression(curItemTree).toString();

        return retStr;
    }
    
    /*
    private String retrieveRepresentNameWithAlias(
        String expressibleName,
        ColumnAlias columnAlias,
        Map<String, String> representNameMap) {
        // remove all spaces and make it less than maxChar
        // retrieving represnetName
        String madeUpColumnAliasName = "";
        int maxChar = 12;
        madeUpColumnAliasName = expressibleName.replaceAll(" ", "");
        madeUpColumnAliasName = madeUpColumnAliasName.substring(0, Math.min(maxChar-1, madeUpColumnAliasName.length())).toLowerCase();
        String madeUpColumnAliasNameWithSerial = columnAlias.assignAlias(madeUpColumnAliasName);

        if (!representNameMap.containsKey(expressibleName)) {
            representNameMap.put(expressibleName, madeUpColumnAliasNameWithSerial);
        }

        return "\"" + madeUpColumnAliasNameWithSerial + "\"";
    }
    */
    
    private String retrieveRepresentNameWithAlias(
        String expressibleName,
        String expressibleSlugName,
        ColumnAlias columnAlias,
        Map<String, String> representNameMap) {
        VirtualDB virtualDB = sqlEngineContext.getVirtualDB();

        int maxChar = virtualDB.getMaxColumnNameLength();
        
        expressibleSlugName = expressibleSlugName.toLowerCase();
        if (expressibleSlugName.length() >= maxChar) {
            expressibleSlugName = expressibleSlugName.substring(0, maxChar);
        }
        String madeUpColumnAliasNameWithSerial = columnAlias.assignAlias(expressibleSlugName);

        if (!representNameMap.containsKey(expressibleName)) {
            representNameMap.put(expressibleName, madeUpColumnAliasNameWithSerial);
        }

        return "\"" + madeUpColumnAliasNameWithSerial + "\"";
    }
    
    private Boolean isExpressibleInSelectList(
		IQueryPlan queryPlan,
        Expressible expressible) {
        DataBlock dataBlock = queryPlan.getPlanBlock();

        List<Measure> selectMeasures = dataBlock.getSelectMeasures();
        List<Dimension> selectDimensions = dataBlock.getSelectDimensions();

        if (expressible instanceof Measure) {
            return selectMeasures.contains(expressible);
        } else {
            // dimension
            return selectDimensions.contains(expressible);
        }
    }
    
    private Boolean isExpressibleReachableByPlan(
        IQueryPlan queryPlan,
        Expressible expressible) throws Exception {
        if (expressible instanceof Measure) {
            Set<Measure> planMeasures = queryPlan.getPlanMeasures();
            return planMeasures.contains(expressible);
        } else {
            // dimension
            // 20150501: the plan might not have the dim in a cross join scenario.
            // so I need to ask joinable, if it throws exception, just remove the item
            // if it gives null, remove the item
            // if it gives back something, keep it

            Set<Dimension> planDimensions = queryPlan.getPlanDimensions();

            if (planDimensions.contains(expressible)) {
                return true;
            } else {
                try {
                    IJoinable joinable = queryPlan.getJoinable(expressible);

                    if (joinable == null) {
                        return false;
                    } else {
                        return true;
                    }
                } catch (PlannerException e) {
                    return false;
                }
            }
        }
    }

    private void genSqlFinalBlock(
        Queue<VeroItem> cleanupItems,
        FinalBlock finalBlock) {
        String dropTable = null;
        String finalSql;
        DBType dbType = sqlEngineContext.getVirtualDB().getDbType();

        for (VeroItem cleanupItem : cleanupItems) {
        String itemName = cleanupItem.getName();

        if (cleanupItem.getType() == VeroType.VIEW) {
            dropTable = "".concat("DROP VIEW").concat(" ").concat(itemName);
        } else {
            // default to table
            if (dbType == DBType.ORACLE) {
                dropTable = "".concat("TRUNCATE TABLE").concat(" ").concat("\"").concat(itemName).concat("\"");
                finalBlock.getSqls().add(dropTable);
            }
            dropTable = "".concat("DROP TABLE").concat(" ").concat(itemName);
        }

        finalSql = translateSql(dbType, VeroSqlParser.createStatement(dropTable));

        if (cleanupItem.getType() == VeroType.TABLE) {
            if (dbType == DBType.MSSQL || dbType == DBType.AZURE) {
                for (String tempBlock : blockTempName) {
                    finalSql = finalSql.replaceAll(tempBlock, "#".concat(tempBlock));
                }
            }
        }
            finalBlock.getSqls().add(finalSql);
        }
    }

    private List<VeroBase> gatherItemsInFilter(BlockFilter blockFilter) {
        Iterator<FilterNode<Expression>> it = blockFilter.getRoot().iterator();
        List<VeroBase> items = new ArrayList<VeroBase>();

        while (it.hasNext()) {
            FilterNode<Expression> curNode = it.next();
            if (!curNode.isOperator()) {
                Expression curFilterExp = curNode.getData();
                for (ExpressionRef<? extends VeroBase> expRef : curFilterExp.getReferences()) {
                    items.add(expRef.getReference());
                }
            }
        }

        return items;
    }
    
    private String combinePhysicalNameSegments(List<String> segments) {
        if (segments == null) { return ""; }

        return segments.stream().map(segment -> "\"" + segment + "\"").collect(Collectors.joining("."));
    }

    private Boolean ignoreFilterNode(DataBlock dataBlock, int type, int nodeId) {
        Boolean ignore = false;

    	if (dataBlock instanceof QueryBlock) {
    		QueryBlock rb = (QueryBlock) dataBlock;
    		if (rb.getPlan() instanceof MultiBlockPlan) {
    			FilterContext filterContext = sqlEngineContext.getFilterContext(findSourceBlock(dataBlock).getBlockFilter().getRID());
    			if (filterContext != null) {
    				if (type == 0) {
    					if (filterContext.getUsageCountForWhere(nodeId) > 0) ignore = true;
    				} else {
    					if (filterContext.getUsageCountForHaving(nodeId) > 0) ignore = true;
    				}
    			}
    		}
    	}

    	return ignore;
    }

    // 20150610 ToDo: it actually can't totally depends on the serial being 0 (the first block)
    // it can be that at some point the data needs to be pushed to hive so it needs these hive config
    private Boolean needHiveConfig(int serial, DataBlock dataBlock) {
        if (serial == 0) {
            DBType dbType = sqlEngineContext.getVirtualDB().getDbType();
            if (dbType == DBType.HIVE) {
                return true;
            }
        }

        return false;
    }

    private String getJoinableNameWithSchema(Boolean prefixSchema, IJoinable joinable) {
        if (prefixSchema == true) {
            return combinePhysicalNameSegments(joinable.getPhysicalNameSegments());
        } else {
            return joinable.getPhysicalName();
        }
    }

    private String genStringForWindowFunction(
        IQueryPlan queryPlan,
		TableAlias tableAlias,
		String formula,
		Expression expression,
		List<VeroType> joinableTypes,
		Boolean needReaggregate) throws Exception {
        String itemToSql = null;
        CommonTree commonTree = VeroSqlParser.parseExpression(formula);
        ExtractResult extractResult = EnhancedNodeExtractor.extract(commonTree);
        
        // 20150719: here the formula contains a table joinable so we need to extra columns, not only vero tokens.
        //List<String> matchedNames = extractVeroIdents(formula);
        List<String> matchedNames = extractColumns(formula);

        int index = 0;

        // windowing function
        List<String> itemsToRemove = new ArrayList<String>();
        List<String> parameters = extractResult.getParametersOnlyNames();
        List<String> arguments = extractResult.getArgumentsOnlyNames();

        // first phase: scan through
        for (String parameter : parameters) {
            //System.out.println("Parameter: " + parameter);
            parameter = parameter.replaceAll("\"", "");

            index = matchedNames.indexOf(parameter);
            if (index != -1) {
                ExpressionRef<? extends VeroBase> curExpRef = findExpressionReference(expression.getReferences(), index);
                VeroObj veroObj = curExpRef.getReference();

                if (veroObj instanceof Column) {
        			// suppose column is local so don't check if it is reachable
        			continue;
                }

                Expressible curExpressible = (Expressible) veroObj;
                if (curExpressible != null) {
        			if (curExpressible instanceof Dimension) {
        			    // the item needs to be in select list to be considered valid
                        if (!isExpressibleInSelectList(queryPlan, curExpressible)) {
                            itemsToRemove.add(parameter);
                        }
        			} else {
        				// measure
        				if (!isExpressibleReachableByPlan(queryPlan, curExpressible)) {
        					itemsToRemove.add(parameter);
        				}
        			}
                } else {
                    // 20141124 ToDo: throw exception, input or parsing error
                    System.out.println("SQL error...");
                    return formula;
                }
            } else {
                // 20141124 ToDo: throw exception, input or parsing error
                System.out.println("SQL error...");
                return formula;
            }
        }

        //int numArgRemoved = 0;
        for (String argument : arguments) {
		//System.out.println("Argument: " + argument);
            argument = argument.replaceAll("\"", "");

            index = matchedNames.indexOf(argument);
            if (index != -1) {
                ExpressionRef<? extends VeroBase> curExpRef = findExpressionReference(expression.getReferences(), index);
                VeroObj veroObj = curExpRef.getReference();

                if (veroObj instanceof Column) {
        			// suppose column is local so don't check if it is reachable
        			continue;
                }

                Expressible curExpressible = (Expressible) veroObj;
                if (curExpressible != null) {
                    if (curExpressible instanceof Dimension) {
                        // the item needs to be in select list to be considered valid
                        if (!isExpressibleInSelectList(queryPlan, curExpressible)) {
                            itemsToRemove.add(argument);
                            //numArgRemoved++;
                        }
                    } else {
                        // measure
                        if (!isExpressibleReachableByPlan(queryPlan, curExpressible)) {
                            itemsToRemove.add(argument);
                            //numArgRemoved++;
                        }
                    }
                } else {
                    // 20141124 ToDo: throw exception, input or parsing error
                    System.out.println("SQL error...");
                    return formula;
                }
            } else {
                // 20141124 ToDo: throw exception, input or parsing error
                System.out.println("SQL error...");
                return formula;
            }
        }

        // second phase: patch formula
        TreePatcher.patchWindwingFunction(commonTree, itemsToRemove, itemsToRemove);
        formula = translateSql(null, VeroSqlParser.createExpression(commonTree));

        // third phase: apply re-aggregate if needed
        // gather type info with matched names
        List<ExpressionRef<? extends VeroBase>> references = expression.getReferences();
        List<VeroItem> matchedVeroItems = generateVeroItems(references, null, matchedNames);

        if (needReaggregate == true) {
            CommonTree patchedTree = TreePatcher.appendAggregateFunc(commonTree, matchedVeroItems, sqlEngineContext.getCurrentDataBlock().getName(), PatchByAppendingFuncReturnType.NONE);
            formula = translateSql(null, VeroSqlParser.createExpression(patchedTree));
        }

        for (String matchedName : matchedNames) {
            // ignore removed item from original ExpressionProp list
            if (itemsToRemove.contains(matchedName)) {
                continue;
            }

            index = matchedNames.indexOf(matchedName);
            
            ExpressionRef<? extends VeroBase> curExpRef = findExpressionReference(expression.getReferences(), index);
            VeroObj veroObj = curExpRef.getReference();

            if (veroObj instanceof Column) {
                Column curColumn = (Column) veroObj;
                if (curColumn != null) {
                    CommonTree curItemTree = VeroSqlParser.parseExpression(formula);
                    Table curTable = curColumn.getTable();
                    // 20160215: tableAlias fix
                    //String tableAliasName = tableAlias.assignAlias(curTable.getPhysicalName());
                    String tableAliasName = tableAlias.assignAlias(curTable);
                    TreePatcher.patchTreeByString(curItemTree, Arrays.asList(matchedName), tableAliasName);
                    formula = VeroSqlParser.createExpression(curItemTree).toString();

                    // 20150430 ToDo: need a way to make items not in the select list into groupby
                    //if (groupBys != null) { groupBys.add(patchedName); }
                }
            } else {
                Expressible curExpressible = (Expressible) veroObj;

                if (curExpressible != null) {
                    if (curExpressible.isConstant()) {
                        itemToSql = genStringForConstant(curExpressible);
                    } else {
                        IJoinable epJoinable = queryPlan.getJoinable(curExpressible);
                        Expression epExpression = queryPlan.getExpression(curExpressible);

                        if (epJoinable instanceof DataBlock) {
                            Map<String, String> representNameMap = blockAliasMap.get(epJoinable.getPhysicalName());
                            String veroIdent = FormulaUtils.parseBlockDerivedName(matchedName)[1];
                            String representName = representNameMap.get(veroIdent);
                            // 20160215: tableAlias fix
                            //String tableAliasName = tableAlias.assignAlias(epJoinable.getPhysicalName());
                            String tableAliasName = tableAlias.assignAlias(epJoinable);
                            itemToSql = "\"" + tableAliasName.concat("\"").concat(".").concat("\"").concat(representName).concat("\"");
                            if (joinableTypes != null) { joinableTypes.add(VeroType.ENGINE_BLOCK); }
                        } else if (epJoinable instanceof VirtualJoinable) {
                            itemToSql = genStringForVirtualJoinable(queryPlan, tableAlias, epExpression.getFormula(), joinableTypes,
                                (VirtualJoinable) epJoinable, false);
                        } else if (epJoinable instanceof CoalesceJoinable) {
                            itemToSql = genStringForCoalesceJoinable(
                                curExpressible,
                                tableAlias,
                                (CoalesceJoinable) epJoinable);
                        } else {
                            // Table
                            // 20160215: tableAlias fix
                            //String tableAliasName = tableAlias.assignAlias(epJoinable.getPhysicalName());
                            String tableAliasName = tableAlias.assignAlias(epJoinable);
                            itemToSql = patchFormulaWithAlias(epExpression.getFormula(), tableAliasName);
                            if (joinableTypes != null) { joinableTypes.add(VeroType.TABLE); }
                        }
                    }

                    //formula = formula.replaceFirst(Pattern.quote(matchedName), itemToSql);
                    formula = replaceFirstAvoidOutsideQuotes(formula, Pattern.quote(matchedName), itemToSql);

                }
            }
        }

        return formula;
    }
}
