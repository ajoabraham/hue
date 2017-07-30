package com.hue.translator.sql;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.vero.common.constant.DBType;
import com.vero.common.constant.JoinType;
import com.vero.common.constant.VeroType;
import com.vero.common.sql.VeroIdent;
import com.vero.common.sql.parser.AggregateFunctions;
import com.vero.common.sql.parser.VeroSqlParser;
import com.vero.common.sql.parser.nodeextractor.EnhancedNodeExtractor;
import com.vero.common.sql.parser.nodeextractor.ExtractResult;
import com.vero.model.Dimension;
import com.vero.model.Expressible;
import com.vero.model.ExpressionRef;
import com.vero.model.Measure;
import com.vero.model.VeroBase;
import com.vero.model.graph.GraphException;
import com.vero.model.report.BaseBlock;
import com.vero.model.report.BlockDerivedDimension;
import com.vero.model.report.BlockDerivedEntity;
import com.vero.model.report.BlockDerivedMeasure;
import com.vero.model.report.BlockToBlockJoin;
import com.vero.model.report.DataBlock;
import com.vero.model.report.EngineBlock;
import com.vero.model.report.FinalBlock;
import com.vero.model.report.IJoinExpression;
import com.vero.model.report.IJoinTree;
import com.vero.model.report.IQueryPlan;
import com.vero.model.report.QueryBlock;
import com.vero.model.report.Report;
import com.vero.model.report.ResultBlock;
import com.vero.model.report.SetBlock;
import com.vero.server.engine.OptimizedPlan;
import com.vero.server.engine.graph.QEException;
import com.vero.server.engine.sql.SqlEngine.VeroItem;
import com.vero.server.engine.sql.SqlEngineContext.BlockContext;
import com.vero.server.engine.sql.SqlEngineContext.SelectItem;
import com.vero.server.engine.sql.formatter.FormatterFactory;
import com.vero.server.engine.sql.vdb.VirtualDB;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SqlEngineUtils {
    public static String joinTypeToString(JoinType type) {
        String name = "";

        switch (type) {
        case INNER_JOIN:
            name = "INNER JOIN";
            break;
        case LEFT_OUTER_JOIN:
            name = "LEFT JOIN";
            break;
        case RIGHT_OUTER_JOIN:
            name = "RIGHT JOIN";
            break;
        case FULL_OUTER_JOIN:
            name = "FULL JOIN";
            break;
        case CROSS_JOIN:
            name = "CROSS JOIN";
            break;
        }

        return name;
    }
    
    public static String patchJoinDef(
        String original,
        String leftTable,
        String leftAlias,
        String rightTable,
        String rightAlias) {
        String leftTarget = null;
        String leftReplacement = null;
        String rightTarget = null;
        String rightReplacement = null;
        
        if (original.contains("tleft" + ".") && original.contains("tright" + ".")) {
            // assume the formula contains tleft. and tright.
            leftTarget = "\\b" + "tleft" + "\\.";
            leftReplacement = leftAlias + ".";
            rightTarget = "\\b" + "tright" + "\\.";
            rightReplacement = rightAlias + ".";
            
            return original.replaceAll(leftTarget, leftReplacement).replaceAll(rightTarget, rightReplacement);
        } else {
            leftTarget = "\\b" + leftTable + "\\b";
            leftReplacement = leftAlias;
            rightTarget = "\\b" + rightTable + "\\b";
            rightReplacement = rightAlias;
            
            String processed = SqlEngineUtils.replaceFirstAvoidOutsideQuotes(
        			original, leftTarget, leftReplacement);
            processed = SqlEngineUtils.replaceFirstAvoidOutsideQuotes(
            		processed, rightTarget, rightReplacement);
            
            return processed;
        }
    }
    
    public static List<String> extractColumns(String src) {
        Expression expression = VeroSqlParser.createExpression(src);
        return extractColumns(expression);
    }

    public static List<String> extractColumns(Expression expression) {
        ExtractResult extractResult = EnhancedNodeExtractor.extract(expression);
        return extractResult.getColumnsOnlyNames();
    }

    public static List<String> extractQualifiedNames(String src) {
        Expression expression = VeroSqlParser.createExpression(src);
        return extractQualifiedNames(expression);
    }
    
    public static List<String> extractQualifiedNames(Expression expression) {
        ExtractResult extractResult = EnhancedNodeExtractor.extract(expression);
        return extractResult.getQualifiedNamesOnlyNames();
    }
        
    public static List<String> extractVeroIdents(String src) {
        List<String> tokens = extractColumns(src);
        Pattern veroPattern = Pattern.compile(VeroIdent.VeroIdentPattern);

        tokens = tokens.stream().filter(token -> veroPattern.matcher(token).matches()).collect(Collectors.toList());

        return tokens;
    }

    public static String getQuotedString(String source) {
        if (source == null) {
            return null;
        }
        
        return "\"" + source + "\""; 
    }
    
    public static String replaceFirstAvoidOutsideQuotes(String original, String regex, String replacement) {
        Matcher matcher = Pattern.compile(regex).matcher(original);

        if (matcher.find() == true) {
            // quote around the replacement
            int start = matcher.start();
            int end = matcher.end();
            int length = original.length();

            if ((start != 0) && (end != length)) {
                // not match to the beginning and the end which imply there is no quote there
                //System.out.println("start==> " + start + " end==> " + end + " length==> " + length);
                //System.out.println("original substring1==>" + original.substring(0, start-1));
                //System.out.println("original substring2==>" + original.substring(end+1));

                if (original.subSequence(start-1, start).equals("\"") && original.subSequence(end, end+1).equals("\"")) {
                    // outside quotes
                    return original.substring(0, start-1).concat(replacement).concat(original.substring(end+1));
                }
            }

            return matcher.replaceFirst(replacement);
        } else {
            return original;
        }
    }

    public static String convertSqlForTest(String sql) {
        return sql.replaceAll("\"", "").replaceAll("\n", "");
    }

    protected static void printVeroItems(
		List<VeroItem> veroItems,
		String msg) {
    	System.out.println(msg + ", List<VeroItem>==> " + veroItems);
    	veroItems.forEach(item -> System.out.println(item));
    }

    public static Boolean emulateWindowFunction(
		DBType dbType,
		BlockContext blockContext,
		String blockName,
		String sql,
		String select,
		String from,
		String where,
		String groupby,
		String having,
		String orderby,
		List<String> allJoins,
		List<String> allSqls,
		List<VeroItem> cleanupItems) {
    	class SortItem {
    		String _key;
    		String _alias;
    		String _order;
    
    		public SortItem(String key, String order) { _key = key; _order = order; }
    		public void setAlias(String alias) { _alias = alias; }
    		public String getKey() { return _key; }
    		public String getAlias() { return _alias; }
    		public String getOrder() { return _order; }
    	};
    
    	class PartitionByItem {
    		String _key;
    		String _alias;
    
    		public PartitionByItem(String key, String alias) { _key = key; _alias = alias; }
    		public String getKey() { return _key; }
    		public String getAlias() { return _alias; }
    	};
    
    	class WindowFunctionProcessContext {
    		private int _viewId;
    		private List<PartitionByItem> _partitionByItems = new ArrayList<PartitionByItem>();
    		private List<SortItem> _sortItems = new ArrayList<SortItem>();
    
    		public void setViewId(int viewId) { _viewId = viewId; }
    		public int getViewId() { return _viewId; }
    		public void addPartitionByItem(PartitionByItem item) { _partitionByItems.add(item); }
    		public List<PartitionByItem> getPartitionByItems() { return _partitionByItems; }
    		public void addSortItem(SortItem item) { _sortItems.add(item); }
    		public List<SortItem> getSortItems() { return _sortItems; }
    	}
    
    	String WF_ALIAS_PREFIX = "WFITEM";
    	String WF_VIEW_PREFIX = "WFVIEW";
    
    	List<SelectItem> selectItems = blockContext.getSelectItems();
    	List<WindowFunctionProcessContext> wfContexts = new ArrayList<WindowFunctionProcessContext>();
    
    	// prepare WindowFunctionProcessContext
    	int wfItemCnt = 0;
    	int wfCnt = 1;
    	for (int i=0; i<selectItems.size(); i++) {
    		SelectItem selectItem = selectItems.get(i);
    		if (selectItem.isWindowFunction()) {
    			WindowFunctionProcessContext wfpContext = new WindowFunctionProcessContext();
    			wfContexts.add(wfpContext);
    			wfpContext.setViewId(wfCnt);
    			selectItem.setUser(wfpContext);
    			ExtractResult extractResult = EnhancedNodeExtractor.extract(selectItem.getKey());
    
    	        // partition by
    	        List<String> names = extractResult.getPartitionByKeysOnlyNames();
    	        for (int j=0; j<names.size(); j++, wfItemCnt++) {
        			String key = names.get(j);
        			String alias = "".concat(WF_ALIAS_PREFIX).concat(Integer.toString(wfItemCnt));
        
        			PartitionByItem pbItem = new PartitionByItem(key, alias);
        			wfpContext.addPartitionByItem(pbItem);
    	        }
    
    	        // order by
    	        names = extractResult.getOrderByOnlyNames();
    	        for (int j=0; j<names.size(); j++, wfItemCnt++) {
        			String value = names.get(j);
        
        			Pattern sortItem = Pattern.compile("(\\w+)=([^,}]+)");
        			Matcher sortItemMatcher = sortItem.matcher(value);
        			String sortKey = null;
        			String order = null;
                    while (sortItemMatcher.find()) {
            			String type = sortItemMatcher.group(1);
            			String innerValue = sortItemMatcher.group(2);
                        if (type.equalsIgnoreCase("sortKey")) {
            				sortKey = innerValue;
                        } else if (type.equalsIgnoreCase("ordering")) {
            				order = innerValue;
                        }
                    }
    
                    SortItem newSortItem = new SortItem(sortKey, order);
                    System.out.println("Adding SortItem: " + newSortItem + " key: " + sortKey);
                    wfpContext.addSortItem(newSortItem);
        			String alias = "".concat(WF_ALIAS_PREFIX).concat(Integer.toString(wfItemCnt));
        			newSortItem.setAlias(alias);
    	        }
    
    	        wfCnt++;
    		}
    	}

    	// generate create view
    	String viewSql = "SELECT ";
    	Joiner joiner = Joiner.on(", ").skipNulls();
    	List<String> genSelectItems = new ArrayList<String>();
        for (int i=0; i<selectItems.size(); i++) {
    		SelectItem item = selectItems.get(i);
    		if (!item.isWindowFunction()) {
    			genSelectItems.add(item.getKey().concat(" ").concat(item.getAlias()));
    		} else {
    			// add partition by and order by
    			WindowFunctionProcessContext wfpContext = (WindowFunctionProcessContext) item.getUser();
    			for (int j=0; j<wfpContext.getPartitionByItems().size(); j++) {
    				PartitionByItem pbItem = wfpContext.getPartitionByItems().get(j);
    				genSelectItems.add(pbItem.getKey().concat(" ").concat(pbItem.getAlias()));
    			}
    
    			for (int j=0; j<wfpContext.getSortItems().size(); j++) {
    				SortItem obItem = wfpContext.getSortItems().get(j);
    				System.out.println("obItem==> " + obItem + " " + obItem.getKey() + " " + obItem.getAlias());
    				genSelectItems.add(obItem.getKey().concat(" ").concat(obItem.getAlias()));
    			}
    		}
        }
        viewSql = viewSql.concat(joiner.join(genSelectItems));

        // 20150219 ToDo: need to try to append new groupby items

        String viewName = blockName.concat("WFV");
        viewSql = viewSql.concat(" ").concat(from).concat(" ").concat(where).concat(" ").concat(groupby).concat(" ").concat(having).concat(orderby);
        viewSql = "".concat("create view ").concat(viewName).concat(" AS ").concat(viewSql);

        allSqls.add(viewSql);
        cleanupItems.add(new VeroItem(VeroType.VIEW, null, viewName));

        // debug
        System.out.println("viewSql==> " + viewSql);
        Statement stm = VeroSqlParser.VERO_SQL_PARSER.createStatement(viewSql);
        System.out.println("Put them together==> " + FormatterFactory.getSqlFormatter().formatSql(stm));

        // generate reference to view
        String referenceSql = "SELECT ";
        joiner = Joiner.on(", ").skipNulls();
        genSelectItems.clear();
        
        for (int i=0; i<selectItems.size(); i++) {
    		SelectItem item = selectItems.get(i);
    		String temp = null;
    
    		if (!item.isWindowFunction()) {
    			temp = WF_VIEW_PREFIX.concat(Integer.toString(0)).concat(".").concat(item.getAlias()).concat(" ").concat(item.getAlias());
    			genSelectItems.add(temp);
    		} else {
    			// reference to window function emulation
    			WindowFunctionProcessContext wfpContext = (WindowFunctionProcessContext) item.getUser();
    
    			String wfEmulation = "( select 1+count(*) from ".concat(blockName).concat("WFV").concat(" ").concat(WF_VIEW_PREFIX).concat(Integer.toString(wfpContext.getViewId())).concat(" where ");
    			Joiner innerJoiner = Joiner.on(" AND ").skipNulls();
    			List<String> innerWFItems = new ArrayList<String>();
    
                for (int j=0; j<wfpContext.getPartitionByItems().size(); j++) {
        			PartitionByItem pbItem = wfpContext.getPartitionByItems().get(j);
        			temp = "".concat(WF_VIEW_PREFIX).concat(Integer.toString(0)).concat(".").concat(pbItem.getAlias()).concat("=").concat(WF_VIEW_PREFIX).concat(Integer.toString(wfpContext.getViewId())).concat(".").concat(pbItem.getAlias());
        			innerWFItems.add(temp);
                }
    
                for (int j=0; j<wfpContext.getSortItems().size(); j++) {
        			SortItem sbItem = wfpContext.getSortItems().get(j);
        			String order = sbItem.getOrder();
        			String operator = "";
        			if (order.equalsIgnoreCase("ASCENDING") || order.equalsIgnoreCase("ASC")) {
        				operator = ">";
        			} else {
        				operator = "<";
        			}
        			temp = "".concat(WF_VIEW_PREFIX).concat(Integer.toString(0)).concat(".").concat(sbItem.getAlias()).concat(operator).concat(WF_VIEW_PREFIX).concat(Integer.toString(wfpContext.getViewId())).concat(".").concat(sbItem.getAlias());
        			innerWFItems.add(temp);
                }

                temp = wfEmulation.concat(innerJoiner.join(innerWFItems)).concat(") AS ").concat(item.getAlias());
                genSelectItems.add(temp);
    		}
        }

        referenceSql = referenceSql.concat(joiner.join(genSelectItems));
        referenceSql = referenceSql.concat(" from ").concat(blockName).concat("WFV").concat(" ").concat(WF_VIEW_PREFIX).concat(Integer.toString(0));
        // 20150219 ToDo: need to append old groupby, orderby items

        allSqls.add(referenceSql);

        // debug
        System.out.println("referenceSql==> " + referenceSql);
        stm = VeroSqlParser.VERO_SQL_PARSER.createStatement(referenceSql);
        System.out.println("Put them together==> " + FormatterFactory.getSqlFormatter().formatSql(stm));

        return true;
    }

    public static Boolean emulateFullJoin(
        VirtualDB virtualDB,
		String blockName,
		String sql,
		String select,
		String from,
		String where,
		String groupby,
		String having,
		String orderby,
		List<String> allJoins,
		List<String> allSqls,
		List<VeroItem> cleanupItems) {
    	class JoinStatement {
    		private Boolean _modifiable = false;
    		private String _statement;
    
    		public JoinStatement(Boolean modifiable, String statement) { _modifiable = modifiable; _statement = statement; }
    		public Boolean getModifiable() { return _modifiable; }
    		public void setModifiable(Boolean modifiable) { _modifiable = modifiable; }
    		public String getStatement() { return _statement; };
    		public void setStatement(String statement) { _statement = statement; }
    	}

    	DBType dbType = virtualDB.getDbType();
    	Boolean found = false;
    	for (String join : allJoins) {
    		if (join.contains("FULL JOIN")) {
    			found = true;
    			break;
    		}
    	}
    	if (!found) return false;

    	System.out.println("Emulating full join...");

        List<JoinStatement> allJoinStatements = new ArrayList<JoinStatement>();
        int fullJoinCount = 0;
        for (String join : allJoins) {
            Boolean modifiable = false;
            if (join.contains("FULL JOIN")) {
        		modifiable = true;
        		join = join.replace("FULL JOIN", "LEFT JOIN");
        		fullJoinCount++;
            }

            JoinStatement newJoin = new JoinStatement(modifiable, join);
            allJoinStatements.add(newJoin);
        }

        String tempSql = sql.replaceAll("FULL JOIN", "LEFT JOIN");
        String eachSql;
        String fullJoinTempTableName = blockName.concat("FJ0");

        cleanupItems.add(new VeroItem(VeroType.TABLE, null, fullJoinTempTableName));
        
        if (virtualDB.supportsCreateTemporaryTable() == false) {
            eachSql = "".concat("CREATE TABLE " + cleanupItems.get(0).getName()).concat(" AS (").concat(tempSql).concat(")");
        } else {
            eachSql = "".concat("CREATE TEMP TABLE " + cleanupItems.get(0).getName()).concat(" AS (").concat(tempSql).concat(")");
        }
        allSqls.add(eachSql);

        if (dbType == DBType.DERBY_LOCAL) {
            allSqls.add("insert into " + cleanupItems.get(0).getName() + " " + tempSql);
        }

        for (int i=1; i<=fullJoinCount; i++) {
    		fullJoinTempTableName = blockName.concat("FJ").concat(Integer.toString(i));
    		cleanupItems.add(new VeroItem(VeroType.TABLE, null, fullJoinTempTableName));
    		tempSql = "".concat(select).concat(" FROM ");
    
    		for (JoinStatement join : allJoinStatements) {
    			if (join.getModifiable() == true) {
    				join.setStatement(join.getStatement().replace("LEFT JOIN", "RIGHT JOIN"));
    				join.setModifiable(false);
    				break;
    			}
    		}
    
    		for (JoinStatement join : allJoinStatements) {
    			tempSql = tempSql.concat(join.getStatement());
    		}

    		tempSql = tempSql.concat("\n").concat(where).concat("\n").concat(groupby).concat("\n").concat(having).concat(orderby).concat("\n");
    		
    		if (virtualDB.supportsCreateTemporaryTable() == false) {
    	        eachSql = "".concat("CREATE TABLE " + cleanupItems.get(i).getName()).concat(" AS (").concat(tempSql).concat(")");
    		} else {
                eachSql = "".concat("CREATE TEMP TABLE " + cleanupItems.get(i).getName()).concat(" AS (").concat(tempSql).concat(")");    		    
    		}

    		allSqls.add(eachSql);

    		if (dbType == DBType.DERBY_LOCAL) {
    		    allSqls.add("insert into " + cleanupItems.get(i).getName() + " " + tempSql);
            }
        }

        String unionSql = "";
        for (int i=0; i<cleanupItems.size(); i++) {
    		if (i != 0) {
    			unionSql = unionSql.concat(" union ");
    		}
    
    		unionSql = unionSql.concat("select * from " + cleanupItems.get(i).getName());
        }
        allSqls.add(unionSql);

        return true;
    }

    public static String patchWindowFunctionSumSum(String formula) {
        String retStr = formula;

        ExtractResult extractResult = EnhancedNodeExtractor.extract(formula);
    	if (extractResult.isWindowFunction() == false) { return retStr; }
    	else {
    		Boolean patchWindowFunction = false;
    		String funcName1 = extractResult.getWindowFunction1();
    		String funcName2 = extractResult.getWindowFunction2();
    
    		if (AggregateFunctions.isAggregateFunction(funcName1) == true) {
                    if (funcName2 != null) {
    			if (AggregateFunctions.isAggregateFunction(funcName2) == false) {
    				patchWindowFunction = true;
    			} else {
    				// nested agg
    				patchWindowFunction = false;
    
    				// 20150427 ToDo: if funcName2 == max/sum and the max/sum is prefixed by us, then we make it to be same as funcName1
    				// otherwise (the func is not prefixed by us), we leave it as is
    
    				/*
    				if (!funcName1.equals(funcName2)) {
    					// needs replace funcName2 with funcName1
    					// in-place replacement
    					String replace = funcName1.concat("(").concat(funcName2);
    					String replacement = funcName1.concat("(").concat(funcName1);
    					retStr = retStr.replaceAll(Pattern.quote(replace), replacement);
    					return retStr;
    				} else {
    					patchWindowFunction = false;
    				}
    				*/
    			}
                    } else {
    			// missing 2nd function
    			patchWindowFunction = true;
                    }
    		} else {
    			// function 1 is not an aggregate function
    			patchWindowFunction = false;
    		}

            if (patchWindowFunction == false) { return retStr; }
            else {
                Pattern windowFuncPattern = Pattern.compile(VeroIdent.VeroWindowFunctionPattern);
                Matcher windowFuncMatcher = windowFuncPattern.matcher(formula);
                while (windowFuncMatcher.find()) {
                    String replace = windowFuncMatcher.group(1);
                    String replacement = funcName1.concat("(").concat(replace).concat(")");
                    retStr = retStr.replaceAll(Pattern.quote(replace), replacement);
                    return retStr;
                }
            }
    	}
    	return retStr;
    }
    
	protected static Boolean isFullJoinPresent(DataBlock dataBlock) throws PlannerException, GraphException {
		if (isDataBlockEmpty(dataBlock)) {
			return false;
		}

		IQueryPlan queryPlan = dataBlock.getPlan();

        if (queryPlan == null) {
            return false;
        }

        if (queryPlan instanceof OptimizedPlan) {
            if (((OptimizedPlan) queryPlan).isConstantOnlyPlan() == true) {
        		// 20150409: the constant plan can still have b2b join that may contain full join
        		// check b2b join
                //if (dataBlock instanceof ResultBlock) {
                if (dataBlock instanceof QueryBlock) {
                    for (BlockToBlockJoin curB2BJoin : ((QueryBlock) dataBlock).getQueryBlockJoins()) {
                        JoinType joinType = curB2BJoin.getJoinType();
        				if (joinType == JoinType.FULL_OUTER_JOIN) {
        					return true;
        				}
                    }
                }

                return false;
            }
        }

        IJoinTree joinTree = queryPlan.getJoinTree();
        if (joinTree == null) {
            return false;
        }

        List<IJoinExpression> joinDefs = new ArrayList<IJoinExpression>(joinTree.getJoinDefs());

    	for (IJoinExpression jDef : joinDefs) {
    		if (jDef.getJoinType() == JoinType.FULL_OUTER_JOIN) {
    			return true;
    		}
    	}
    
        //if (dataBlock instanceof ResultBlock) {
    	if (dataBlock instanceof QueryBlock) {
            for (BlockToBlockJoin curB2BJoin : ((QueryBlock) dataBlock).getQueryBlockJoins()) {
                JoinType joinType = curB2BJoin.getJoinType();
    			if (joinType == JoinType.FULL_OUTER_JOIN) {
    				return true;
    			}
            }
        }
    
    	return false;
	}

	protected static Boolean isDataBlockEmpty(DataBlock dataBlock) {
    	if (dataBlock instanceof QueryBlock) {
    		QueryBlock qb = (QueryBlock) dataBlock;
    		return qb.isEmpty();
    	} else {
    		return false;
    	}
    }

	protected static DataBlock findDataBlock(Report report, String physicalName) {
        for (QueryBlock queryBlock : report.getQueryBlocks()) {
            for (SetBlock setBlock : queryBlock.getSetBlocks()) {
                if (setBlock.getPhysicalName().equals(physicalName)) {
                    return setBlock;
                }
            }

            for (EngineBlock engineBlock : queryBlock.getEngineBlocks()) {
                if (engineBlock.getPhysicalName().equals(physicalName)) {
                    return engineBlock;
                }
            }

            if (queryBlock.getPhysicalName().equals(physicalName)) {
                return queryBlock;
            }
        }

        ResultBlock resultBlock = report.getResultBlock();
        for (SetBlock setBlock : resultBlock.getSetBlocks()) {
            if (setBlock.getPhysicalName().equals(physicalName)) {
                return setBlock;
            }
        }

        for (EngineBlock engineBlock : resultBlock.getEngineBlocks()) {
            if (engineBlock.getPhysicalName().equals(physicalName)) {
                return engineBlock;
            }
        }
        
        if (resultBlock.getPhysicalName().equals(physicalName)) {
            return resultBlock;
        }
        
        return null;
	}
	
    protected static QueryBlock findSourceBlock(BaseBlock baseBlock) {
        QueryBlock source = null;

        if (baseBlock != null) {
            if (baseBlock instanceof FinalBlock) {
                source = ((FinalBlock) baseBlock).getSourceBlock();
            } else if (baseBlock instanceof EngineBlock) {
                source = ((EngineBlock) baseBlock).getSourceBlock();
            } else if (baseBlock instanceof QueryBlock) {
                source = (QueryBlock) baseBlock;
            }
        }

        return source;
    }

    protected static ExpressionRef<? extends VeroBase> findExpressionReference(
        List<ExpressionRef<? extends VeroBase>> references, int position) {
        if (references == null) return null;

        for (ExpressionRef<? extends VeroBase> reference : references) {
            if (reference.getPosition() == position) {
                return reference;
            }
        }

        return null;
    }
    
    protected static BlockDerivedDimension findBlockDerivedDimension(QueryBlock queryBlock, String blockRefId) {
        if (blockRefId == null) {
            return null;
        }
        for (BlockDerivedDimension curEntity : queryBlock.getBlockDerivedDimensions()) {
            String rID = curEntity.getRID();
            
            // getRID() may return null: 
            // A newly created block derived entity does not have a RID until user saved report
            if (rID != null) {
                if (rID.equals(blockRefId)) {
                    return curEntity;
                }
            }
        }
        return null;
    }

    protected static BlockDerivedMeasure findBlockDerivedMeasure(QueryBlock queryBlock, String blockRefId) {
        if (blockRefId == null) {
            return null;
        }
        for (BlockDerivedMeasure curEntity : queryBlock.getBlockDerivedMeasures()) {
            String rID = curEntity.getRID();
            
            // getRID() may return null: 
            // A newly created block derived entity does not have a RID until user saved report 
            if (rID != null) {
                if (rID.equals(blockRefId)) {
                    return curEntity;
                }
            }
        }
        return null;
    }

    protected static BlockDerivedEntity findBlockDerivedEntity(QueryBlock queryBlock, String blockRefId) {
    	BlockDerivedEntity curEntity = findBlockDerivedDimension(queryBlock, blockRefId);
    
    	if (curEntity == null) {
    		return findBlockDerivedMeasure(queryBlock, blockRefId);
    	} else {
    		return curEntity;
    	}
    }
    
    protected static List<Expressible> genSelectionProcessingOrder(List<Expressible> originalOrder) {
        // re-arrange processing order of Expressibles: simple first than derived
        List<Expressible> finalList = Lists.newArrayList();
        List<Expressible> complexList = Lists.newArrayList();
        for (Expressible e : originalOrder) {
            if ((e instanceof Dimension) || (e instanceof Measure)) {
                finalList.add(e);
            } else {
                complexList.add(e);
            }
        }
        finalList.addAll(complexList);
        
        return finalList;
    }
    
    // this function takes a name, remove all quotes and insert into set if name is unique
    protected static void QuotedNameAwareSetInsertion(Set<String> names, String name) {
        Boolean found = false;
        if (name != null) {
            for (String item : names) {
                if (item.replaceAll("\"", "").equals(name.replaceAll("\"", ""))) {
                    found = true;
                    break;
                }
            }
            
            if (found == false) {
                names.add(name);
            }
        }
    }
}
