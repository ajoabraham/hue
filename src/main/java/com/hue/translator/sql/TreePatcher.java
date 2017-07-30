package com.hue.translator.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;

import com.google.common.base.Joiner;
import com.vero.common.constant.VeroType;
import com.vero.common.sql.parser.AggregateFunctions;
import com.vero.common.sql.parser.VeroSqlParser;
import com.vero.common.util.CommonUtils;
import com.vero.model.util.FormulaUtils;
import com.vero.server.engine.sql.SqlEngine.VeroItem;
import com.vero.server.engine.sql.formatter.FormatterFactory;

public class TreePatcher {
    private static final String GENERIC_NODE = "GENERIC";
    private static final String QUERY_SPEC_NODE = "QUERY_SPEC";
    private static final String GROUP_BY_NODE = "GROUP_BY";
    private static final String FUNCTION_CALL = "FUNCTION_CALL";
    private static final String TARGET_AGGREGATE_FUNCTION_FOR_MEASURE = "sum";
    private static final String TARGET_AGGREGATE_FUNCTION_FOR_DIMENSION = "count";
    private static final String THIS_AGGREGATE_FUNCTION = "max";
	public static final String WINDOW = "WINDOW";
	public static final String PARTITION_BY = "PARTITION_BY";
	public static final String ORDER_BY = "ORDER_BY";
	public static final String ORDER_BY_ITEM = "SORT_ITEM";
	public static final String QNAME = "QNAME";
	private static Map<String, Integer> typesMap = null;

    public enum PatchByAppendingFuncReturnType {
    	NONE,
        USE_INPUT_TREE,
        REGEN_TREE_FUNC_MAX,
        REGEN_TREE_FUNC_SUM,
        REGEN_TREE_FUNC_COUNT
    }

    public class FuncInfo {
        public CommonTree funcNode;
        public String funcName;
        public Boolean isAggregateFunc;
        public int numArguments;

        FuncInfo(CommonTree node, String name, Boolean aggregate, int arguments) {
            funcNode = node;
            funcName = name;
            isAggregateFunc = aggregate;
            numArguments = arguments;
        }
    }

    public static CommonTree appendAggregateFunc(
		CommonTree tree,
		List<VeroItem> matchedVeroItems,
		String thisBlockName,
		PatchByAppendingFuncReturnType funcType) {
        List<FuncInfo> funcStack = new ArrayList<FuncInfo>();

        PatchByAppendingFuncReturnType returnType;
        if (funcType == PatchByAppendingFuncReturnType.NONE) {
            returnType = patchByAppendingFunc(tree, matchedVeroItems, thisBlockName, funcStack);
        } else {
            returnType = funcType;
        }

        if (returnType == PatchByAppendingFuncReturnType.REGEN_TREE_FUNC_MAX) {
            CommonTree newTree = createFunctionCall(THIS_AGGREGATE_FUNCTION, tree);
            tree.setParent(newTree);
            return newTree;
        } else if (returnType == PatchByAppendingFuncReturnType.REGEN_TREE_FUNC_SUM) {
            CommonTree newTree = createFunctionCall(TARGET_AGGREGATE_FUNCTION_FOR_MEASURE, tree);
            tree.setParent(newTree);
            return newTree;
        } else if (returnType == PatchByAppendingFuncReturnType.REGEN_TREE_FUNC_COUNT) {
            CommonTree newTree = createFunctionCall(TARGET_AGGREGATE_FUNCTION_FOR_DIMENSION, tree);
            tree.setParent(newTree);
            return newTree;
    	} else {
            return tree;
        }
    }

    private static PatchByAppendingFuncReturnType patchByAppendingFunc(
		CommonTree tree,
		List<VeroItem> matchedVeroItems,
		String thisBlockName,
		List<FuncInfo> funcStack) {
        //System.out.println("Text = " + tree.getText() +  " ChildCount = " + tree.getChildCount() + " Type = " + tree.getType());
        PatchByAppendingFuncReturnType returnType = PatchByAppendingFuncReturnType.USE_INPUT_TREE;
        Boolean isWindowFunction = false;
        for (Tree t : children(tree)) {
    		String name = ((CommonTree)t).getText();
    		if (name.equalsIgnoreCase("WINDOW")) {
    			isWindowFunction = true;
    			break;
    		}
        }

        if (tree.getText().equals(FUNCTION_CALL) && (isWindowFunction == false)) {
            // function
            String funcName = tree.getChild(0).getChild(0).getText();
            Boolean isAggregateFunc = AggregateFunctions.isAggregateFunction(funcName);

            int numArguments = tree.getChildCount()-1;
            FuncInfo newFuncInfo = new TreePatcher().new FuncInfo(tree, funcName, isAggregateFunc, numArguments);
            funcStack.add(newFuncInfo);

            // process children
            for (Tree t : children(tree)) {
                PatchByAppendingFuncReturnType childReturnType = patchByAppendingFunc((CommonTree)t, matchedVeroItems, thisBlockName, funcStack);
                if (childReturnType != PatchByAppendingFuncReturnType.USE_INPUT_TREE) {
                    returnType = childReturnType;
                }
            }

            funcStack.remove(funcStack.size()-1);
        } else {
            String text = tree.getText();

            if (text.equals("QNAME")) {
                String qname = tree.getChild(0).getText();

                if (qname.startsWith("@")) {
        			// determine if it needs re-aggregate
        			Boolean [] checkReaggregate = checkReaggregateByItem(qname, matchedVeroItems);
        
        			if (checkReaggregate[0] == true) {
                        // determine if it is current or target block
                        String patchedAggregationFunction = null;
                        String[] split = FormulaUtils.parseBlockDerivedName(qname);

                        if (split[0].equals(thisBlockName) || split[0].equals("this")) {
                            patchedAggregationFunction = THIS_AGGREGATE_FUNCTION;
                        } else {
                        	if (checkReaggregate[1] == true) {
                        		patchedAggregationFunction = TARGET_AGGREGATE_FUNCTION_FOR_DIMENSION;
                        	} else {
                        		patchedAggregationFunction = TARGET_AGGREGATE_FUNCTION_FOR_MEASURE;
                        	}
                        }

                        // examine func stack now
                        Boolean hasAggregateFunc = false;
                        Iterator<FuncInfo> funcIterator = funcStack.iterator();
                        while (funcIterator.hasNext()) {
                            FuncInfo curFuncInfo = funcIterator.next();

                            if (curFuncInfo.isAggregateFunc) {
                                hasAggregateFunc = true;
                                break;
                            }
                        }

                        if (hasAggregateFunc == false) {
                            // need to append one
                            // let's append max for everything now, testing for correctness
                            // find the closet place to append an aggregate function
                            if (funcStack.size() == 0) {
                                // no func before
                                Tree parent = tree.getParent();
                                if (parent == null) {
                                    //System.out.println("Case 3 need to patch, parent is null...");
                                    if (patchedAggregationFunction == THIS_AGGREGATE_FUNCTION) {
                                        return PatchByAppendingFuncReturnType.REGEN_TREE_FUNC_MAX;
                                    } else {
                                    	if (checkReaggregate[1] == true) {
                                    		return PatchByAppendingFuncReturnType.REGEN_TREE_FUNC_COUNT;
                                    	} else {
                                    		return PatchByAppendingFuncReturnType.REGEN_TREE_FUNC_SUM;	
                                    	}
                                    }
                                } else {
                                    //System.out.println("Case 0 easy append, no func");
                                    int childIndex = tree.getChildIndex();
                                    parent.setChild(childIndex, createFunctionCall(patchedAggregationFunction, qname));
                                }
                            } else {
                                FuncInfo curFuncInfo = null;
                                Boolean patched = false;
                                for (int i = funcStack.size()-1; i >= 0; i--) {
                                    // search backward until the first func that has arguments more than 1
                                    curFuncInfo = funcStack.get(i);
                                    //System.out.println("Curfun = " + curFuncInfo.funcName + " #ofArguments = " + curFuncInfo.numArguments);
                                    if (curFuncInfo.numArguments > 1) {
                                        if (i == funcStack.size()-1) {
                                            // the first one from backwards, easy append
                                            //System.out.println("Case 1 backwards first 1...");
                                            Tree parent = tree.getParent();
                                            int childIndex = tree.getChildIndex();
                                            parent.setChild(childIndex, createFunctionCall(patchedAggregationFunction, qname));
                                            patched = true;
                                            break;
                                        } else {
                                            // in-between
                                            //System.out.println("Case 2 in-between...");
                                            Tree parent = curFuncInfo.funcNode;
                                            Tree previous = funcStack.get(i+1).funcNode;

                                            int childIndex = previous.getChildIndex();
                                            parent.setChild(childIndex, createFunctionCall(patchedAggregationFunction, previous));
                                            patched = true;
                                            break;
                                        }
                                    }
                                }

                                if (patched == false) {
                                    Tree previous = funcStack.get(0).funcNode;
                                    Tree parent = previous.getParent();

                                    if (parent == null) {
                                        //System.out.println("Case 3 need to patch, parent is null...");
                                        if (patchedAggregationFunction == THIS_AGGREGATE_FUNCTION) {
                                            return PatchByAppendingFuncReturnType.REGEN_TREE_FUNC_MAX;
                                        } else {
                                        	if (checkReaggregate[1] == true) {
                                        		return PatchByAppendingFuncReturnType.REGEN_TREE_FUNC_COUNT;
                                        	} else {
                                        		return PatchByAppendingFuncReturnType.REGEN_TREE_FUNC_SUM;
                                        	}
                                        }
                                    } else {
                                        //System.out.println("Case 4 need to patch, parent is not null...");
                                        int childIndex = previous.getChildIndex();
                                        parent.setChild(childIndex,  createFunctionCall(patchedAggregationFunction, previous));
                                    }
                                }
                            }
                        }
        			}
                }
            }

            // process others
            for (Tree t : children(tree)) {
                PatchByAppendingFuncReturnType childReturnType = patchByAppendingFunc((CommonTree)t, matchedVeroItems, thisBlockName, funcStack);
                if (childReturnType != PatchByAppendingFuncReturnType.USE_INPUT_TREE) {
                    returnType = PatchByAppendingFuncReturnType.REGEN_TREE_FUNC_MAX;
                }
            }
        }

        return returnType;
    }

    private static CommonTree createFunctionCall(String funcName, String funcArg) {
        return VeroSqlParser.parseExpression(funcName.concat("(").concat(funcArg).concat(")"));
    }

    private static CommonTree createFunctionCall(String funcName, Tree rest) {
        CommonTree template = VeroSqlParser.parseExpression(funcName.concat("(").concat("abc").concat(")"));
        int childIndex = template.getChild(1).getChildIndex();
        template.setChild(childIndex, rest);

        return template;
    }

    // boolean[0] = does it need re-aggreagate?
    // boolean[1] = is dimension in a block derived measure?
    private static Boolean [] checkReaggregateByItem(String name, List<VeroItem> matchedVeroItems) {
    	Boolean [] retval = new Boolean[2];
    	
		if (matchedVeroItems == null) {
			retval[0] = true;
			retval[1] = false;
			return retval; 
		}
	
		for (VeroItem veroItem : matchedVeroItems) {
			if (veroItem.getName().equals(name)) {
				System.out.println("Check for needReaggregateByItem: " + veroItem.getName() + ", Type: " + veroItem.getType());
	
				if (veroItem.getParentType() != null) {
					VeroType parentType = veroItem.getParentType();
					if (parentType == VeroType.BLOCK_DERIVED_DIMENSION) {
						retval[0] = false;
						retval[1] = false;
						return retval;
					} else if (parentType == VeroType.BLOCK_DERIVED_MEASURE) {
						retval[0] = true;
						
						if (veroItem.getType() == VeroType.DIMENSION) {
							retval[1] = true;
						} else {
							retval[1] = false;
						}
						return retval;
					}
				} else {
					if (veroItem.getType() == VeroType.MEASURE) {
					    if (AggregateFunctions.startsWithAggregateFunction(veroItem.getReplacement())) {
					    	retval[0] = false;
					    	retval[1] = false;
					        return retval;
					    } else {
					    	retval[0] = true;
					    	retval[1] = false;
					        return retval;
					    }
					} else {
						retval[0] = false;
						retval[1] = false;
						return retval;
					}
				}
			}
		}

		retval[0] = false;
		retval[1] = false;
		return retval;
    }

    public static int findType(String type) {
    	if (typesMap == null) {
    		typesMap = new HashMap<String, Integer>();
    		String sql = "select a, b, max(c) from dual group by a having a>10";
    		CommonTree tree = VeroSqlParser.parseStatement(sql);
    		gatherTypes(tree, typesMap);
    	}
    
    	if (typesMap.containsKey(type)) {
    		return typesMap.get(type).intValue();
    	} else {
    		return 0;
    	}
    }

    public static void treeRemoveItem(
        CommonTree srcTree,
        String type,
        List<String> items) {
        if ((items == null) || (items.size() == 0)) {
            return;
        }

        //System.out.println("srcTree childCount = " + srcTree.getChildCount());

        if (srcTree.getChildCount() == 0) {
            return;
        }

        // process node
        //System.out.println("srcTree getText = " + srcTree.getText());
        if (type.equalsIgnoreCase(QNAME)) {
	        if (srcTree.getText().equals(QNAME)) {
	            Tree childTree = srcTree.getChild(0);
	            //System.out.println("QNAME's first child = " + childTree.getText());
	            if (items.contains(childTree.getText())) {
        			if (srcTree.getParent() == null) {
        
        			} else {
        				srcTree.getParent().deleteChild(srcTree.getChildIndex());
        				srcTree.getParent().freshenParentAndChildIndexes();
        			}
	            }
	        }
        } else if (type.equalsIgnoreCase(ORDER_BY_ITEM)) {
            for (Tree t : children(srcTree)) {
        		if (t.getText().equalsIgnoreCase(QNAME)) {
        			Tree childchildTree = t.getChild(0);
        			if (items.contains(childchildTree.getText())) {
        				if (srcTree.getParent() != null) {
        					// found a matching node
        					srcTree.getParent().deleteChild(srcTree.getChildIndex());
        					srcTree.getParent().freshenParentAndChildIndexes();
        				}
        			}
        		}
            }
        }

        // process children
        for (Tree t : children(srcTree)) {
            treeRemoveItem((CommonTree)t, type, items);
        }

        return;
    }

    public static void recursivelyRemedyWindowFunction(CommonTree srcTree) {
        if (srcTree.getChildCount() == 0) { return; }
        // process node
    	Boolean foundOrderBy = false;
        if (srcTree.getText().equals(WINDOW)) {
    		// check if all ORDER_BY are removed
    		//System.out.println("srcTree.getText()==> " + srcTree.getText());
    		for (Tree t : children(srcTree)) {
    			String token = t.getText();
    			//System.out.println("token==> " + token);
    			if (token.equalsIgnoreCase(ORDER_BY)) {
    				foundOrderBy = true;
    			} else if (token.equalsIgnoreCase("rows")) {
    				if (foundOrderBy == false) {
    					srcTree.deleteChild(t.getChildIndex());
    					srcTree.freshenParentAndChildIndexes();
    				}
    			}
    		}
        }

        // process children
        for (Tree t : children(srcTree)) {
            recursivelyRemedyWindowFunction((CommonTree)t);
        }
	}

    private static void recursivelyPatchWindowFunction(
    	CommonTree srcTree,
    	List<String> partitionByToRemove,
    	List<String> orderByToRemove) {
        if (srcTree.getChildCount() == 0) { return; }

        // process node
        if (srcTree.getText().equals(PARTITION_BY)) {
            treeRemoveItem(srcTree, QNAME, partitionByToRemove);
            if (srcTree.getChildCount() == 0) {
                if (srcTree.getParent() != null) {
    				srcTree.getParent().deleteChild(srcTree.getChildIndex());
    				srcTree.getParent().freshenParentAndChildIndexes();
                }
            }
            return;
        } else if (srcTree.getText().equals(ORDER_BY)) {
            // 20150311 ToDo: need to apply partition_by solution but order by has asc/desc to string...
            treeRemoveItem(srcTree, ORDER_BY_ITEM, orderByToRemove);
            if (srcTree.getChildCount() == 0) {
                if (srcTree.getParent() != null) {
    				srcTree.getParent().deleteChild(srcTree.getChildIndex());
    				srcTree.getParent().freshenParentAndChildIndexes();
                }
            }
            return;
        }

        // process children
        for (Tree t : children(srcTree)) {
            recursivelyPatchWindowFunction((CommonTree)t, partitionByToRemove, orderByToRemove);
        }
	}

    public static void recursivelyFixWindowFucntion(
        CommonTree srcTree) {
        if (srcTree.getChildCount() == 0) { return; }

        // process node
	    String text = srcTree.getText();
	    System.out.println("Currently processed node==> " + text);
        if (text.equals(PARTITION_BY)) {
            return;
        } else if (text.equals(ORDER_BY)) {
    		if (srcTree.getChildCount() == 0) {
    			if (srcTree.getParent() != null) {
    				// found a matching node
    				srcTree.getParent().deleteChild(srcTree.getChildIndex());
    				srcTree.getParent().freshenParentAndChildIndexes();
    			}
    		}
            return;
        }

        // process children
        for (Tree t : children(srcTree)) {
            recursivelyFixWindowFucntion((CommonTree)t);
        }
    }

    public static void patchWindwingFunction(
        CommonTree srcTree,
        List<String> partitionByToRemove,
        List<String> orderByToRemove) {
    	recursivelyPatchWindowFunction(srcTree, partitionByToRemove, orderByToRemove);
    	recursivelyRemedyWindowFunction(srcTree);
    }

    /*
     * This function tries to patch the srcTree as follow:
     * It checks if there is any nested agg function.
     * If there is, it removes the first level only without looking further.
     * If there is anything patched, it will patch where/having string as well since there
     * might be items using the same item that has been patched.
     */
    public static CommonTree removeAggFunc(
        CommonTree tree,
        List<String> aggregateFuncs,
        int mode) {
        List<CommonTree> aggreagateFuncStacks = new ArrayList<CommonTree>();
        
        return removeAggFuncHelper(tree, aggregateFuncs, aggreagateFuncStacks, mode);
    }
    
    private static Boolean findAggFuncRecursively(
        CommonTree tree,
        List<String> aggregateFuncs) {
        Boolean ret = false;
        
        if (tree.getText().equals(FUNCTION_CALL)) {
            // function
            String funcName = tree.getChild(0).getChild(0).getText(); //FUNCTION_CALL->QNAME->xxx
            if (aggregateFuncs.contains(funcName.toLowerCase())) {
                return true;
            }
        }
        
        for (Tree t : children(tree)) {
            ret = findAggFuncRecursively((CommonTree)t, aggregateFuncs);
            if (ret == true) {
                return true;
            }
        }
        
        return false;
    }
    
    private static CommonTree removeAggFuncHelper(
        CommonTree tree,
        List<String> aggregateFuncs,
        List<CommonTree> aggregateFuncStacks,
        int mode) { // mode=1: remove all aggs. mode=2: remove outer nested aggs
        // process node
        if (tree.getText().equals(FUNCTION_CALL)) {
            // function
            String funcName = tree.getChild(0).getChild(0).getText(); //FUNCTION_CALL->QNAME->xxx
            if (aggregateFuncs.contains(funcName.toLowerCase())) {
                // found an agg
                aggregateFuncStacks.add(tree);
                if (mode == 1) {
                    // remove all aggs
                    if (tree.getParent() == null) {
                        // root
                        tree = (CommonTree) tree.getChild(1);  // 0 is QNAME (the function name), 1 is the argument
                    } else {                      
                        int i=0;
                        for (Tree tt : children(tree)) {
                            //System.out.println("CurTT==> " + VeroSqlParser.createExpression(tt).toString());
                            
                            // to bypass the first element which is the function name
                            if (i != 0) {
                                /*
                                System.out.println("inTree==> " + VeroSqlParser.createExpression(tree).toString());
                                System.out.println("inTree.getChild(1)==> " + VeroSqlParser.createExpression(tt).toString());
                                System.out.println("inTreeParent==> " + VeroSqlParser.createExpression(tree.getParent()).toString());
                                */
                                tree.getParent().addChild(tt);
                                tt.setParent(tree.getParent());
                                tt.freshenParentAndChildIndexes();
                            }
                            i++;
                        }

                        int childIndex = tree.getChildIndex();
                        tree.getParent().deleteChild(childIndex);
                        tree.getParent().freshenParentAndChildIndexes();
                    }
                } else if (mode == 2){
                    List<Tree> childNodes = children(tree);
                    if (childNodes.size() > 1) {
                        for (int i=1; i<childNodes.size(); i++) {
                            Tree child = childNodes.get(i);
                            Boolean foundAgg = findAggFuncRecursively((CommonTree)child, aggregateFuncs);
                            
                            if (foundAgg == true) {
                                //String src = FormatterFactory.getSqlFormatter().formatSql(VeroSqlParser.createExpression(tree)).replaceAll("\"", "");
                                //String wholePatchedPattern = Pattern.quote(src);
                                CommonTree retTree = null;
                                
                                if (tree.getParent() == null) {
                                    // it's root
                                    retTree = (CommonTree)child;
                                } else {
                                    int childIndex = tree.getChildIndex();
                                    tree.getParent().setChild(childIndex, child);
                                    tree.freshenParentAndChildIndexes();
                                    retTree = tree;
                                }
                                aggregateFuncStacks.remove(0);
                                return retTree;
                            }
                        }
                    }
                }
                aggregateFuncStacks.remove(0);
            }
        }

        // process children
        for (Tree t : children(tree)) {
            removeAggFuncHelper((CommonTree)t, aggregateFuncs, aggregateFuncStacks, mode);
        }

        return tree;
    }
    
    public static CommonTree recursivelyPatchCoalesce(
    	CommonTree srcTree,
    	List<LinkedHashSet<String>> listCoalesceSets) {
    	if (srcTree.getChildCount() == 0) { return srcTree; }

        // process node
    	//System.out.println("srcTree.getText()==> " + srcTree.getText());
        if (srcTree.getText().equals(QNAME)) {
    		String qname = combineNames(srcTree);
    		//System.out.println("QNAME==> " + qname);
    
    		for (Set<String> curCoalesceSet :  listCoalesceSets) {
        		if (curCoalesceSet.contains(qname)) {
        		    // in set
        		    //System.out.println("In set==> " + qname);
        
        		    if (srcTree.getParent() == null) {
        		        // it's root
        		        return createFunctionCall("coalesce", combineCoalesceSets(curCoalesceSet));
        		    } else {
                        int childIndex = srcTree.getChildIndex();
                        srcTree.getParent().setChild(childIndex, createFunctionCall("coalesce", combineCoalesceSets(curCoalesceSet)));
                        break;
        		    }
        		}
    		}

            return srcTree;
        } else if (srcTree.getText().equals(FUNCTION_CALL)) {
            // function
            String funcName = srcTree.getChild(0).getChild(0).getText();
            if (funcName.equalsIgnoreCase("coalesce")) {
                Boolean patched = false;
                // find the first child that's in set
                for (Tree t : children(srcTree)) {
                    if (t.getText().equals(QNAME)) {
                        String qname = combineNames((CommonTree) t);

                        for (Set<String> curCoalesceSet :  listCoalesceSets) {
                            if (curCoalesceSet.contains(qname)) {
                                // 20150703 ToDo: find missing elements, but for now, let's just replace the node with the set
                                patched = true;
                                if (srcTree.getParent() == null) {
                                    // it's root
                                    return createFunctionCall("coalesce", combineCoalesceSets(curCoalesceSet));
                                } else {
                                    int childIndex = srcTree.getChildIndex();
                                    srcTree.getParent().setChild(childIndex, createFunctionCall("coalesce", combineCoalesceSets(curCoalesceSet)));
                                }

                                break;
                            }
                        }

                        if (patched == true) {
                            break;
                        }
                    }
                }

                if (patched == false) {
                    // process children
                    for (Tree t : children(srcTree)) {
                        if (!t.getText().equals(QNAME)) {
                            recursivelyPatchCoalesce((CommonTree)t, listCoalesceSets);
                        }
                    }
                }

                return srcTree;
            }
        } else if (srcTree.getText().equalsIgnoreCase("coalesce")) {
            Boolean patched = false;
            // find the first child that's in set
            for (Tree t : children(srcTree)) {
                if (t.getText().equals(QNAME)) {
                    String qname = combineNames((CommonTree) t);

                    for (Set<String> curCoalesceSet :  listCoalesceSets) {
                        for (String curCoalesceSetString : curCoalesceSet) {                            
                            if (CommonUtils.equalsWithIgnoreCaseQuotes(curCoalesceSetString, qname)) {
                                // 20150703 ToDo: find missing elements, but for now, let's just replace the node with the set
                                patched = true;
                                if (srcTree.getParent() == null) {
                                    // it's root
                                    return createFunctionCall("coalesce", combineCoalesceSets(curCoalesceSet));
                                } else {
                                    int childIndex = srcTree.getChildIndex();
                                    srcTree.getParent().setChild(childIndex, createFunctionCall("coalesce", combineCoalesceSets(curCoalesceSet)));
                                }

                                break;
                            }
                        }
                        
                        if (patched == true) {
                            break;
                        }
                    }

                    if (patched == true) {
                        break;
                    }
                }
            }

            if (patched == false) {
                // process children
                for (Tree t : children(srcTree)) {
                    if (!t.getText().equals(QNAME)) {
                        recursivelyPatchCoalesce((CommonTree)t, listCoalesceSets);
                    }
                }
            }

            return srcTree;
        }

        // process children
        for (Tree t : children(srcTree)) {
            recursivelyPatchCoalesce((CommonTree)t, listCoalesceSets);
        }

        return srcTree;
	}

    private static String combineNames(CommonTree node) {
        List<Tree> children = node.getChildren();
        List<String> names = new ArrayList<String>();
        for (Tree child : children) {
            names.add(child.getText());
        }
        Joiner joiner = Joiner.on(".").skipNulls();

        return joiner.join(names);
    }

    private static String combineCoalesceSets(Set<String> coalesceSets) {
        Joiner joiner = Joiner.on(", ").skipNulls();

        return joiner.join(coalesceSets);
    }

    public static void patchTreeByTreesGroupBy(CommonTree srcTree, CommonTree targetTree)
    {
        //System.out.println("Text = " + srcTree.getText() +  " ChildCount = " + srcTree.getChildCount() + " Type = " + srcTree.getType());
        if (srcTree.getChildCount() == 0) {
            return;
        }

        // process node
    	if (srcTree.getText().equals(QUERY_SPEC_NODE)) {
    		// patch
    		targetTree.setParent(srcTree);
    		srcTree.addChild(targetTree);
    		return;
    	}

        // process children
        for (Tree t : children(srcTree)) {
            patchTreeByTreesGroupBy((CommonTree)t, targetTree);
        }

        return;
    }

    private static void gatherTypes(CommonTree srcTree, Map<String, Integer> typesMap)
    {
        if (srcTree.getChildCount() == 0) {
            return;
        }

        // process node
    	if (!typesMap.containsKey(srcTree.getText())) {
    		typesMap.put(srcTree.getText(), new Integer(srcTree.getType()));
    	}

        // process children
        for (Tree subTree : children(srcTree)) {
            gatherTypes((CommonTree)subTree, typesMap);
        }

        return;
    }

    public static CommonTree createGroupByNode(List<String> names)
    {
    	CommonTree groupByNode = createGenericNode(GENERIC_NODE);
    	groupByNode.token.setText(GROUP_BY_NODE);
    	//groupByNode.token.setType(80);
    	groupByNode.token.setType(findType(GROUP_BY_NODE));
    	groupByNode.deleteChild(0);
    
    	for (String name : names) {
    		CommonTree genericNode = createGenericNode(name);
    		genericNode.setParent(groupByNode);
    		groupByNode.addChild(genericNode);
    	}
    
    	return groupByNode;
    }

    public static CommonTree createGenericNode(String name)
    {
		return VeroSqlParser.parseExpression(name);
    }

    public static void patchTreeByTreeSelect(CommonTree srcTree, String target, CommonTree goalTree)
    {
        if (srcTree.getChildCount() == 0) {
        // leaf
		//System.out.println("leaf " + srcTree.getToken().getText());
		if (srcTree.getToken().getText().equals(target)) {
			Tree tempTree = srcTree.getChild(0).dupNode();
			tempTree.deleteChild(0);
			tempTree.addChild(goalTree);
			srcTree.deleteChild(0);
			srcTree.addChild(tempTree);
			//System.out.println("Found 1!!!");
		}
		    return;
        }

        // process node
        System.out.println("node " + srcTree.getToken().getText());
        if (srcTree.getToken().getText().equals(target)) {
    		Tree tempTree = srcTree.getChild(0).dupNode();
    		tempTree.deleteChild(0);
    		tempTree.addChild(goalTree);
    		srcTree.deleteChild(0);
    		srcTree.addChild(tempTree);
    		//System.out.println("Found 2!!!");
    	}

        // process children
        for (Tree t : children(srcTree)) {
            patchTreeByTreeSelect((CommonTree)t, target, goalTree);
        }

        return;
    }

    public static void patchTreeByTree(CommonTree srcTree, String target, CommonTree goalTree)
    {
        if (srcTree.getChildCount() == 0) {
            // leaf
    		//System.out.println("leaf " + srcTree.getToken().getText());
    		if (srcTree.getToken().getText().equals(target)) {
    			srcTree.deleteChild(0);
    			srcTree.addChild(goalTree);
    			System.out.println("Found 1!!!");
    		}
    		return;
        }

        // process node
        System.out.println("node " + srcTree.getToken().getText());
        if (srcTree.getToken().getText().equals(target)) {
    		srcTree.deleteChild(0);
    		srcTree.addChild(goalTree);
    		//System.out.println("Found 2!!!");
        }

        // process children
        for (Tree t : children(srcTree)) {
            patchTreeByTree((CommonTree)t, target, goalTree);
        }

        return;
    }

    public static void patchTreeByTreesSelect(CommonTree srcTree, String target, List<CommonTree> goalTrees)
    {
        if (srcTree.getChildCount() == 0) {
            // leaf
    		//System.out.println("leaf " + srcTree.getToken().getText());
    		if (srcTree.getToken().getText().equals(target)) {
    			Tree templateTree = srcTree.getChild(0).dupNode();
    			boolean firstTime = false;
    			for (CommonTree t : goalTrees) {
    			Tree tempTree = templateTree.dupNode();
    			tempTree.deleteChild(0);
    			tempTree.addChild(t);
    			if (firstTime == true) {
    				srcTree.deleteChild(0);
    				firstTime = true;
    			}
    			srcTree.addChild(tempTree);
    			//System.out.println("Found 1!!!");
    			}
    		}
    	    return;
        }

        // process node
        //System.out.println("node " + srcTree.getToken().getText());
        if (srcTree.getToken().getText().equals(target)) {
            Tree templateTree = srcTree.getChild(0).dupNode();
            boolean firstTime = false;
    		for (CommonTree t : goalTrees) {
    			Tree tempTree = templateTree.dupNode();
    			tempTree.deleteChild(0);
    			tempTree.addChild(t);
    			if (firstTime == true) {
    				srcTree.deleteChild(0);
    				firstTime = true;
    			}
    			srcTree.addChild(tempTree);
    			//System.out.println("Found 2!!!");
    		}
    	}

        // process children
        for (Tree t : children(srcTree)) {
            patchTreeByTreesSelect((CommonTree)t, target, goalTrees);
        }

        return;
    }

    public static void patchTreeByString(CommonTree tree, List<String> columns, String goal) {
        if (tree.getChildCount() == 0) {
            // leaf
    		if (columns.contains(tree.getText())) {
    		    Tree parent = tree.getParent();
    
    		    // only patch tree when it's QNAME node
    		    if (parent.getText().equals("QNAME")) {
        		    Tree tmpTree = createGenericNode(goal);
        		    // remove QNAME at index 0
        		    tmpTree.deleteChild(0);
                    tmpTree.setParent(tree.getParent());
                    parent.deleteChild(0);
                    parent.addChild(tmpTree);
                    parent.addChild(tree);
        
        		    ((CommonTree)tmpTree).getToken().setText(goal);
        		    ((CommonTree)tmpTree).getToken().setType(tree.getType());
    		    }
    		}
    		return;
        }

        // process children
        for (Tree t : children(tree)) {
            patchTreeByString((CommonTree)t, columns, goal);
        }

        return;
    }

    public static void updateTreeByString(CommonTree tree, String target, String goal) {
        if (tree.getChildCount() == 0) {
            // leaf
    		if (tree.getText().equals(target)) {
    			//modifyText(tree, goal+'.'+target);
    			modifyText(tree, goal);
    			//System.out.println("Found 1!!!");
    		}
    		return;
        }

        // process node
    	if (tree.getText().equals(target)) {
    		modifyText(tree, goal+'.'+target);
    		//System.out.println("Found 2!!!");
    	}

        // process children
        for (Tree t : children(tree)) {
            updateTreeByString((CommonTree)t, target, goal);
        }

        return;
    }

    public static void modifyText(CommonTree tree, String goal)
    {
    	if (tree.getToken() == null) {
    		return;
    	}
    
    	tree.getToken().setText(goal);
    }

    private static List<Tree> children(Tree tree) {
        List<Tree> list = new ArrayList<Tree>();

        for (int i = 0; i < tree.getChildCount(); i++) {
            list.add(tree.getChild(i));
        }
        return list;
    }
}
