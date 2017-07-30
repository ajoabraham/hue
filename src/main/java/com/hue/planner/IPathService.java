package com.hue.planner;

import java.util.List;

import com.hue.graph.GraphException;
import com.hue.graph.Path;
import com.hue.model.Expressible;
import com.hue.model.Table;

public interface IPathService {
	
	/**
	 * Given a set of Expressibles this will return all of the available 
	 * Paths starting with the expressible and ending with a chain of tables. 
	 * Currently only supports Dimension and Measure Expressibles.
	 * 
	 * All the tables in a join sequence must be incoming.
	 * 
	 * A base path:
	 * Dimension --> Expression --> Table <-- JoinExpression <-- Table ...
	 * 
	 * @param expressible
	 * @return
	 * @throws GraphException 
	 */
	public List<Path> getBasePaths(Expressible... expressible) throws GraphException;
	
	/**
	 * Same as the getBasePaths but only returns paths ending with the specified root table.
	 * @param rootTable
	 * @param expressible
	 * @return
	 * @throws GraphException
	 */

	public List<Path> getBasePathsToTargetRootTable(Table rootTable, Expressible... expressible) throws GraphException;
	/**
	 * Returns a path starting from a set of measures and going down "has_virtual_expression" edge
	 * until we meet a measure node that doest have "has_virtual_expression" these terminal nodes
	 * are considered base components of a virtual expression.
	 * 
	 * @param expressible
	 * @return
	 * @throws GraphException
	 */
	public List<Path> getVirtualPaths(Expressible... expressible) throws GraphException;
	
	public List<Path> getSecondaryPaths(List<Table> targetRoots, List<Expressible> start) throws GraphException;

}
