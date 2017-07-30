package com.hue.graph;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.hue.model.Expressible;
import com.hue.model.Table;
import com.hue.model.persist.Schema;
import com.hue.planner.IPathService;

public class PathService implements IPathService {

    private final Schema schema;

	public PathService(Schema schema){
		this.schema = schema;
	}

	public Schema getSchema(){
		return schema;
	}

	@Override
	public List<Path> getBasePaths(Expressible... expressible) throws GraphException {
        if(getSchema() == null) {
            return Collections.emptyList();
        }
		return GraphOps.qeGetBasePaths(getSchema(), expressible);
	}

	@Override
	public List<Path> getVirtualPaths(Expressible... expressible) throws GraphException {
//        if(getSchema() == null) {
//            return Collections.emptyList();
//        }
//		return GraphOps.qeGetVirtualPaths(getSchema(), expressible);
		return Collections.emptyList();
	}

	@Override
	public List<Path> getBasePathsToTargetRootTable(Table rootTable, Expressible... expressible) throws GraphException {
		List<Path> allPaths = getBasePaths(expressible);
		return allPaths.stream().filter(p -> p.endNode().equals(rootTable)).collect(Collectors.toList());
	}

	@Override
	public List<Path> getSecondaryPaths(List<Table> targetRoots, List<Expressible> expressible) throws GraphException {
        if(getSchema() == null) {
            return Collections.emptyList();
        }
		return GraphOps.qeGetSecondaryPaths(getSchema(), targetRoots, expressible);
	}

	/**
	 * Given a set of expressibles return a list of expressible that can
	 * correctly work together.  The goal of this message is to prevent users
	 * from encountering cross joins.  The suppression level should eventually
	 * be configurable.
	 *
	 * @param expressible
	 * @return
	 * @throws GraphException
	 */
//    public DynamicUniverse getDynamicUniverse(List<Expressible> expressible) throws GraphException {
//        return getDynamicUniverse(getProject(), expressible);
//    }
//
//    public DynamicUniverse getDynamicUniverse(Project project, List<Expressible> expressible) throws GraphException{
//		DynamicUniverse du = new DynamicUniverse();
//        if(expressible.isEmpty()) {     // XXX - bail out; otherwise ArrayOutOfBoundIndex below
//            return du;
//        }
//
//        ArrayList<Path> paths = GraphOps.qeGetAllBasePaths(project);
//
//		Map<Expressible,Set<Table>> expToTable = Maps.newHashMap();
//		Map<Table,Set<Expressible>> tableToExp = Maps.newHashMap();
//		
//		// maintain a list of sub table nodes
//		// will need to prune the roots of these
//		// since they will incorrectly minimize the 
//		// common roots scope.  ie orders <- order_details
//		// if orders is considered a common root then it 
//		// will remove valid expressible
//		Set<VeroObj> subtables = Sets.newHashSet();
//		for(Path path : paths){
//			List<VeroObj> temps = path.nodes().stream().filter((n) -> { return n instanceof Table;}).collect(Collectors.toList());
//			if(path.endNode() instanceof Table) temps.remove(path.endNode());
//			subtables.addAll(temps);
//			
//			if(tableToExp.get(path.endNode()) == null){
//				Set<Expressible> le = Sets.newHashSet();
//				le.add((Expressible) path.startNode());
//				tableToExp.put((Table) path.endNode(),le);
//			}else{
//				tableToExp.get(path.endNode()).add((Expressible) path.startNode());
//			}
//
//			if(expToTable.get(path.startNode()) == null){
//				Set<Table> le = Sets.newHashSet();
//				le.add((Table) path.endNode());
//				expToTable.put((Expressible) path.startNode(),le);
//			}else{
//				expToTable.get(path.startNode()).add((Table) path.endNode());
//			}
//		}
//		
//		Set<Table> commonRoots = Sets.newHashSet();
//		for(int i=0;i<expressible.size();i++){
//			Set<Table> t = expToTable.get(expressible.get(i));
//			if(t==null || t.isEmpty()) continue; 
//			commonRoots.addAll(t);
//		}
//	
//		if(commonRoots.isEmpty()){
//			processEmptyRoots(commonRoots, expressible);
//		}
//		commonRoots.removeAll(subtables);
//		
//		//now filter root to only include commons
//		Set<VeroObj> allExpTables = Sets.newHashSet();
//		for(int i=0;i<expressible.size();i++){
//			Set<Table> t = expToTable.get(expressible.get(i));
//			if(t==null || t.isEmpty()) continue; 
//			allExpTables.addAll(t);			
//		}
//		if(allExpTables.size()>0)
//			commonRoots.retainAll(allExpTables);
//		
//		Set<Expressible> valids = Sets.newHashSet();
//		Iterator<Table> it = commonRoots.iterator();
//		while(it.hasNext()){
//			Set<Expressible> set = tableToExp.get(it.next());
//			valids.addAll(set);
//		}
//		
//		du.setSameGranularity(valids);
//		List<Expressible> selMeasures = expressible.stream().filter((e) -> e instanceof Measure).collect(Collectors.toList());
//		if(selMeasures.size()>0){
//			paths.stream().filter((p) -> p.startNode() instanceof Measure).map(Path::startNode).forEach((vo) -> du.setAsymmetricKey((Measure) vo));
//
//			Iterator<Measure> ita = du.getAsymmetricKeys().iterator();
//			while(ita.hasNext()){
//				Measure am = ita.next();
//				expToTable.get(am).stream().forEach(t -> {
//					tableToExp.get(t).stream().filter(e -> e instanceof Dimension).forEach( d -> { du.appendAsymmetricDimension(am, (Dimension) d); });
//				});
//			}
//		}
//
//		// TODO missing case there are more than one measure and
//		// the additional measure could have mixed dimensionality
//		// at a different level. Those mixed level dimensions should
//		// be included on the fact that they have a common dimension
//		// in the current expressible.  I think we need a map from
//		// measure -> [ dimensions ].
//		if(selMeasures.size()>1){
//
//		}
//
//		du.promote(GraphOps.qeGetAllVirtualToBaseNodeMap(), selMeasures.size()>0);
//		return du;
//	}

//	private void processEmptyRoots(Set<Table> commonRoots, List<Expressible> expressible) throws GraphException {
//		List<Path> vp = getVirtualPaths(expressible.toArray(new Expressible[expressible.size()]));
//		List<VeroObj> baseExp = vp.stream().map(Path::endNode).collect(Collectors.toList());
//		List<Path> bp = getBasePaths(baseExp.toArray(new Expressible[baseExp.size()]));
//		commonRoots.addAll(bp.stream().map((e) -> { return ((Table) e.endNode()); }).collect(Collectors.toSet()));
//	}

}
