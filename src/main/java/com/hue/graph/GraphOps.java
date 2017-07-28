package com.hue.graph;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.hue.model.persist.Schema;

public class GraphOps {
	protected static final Logger logger = LoggerFactory.getLogger(GraphOps.class.getName());

//	private static Pipe<Vertex, ?> virtualPathPipe;
//	private static Object vp = __.out("has_virtual_expression").out("composed_of")
//								.loops().has("label", "measure")
//								.filter(tr -> tr.asAdmin().getSideEffects().)

//	@SuppressWarnings("unchecked")
//	public static void precompilePipes() {
//		if (virtualPathPipe == null) {
//			__.out('has_virtual_expression').out('composed_of');
////			virtualPathPipe = GremlinGroovyScriptEngine (
////					"_().out('has_virtual_expression').out('composed_of').loop(2){it.loops < 150}{true}.has('label','measure').filter{it.out('has_virtual_expression').count()==0}.path");
//		}
//	}
//
//    @SuppressWarnings("unchecked")
//	public static ArrayList<Path> qeGetVirtualPaths(Schema p, Graphable... targets) throws GraphException {
//        if (virtualPathPipe == null) precompilePipes();
//
//        Set<Graphable> tgtSet = Sets.newHashSet(targets);
//        ArrayList<Vertex> starts = Lists.newArrayList();
//        
//        for (Graphable t : tgtSet) {
//            starts.add(t.v());
//        }
//        virtualPathPipe.setStarts(starts.iterator());
//
//        ArrayList<Path> paths = Lists.newArrayList();
//        try {
//            for (Object o : virtualPathPipe) {
//                ArrayList<Vertex> pathy = (ArrayList<Vertex>) o;
//                Path path = new Path();
//                for (int j = 0; j < pathy.size(); j++) {
//                    Vertex v = pathy.get(j);
//                    path.add((Graphable) v.property("object").value());
//                }
//                paths.add(path);
//            }
//            return paths;
//        }
//        catch (Exception e) {
//            throw new GraphException(e);
//        }
//    }

	public static ArrayList<Path> qeGetBasePaths(Schema schema, Graphable... targets) throws GraphException {
		Set<Graphable> tgtSet = Sets.newHashSet(targets);

		ArrayList<Path> paths = Lists.newArrayList();
		try {
			for (Graphable vb : tgtSet) {
				List<org.apache.tinkerpop.gremlin.process.traversal.Path> res = Lists.newArrayList();
				GraphTraversalSource g = schema.getGraph().traversal();
				res = g.V(vb.v().id())
						.out(Edges.HAS_EXPRESSION.getName())
						.out(Edges.QUERIES_FROM.getName())
						.hasLabel("table")
						.path().toList();
				for (org.apache.tinkerpop.gremlin.process.traversal.Path innerPath : res) {
					Path path = new Path();
					for (int j = 0; j < innerPath.size(); j++) {
						Vertex v = innerPath.get(j);
						path.add((Graphable) v.property("object").value());
					}
					paths.add(path);
				}
			}
			Set<Graphable> end = paths.stream().map(Path::endNode).collect(Collectors.toSet());
			for (Graphable start : end) {
				GraphTraversalSource g = schema.getGraph().traversal();
				List<org.apache.tinkerpop.gremlin.process.traversal.Path> res = 
						g.V(start.v().id())
						.in("joins").in("joins").path().toList();

				for (org.apache.tinkerpop.gremlin.process.traversal.Path o : res) {
					Path path = new Path();
					for (int j = 0; j < o.size(); j++) {
						Vertex v = o.get(j);
						path.add((Graphable) v.property("object").value());
					}
					for (int x = 0; x < paths.size(); x++) {
						Path basePath = paths.get(x);
						if (basePath.endNode().equals(path.startNode())) {
							paths.add(basePath.append(path));
						}
					}
				}
			}
			return paths;
		}
		catch (Exception e) {
			throw new GraphException(e);
		}
	}

//	@SuppressWarnings({ "unchecked", "rawtypes" })
//	public static List<Path> qeGetSecondaryPaths(Schema schema, List<Table> targetRoots, List<Expressible> expressible)
//			throws GraphException {
//		Set<Graphable> tgtSet = Sets.newHashSet(expressible);
//
//		ArrayList<Path> paths = Lists.newArrayList();
//		try {
//			for (Graphable vb : tgtSet) {
//				List<List> res = Lists.newArrayList();
//				res = new GremlinPipeline<Vertex, Vertex>(vb.v()).out(Edges.HAS_EXPRESSION.getName())
//						.out(Edges.QUERIES_FROM.getName()).has("label", "table").path().toList();
//
//				for (List<Vertex> innerPath : res) {
//					Path path = new Path();
//					for (int j = 1; j < innerPath.size(); j++) {
//						Vertex v = innerPath.get(j);
//						path.add((Graphable) v.property("object").value());
//					}
//					paths.add(path);
//				}
//			}
//			List<Graphable> end = paths.stream().map(Path::endNode).collect(Collectors.toList());
//
//			String tableList = targetRoots.stream().map((n) -> "'" + n.getName() + "'").collect(
//					Collectors.joining(","));
//			for (Graphable start : end) {
//				Pipe<Vertex, ?> pipe = Gremlin.compile(
//						"_().sideEffect{x=it}.out('joins').dedup().out('joins').filter{it.allow_roll_down!=0}"
//								+ ".dedup().filter{x!=it}.loop(6){it.loops < 150}{true}.has('label','table')"
//								+ ".filter{[" + tableList + "].contains(it.name)}.path");
//
//				pipe.setStarts(new SingleIterator<Vertex>(start.v()));
//				for (Object o : pipe) {
//					ArrayList<Vertex> pathy = (ArrayList<Vertex>) o;
//					Path path = new Path();
//					boolean skipPath = false;
//					for (int j = 0; j < pathy.size(); j++) {
//						Vertex v = pathy.get(j);
//						Graphable gv = (Graphable) v.property("object");
//
//						// doing this because the filter above
//						// .filter{it.allow_roll_down!=0} is not working
//						if (gv instanceof Join) {
//							Join je = (Join) gv;
//							if (je.getAllowRollDown() == 0) {
//								skipPath = true;
//								break;
//							}
//						}
//						path.add(gv);
//					}
//
//					// skipping paths with any disallowed roll downs
//					if (!skipPath) {
//						for (int x = 0; x < paths.size(); x++) {
//							Path basePath = paths.get(x);
//							if (basePath.endNode().equals(path.startNode())) {
//								paths.add(basePath.append(path));
//							}
//						}
//					}
//				}
//
//			}
//			Iterator<Path> it = paths.iterator();
//			while (it.hasNext()) {
//				Path ipath = it.next();
//				if (!targetRoots.contains(ipath.endNode()))
//					it.remove();
//			}
//			return paths;
//		}
//		catch (Exception e) {
//			throw new GraphException(e.getMessage());
//		}
//	}
	
	public static ArrayList<Path> qeGetAllBasePaths(Schema schema) throws GraphException {
		ArrayList<Path> paths = GraphOps.qeGetBasePaths(schema, schema.getDimensions().toArray(new Graphable[schema.getDimensions().size()]));
		paths.addAll(
				 GraphOps.qeGetBasePaths(schema, schema.getMeasures().toArray(new Graphable[schema.getMeasures().size()]))
				);
		return paths;
	}
}
