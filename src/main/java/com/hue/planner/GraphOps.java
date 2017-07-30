package com.hue.planner;

public class GraphOps {
//	private static final Logger logger = LoggerFactory.getLogger(GraphOps.class.getName());
//
//	public static Node getNodeById(String id, Label lbl){
//		GraphDatabaseService gdb = GraphService.getDb();
//		Node nd = null;
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			if (gdb.findNodesByLabelAndProperty(lbl, "id", id).iterator().hasNext())
//				nd = gdb.findNodesByLabelAndProperty(lbl, "id", id).iterator().next();
//		}
//
//		return nd;
//	}
//
//	public static Set<VeroNode> getAllExpressibles(Datasource ds) throws QEException{
//		Set<VeroNode> reachable = Sets.newHashSet();
//		VeroFactory vf = new VeroFactory();
//
//		GraphDatabaseService gdb = GraphService.getDb();
//		try(Transaction tx = gdb.beginTx())
//		{
//			TraversalDescription td = gdb.traversalDescription()
//				 .relationships(RelTypes.HAS_TABLE, Direction.OUTGOING)
//				 // VB-169 - Added incoming filter below
//				 .relationships(RelTypes.QUERIES_FROM,Direction.INCOMING)
//				 .relationships(RelTypes.HAS_EXPRESSION)
//				 .evaluator(PathEvaluators.HAS_EXPRESSION_EVAL)
//				 .relationships(RelTypes.COMPOSED_OF)
//				 .relationships(RelTypes.HAS_VIRTUAL_EXPRESSION)
//				 .evaluator(PathEvaluators.END_NODE_IS_EXP_OR_VIRTUAL_EXP);
//
//			for(Path p : td.traverse(ds.getNode()))
//			{
//				logger.debug(p.toString());
//				if(p.startNode().equals(ds.getNode())){
//					reachable.add(vf.createVeroNode(p.endNode()));
//				}
//			}
//		}catch(Exception e){
//			logger.error("Unable to discover Expressibles attached to the provided datasource.", e);
//			throw new QEException(e);
//		}
//
//		return reachable;
//	}
//
//	public Path subPath( Path source, int from, int to )
//    {
//        if ( source.length() < from || source.length() < to)
//            throw new IllegalArgumentException( source + " isn't long enough (" + from + ")" );
//
//        Iterator<Node> nodes = source.nodes().iterator();
//        Iterator<Relationship> relationships = source.relationships().iterator();
//
//        for ( int i = 0; i < from; i++ )
//        {
//            nodes.next();
//            relationships.next();
//        }
//
//        PathImpl.Builder builder = new PathImpl.Builder( nodes.next() );
//        while ( relationships.hasNext() )
//        {
//            builder = builder.push( relationships.next() );
//        }
//        return builder.build();
//    }
//
//	public static void writePlanToJson(JsonGenerator g, OptimizedPlan plan) throws JsonGenerationException, IOException {
//		if(plan == null || plan.getRootTable()==null) return;
//
//		GraphDatabaseService gdb = GraphService.getDb();
//		Transaction tx = gdb.beginTx();
//		try
//		{
//			if(plan instanceof OptimizedPlan){
//				logger.debug("Writing OptimizedPlan to graph details json.");
//				// write out messages
//				g.writeArrayFieldStart("planMessages");
//					plan.getPlanMessages().stream().forEach((m)->{
//						try {
//							g.writeString(m);
//						} catch (Exception e) {
//							logger.debug("couldnt write out plan messages.");;
//						};
//					});
//				g.writeEndArray();
//
//
//				g.writeStringField("planType", "op");
//
//				//start nodes
//				g.writeArrayFieldStart("nodes");
//
//				List<Node> nodes = Lists.newArrayList();
//				List<Relationship> rels = Lists.newArrayList();
//				Set<Path> pathsToPlan = Sets.newHashSet(plan.getPlanPaths());
//
//				// write the base plan to json
//				writePathNodesToJSON(g, plan, nodes,plan.getPlanPaths(),plan.getRootTable().getName());
//
//				// add disjoint paths as well.  We want to group the nodes by their root table
//				// for easier access to the group.
//				for(DisjointPlan dp : plan.getDisjointedPlans()){
//					pathsToPlan.addAll(dp.getNeoPaths());
//					writePathNodesToJSON(g, plan, nodes,dp.getNeoPaths(),dp.getRootTable().getName());
//				}
//
//				// Now process any table hints that were not
//				// included in the selected disjoint and normal plans
//				plan.getSourceBlock().getTableHints().forEach((h) -> {
//					if(!nodes.contains(h.getNode())){
//						Node n = h.getNode();
//						nodes.add(n);
//						try {
//							g.writeStartObject();
//							g.writeStringField("label", Iterables.get(n.getLabels(),0).toString());
//							g.writeStringField("nodeGroup","hintGroup");
//							g.writeNumberField("nodeId", n.getId());
//
//							n.getPropertyKeys().forEach((key) ->{
//								try {
//									Object v = n.getProperty(key);
//									if(v instanceof String)
//										g.writeStringField(key, (String)v);
//									if(v instanceof Number)
//										g.writeObjectField(key, v);
//								} catch (Exception e) {
//									throw new RuntimeException(e);
//								}
//							});
//							g.writeStringField("disjointType",DisjointType.NONE.toString());
//							g.writeBooleanField("isHint",true);
//							g.writeEndObject();
//						} catch (Exception e1) {
//							logger.error("Unable to process table hint to json: " + h.getName());
//							throw new RuntimeException(e1);
//						}
//
//					}
//				} );
//
//				// end Nodes
//				g.writeEndArray();
//
//				// write relationships
//				g.writeArrayFieldStart("links");
//
//				for(Path p : pathsToPlan){
//					for(Relationship r : p.relationships()){
//						if(rels.contains(r))
//							continue;
//
//						rels.add(r);
//						g.writeStartObject();
//						g.writeStringField("type", r.getType().name());
//
//						r.getPropertyKeys().forEach((key) ->{
//							try {
//								Object v = r.getProperty(key);
//								if(v instanceof String)
//									g.writeStringField(key, (String)v);
//								if(v instanceof Number)
//									g.writeObjectField(key, v);
//							} catch (Exception e) {
//								logger.error("Runtime exception while writing properties to json from relationship.");
//								throw new RuntimeException(e);
//							}
//						});
//
//						g.writeNumberField("source", nodes.indexOf(r.getStartNode()));
//						g.writeNumberField("target",nodes.indexOf(r.getEndNode()));
//
//						g.writeNumberField("sourceId",r.getStartNode().getId());
//						g.writeNumberField("targetId",r.getEndNode().getId());
//						g.writeEndObject();
//					}
//				}
//				//end relationships
//				g.writeEndArray();
//			}
//		}finally{
//			tx.close();
//		}
//
//	}
//
//	public static boolean hasLabel(Node node, Label label) {
//		GraphDatabaseService gdb = GraphService.getDb();
//		boolean res = false;
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			res = node.hasLabel(label);
//		}
//		return res;
//	}
//
//	public static Object getProperty(Node node, String prop_name){
//		GraphDatabaseService gdb = GraphService.getDb();
//		Object res = null;
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			res = node.getProperty(prop_name);
//		}
//		return res;
//	}
//
//	public static boolean contains(Path path, VeroNode n) {
//		boolean hasNode = false;
//		Node testNode = n.getVeroObj();
//		try ( Transaction tx = GraphService.getDb().beginTx() )
//		{
//			for(Node neoNode : path.nodes()){
//				if(neoNode.equals(testNode)){
//					hasNode = true;
//					break;
//				}
//			}
//		}
//		return hasNode;
//	}
//
//	public static void printPaths(Set<Path> paths){
//		try ( Transaction tx = GraphService.getDb().beginTx() )
//		{
//			logger.debug(paths.toString());
//		}
//	}
//
//	public static JoinDef getJoinDef(Table lt, Table rt) throws QEException{
//		try ( Transaction tx = GraphService.getDb().beginTx() )
//		{
//			if(lt.getDatasource() != rt.getDatasource()){
//				throw new QEException("Tables are not in the same datasource.");
//			}
//			Relationship found = null;
//			for(Relationship rel : lt.getNode().getRelationships(RelTypes.JOINS, Direction.BOTH)){
//				if(rel.isType(RelTypes.JOINS) &&
//						(rel.getEndNode().equals(rt.getNode()) || rel.getStartNode().equals(rt.getNode()))){
//					found = rel;
//					break;
//				}
//			}
//			if(found==null){
//				throw new QEException("No such join definitions exists.");
//			}else{
//				Table left, right;
//				if(lt.getNode().equals(found.getStartNode())){
//					left = lt;
//					right = rt;
//				}else{
//					left = rt;
//					right = lt;
//				}
//				String exp = (String) found.getProperty("join_formula");
//				JoinType t = JoinType.valueOf((String) found.getProperty("join_type"));
//				CardinalityType c = CardinalityType.valueOf((String) found.getProperty("cardinality_type"));
//				JoinDef jd = new JoinDef(left, right,exp,t);
//				jd.setCardinalityType(c);
//				jd.setCost((int) found.getProperty("cost"));
//				jd.setAllowRollDown((int) found.getProperty("allow_rundown",-1));
//
//				return jd;
//			}
//		}
//	}
//
//	public static void getCascadingDependants(Dependent<Graphable> dep) throws PersistenceException{
//		Graphable g = dep.getDependent();
//		GraphDatabaseService gdb = GraphService.getDb();
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			Node n = null;
//			try{
//				ResourceIterable<Node> f = gdb.findNodesByLabelAndProperty(g.getNodeLabel(), "id", g.getId());
//				n = f.iterator().next();
//			}catch(NoSuchElementException er){
//				// some cases the column was not loaded
//				if(!(g instanceof Column)){
//					logger.error("Could not find node " + dep.getDependent() + " . It may have already been deleted.");
//					throw new PersistenceException("Could not find node in graph.") ;
//				}
//			}
//
//			if(g instanceof Column){
//				List<Graphable> exp = getExpressionsUsingColumn((Column)g);
//				for(Graphable e : exp){
//					Dependent<Graphable> next = new Dependent<Graphable>(e,  Dependent.DepType.HARD);
//					dep.addChild(next);
//					getCascadingDependants(next);
//				}
//				return ;
//			}
//
//			for(Relationship rel : n.getRelationships(Direction.INCOMING)){
//				if(rel.isType(RelTypes.HAS_TABLE) ||
//						rel.isType(RelTypes.COLUMN_OF)	||
//						rel.isType(RelTypes.JOINS)){
//					continue;
//				}
//
//				Node start = rel.getStartNode();
//				Dependent<Graphable> next = null;
//				if(Iterables.size(start.getRelationships(Direction.OUTGOING))==1){
//					next = new Dependent<Graphable>(nodeToGraphable(start),Dependent.DepType.HARD);
//				}else if(start.hasLabel(NodeLabels.EXPRESSION)){
//					boolean b = (boolean)start.getProperty("is_virtual");
//					if(b){
//						next = new Dependent<Graphable>(nodeToGraphable(start),Dependent.DepType.HARD);
//					}else{
//						next = new Dependent<Graphable>(nodeToGraphable(start),Dependent.DepType.SOFT);
//					}
//				}else{
//					next = new Dependent<Graphable>(nodeToGraphable(start),Dependent.DepType.SOFT);
//				}
//				dep.addChild(next);
//				if(next.getDepType() == DepType.HARD){
//					getCascadingDependants(next);
//				}
//			}
//		}
//	}
//
//	public static Graphable nodeToGraphable(Node n) throws PersistenceException{
//		Label l = Iterables.get(n.getLabels(),0);
//		Graphable g = null;
//
//		if(l.equals(NodeLabels.DIMENSION)){
//			g =Dimension.find((String) n.getProperty("id"));
//		}else if(l.equals(NodeLabels.MEASURE)){
//			g =Measure.find((String) n.getProperty("id"));
//		}else if(l.equals(NodeLabels.EXPRESSION)){
//			g =Expression.find((String) n.getProperty("id"));
//		}else if(l.equals(NodeLabels.COLUMN)){
//			g =Column.find((String) n.getProperty("id"));
//		}else if(l.equals(NodeLabels.TABLE)){
//			g =Table.find((String) n.getProperty("id"));
//		}else if(l.equals(NodeLabels.DATASOURCE)){
//			g =Datasource.find((String) n.getProperty("id"));
//		}
//
//		return g;
//	}
//	public static void save(Dimension d) throws ValidationException{
//		if(d.isConstant()) throw new ValidationException("Graph doesnt save constants.");
//
//		d.validate();
//		Node dim = d.getNode();
//		GraphDatabaseService gdb = GraphService.getDb();
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			if(dim == null){
//				dim = gdb.createNode(NodeLabels.DIMENSION);
//			}
//
//			dim.setProperty("id", d.getId());
//			dim.setProperty("name", d.getName());
//			dim.setProperty("project_id", d.getProject().getId());
//
//			tx.success();
//		}
//	}
//
//	public static void save(Measure m) throws ValidationException{
//		if(m.isConstant()) throw new ValidationException("Graph doesnt save constant measures.");
//
//		m.validate();
//		Node mn = m.getNode();
//		GraphDatabaseService gdb = GraphService.getDb();
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			if(mn == null){
//				mn = gdb.createNode(NodeLabels.MEASURE);
//			}
//
//			mn.setProperty("id", m.getId());
//			mn.setProperty("name", m.getName());
//			mn.setProperty("project_id", m.getProject().getId());
//
//			tx.success();
//		}
//	}
//
//	public static void save(Expression e) throws ValidationException{
//		if(e.isConstant()) throw new ValidationException("Graph doesnt save constant expressions.");
//		e.validate();
//
//		Node en = e.getNode();
//		Node dep;
//		String projid = null;
//
//		if(e.getMeasure() != null){
//			dep = e.getMeasure().getNode();
//			projid = e.getMeasure().getProject().getId();
//		}else if(e.getDimension() != null){
//			dep = e.getDimension().getNode();
//			if(e.getDimension().getProject() != null){
//				projid = e.getDimension().getProject().getId();
//			}
//		}else{
//			return;
//		}
//
//		boolean hasOrel = hasOutgoingRelBetween(dep, en);
//
//		GraphDatabaseService gdb = GraphService.getDb();
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			if(en == null){
//				en = gdb.createNode(NodeLabels.EXPRESSION);
//			}
//
//			if(!hasOrel){
//				if(e.isVirtual()){
//					dep.createRelationshipTo(en, RelTypes.HAS_VIRTUAL_EXPRESSION);
//				}else{
//					dep.createRelationshipTo(en, RelTypes.HAS_EXPRESSION);
//				}
//			}
//
//			en.setProperty("id", e.getId());
//			en.setProperty("formula", e.getFormula());
//			en.setProperty("is_virtual", e.isVirtual());
//			if(projid != null)
//				en.setProperty("project_id", projid);
//			List<String> columnSet = e.getColumns();
//			if(columnSet.size()>0) en.setProperty("column_set", String.join(",", columnSet.toArray(new String[0])));
//
//			tx.success();
//		}
//		if(e.isVirtual()){
//			for(ExpressionProp ep : e.getExpressionProps()){
//				if(ep.getType()!=VeroType.MEASURE) continue;
//				try {
//					Node end = GraphOps.getNodeById(ep.getTargetObjectId(), NodeLabels.get(ep.getType()));
//					if(!hasOutgoingRelBetween(en, end)){
//						try ( Transaction tx = gdb.beginTx() )
//						{
//							en.createRelationshipTo(end, RelTypes.COMPOSED_OF);
//							tx.success();
//						}
//					}
//				} catch (QEException e1) {
//					throw new ValidationException("Expression prop likely contains non-graphable VeroType", e1);
//				}
//			}
//		}else{
//			for(Table t : e.getTables()){
//				if(!hasOutgoingRelBetween(en, t.getNode())){
//					try ( Transaction tx = gdb.beginTx() )
//					{
//						en.createRelationshipTo(t.getNode(), RelTypes.QUERIES_FROM);
//						tx.success();
//					}
//				}
//			}
//		}
//
//	}
//
//	public static void save(Datasource d) throws ValidationException{
//		d.validate();
//		Node dn=d.getNode();
//
//		GraphDatabaseService gdb = GraphService.getDb();
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			if(dn == null)
//				dn = gdb.createNode(NodeLabels.DATASOURCE);
//
//
//			dn.setProperty("id", d.getId());
//			dn.setProperty("name", d.getName());
//			dn.setProperty("project_id", d.getProject().getId());
//
//			tx.success();
//		}
//	}
//
//	public static void save(Column c) throws ValidationException{
//		c.validate();
//		Node cn = c.getNode();
//		Node tn = c.getTable().getNode();
//		boolean hasOrel = hasOutgoingRelBetween(cn,tn);
//
//		GraphDatabaseService gdb = GraphService.getDb();
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			if(cn == null){
//				cn = gdb.createNode(NodeLabels.COLUMN);
//			}
//
//			if(!hasOrel)
//				cn.createRelationshipTo(tn, RelTypes.COLUMN_OF);
//
//			cn.setProperty("id", c.getId());
//			cn.setProperty("name", c.getName());
//
//			tx.success();
//		}
//	}
//
//	public static void save(Table t) throws ValidationException{
//		t.validate();
//		Node tn = t.getNode();
//		Node dn = t.getDatasource().getNode();
//		boolean hasOrel = hasOutgoingRelBetween(dn, tn);
//
//		GraphDatabaseService gdb = GraphService.getDb();
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			if(tn == null){
//				tn = gdb.createNode(NodeLabels.TABLE);
//			}
//
//			tn.setProperty("id", t.getId());
//			tn.setProperty("name", t.getName());
//			if(t.getSchemaName() !=null){
//				tn.setProperty("schema_name", t.getSchemaName());
//			}
//			tn.setProperty("physical_name", t.getPhysicalName());
//			tn.setProperty("row_count", t.getRowCount());
//			tn.setProperty("table_type", t.getTableType());
//
//			if(!hasOrel)
//				dn.createRelationshipTo(tn, RelTypes.HAS_TABLE);
//
//			tx.success();
//		}
//	}
//
//	public static void save(JoinDef j) throws ValidationException{
//		j.validate();
//		Table left = (Table) j.getLeft();
//		Table right = (Table) j.getRight();
//
//		Node nLeft = left.getNode();
//		Node nRight = right.getNode();
//
//		GraphDatabaseService gdb = GraphService.getDb();
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			for(Relationship rel : nLeft.getRelationships(RelTypes.JOINS)){
//				if(rel.getEndNode().equals(nRight) ||
//						(rel.getStartNode().equals(nRight) && rel.getEndNode().equals(nLeft))){
//					rel.delete();
//				}
//			}
//
//			if(j.getCardinalityType() == CardinalityType.MANY_TO_ONE){
//				Relationship rel = nLeft.createRelationshipTo(nRight, RelTypes.JOINS);
//				rel.setProperty("cardinality_type", j.getCardinalityType().toString());
//				rel.setProperty("join_type", j.getJoinType().toString());
//				rel.setProperty("join_formula", j.getJoinFormula());
//				rel.setProperty("cost", j.getCost());
//				rel.setProperty("allow_rolldown",j.getAllowRollDown());
//			}else if(j.getCardinalityType() == CardinalityType.ONE_TO_MANY){
//				// We have to flip everything in this case
//				switchSides(j);
//
//				Relationship rel = nRight.createRelationshipTo(nLeft, RelTypes.JOINS);
//				rel.setProperty("cardinality_type", j.getCardinalityType().toString());
//				rel.setProperty("join_type", j.getJoinType().toString());
//				rel.setProperty("join_formula", j.getJoinFormula());
//				rel.setProperty("cost", j.getCost());
//				rel.setProperty("allow_rolldown",j.getAllowRollDown());
//			}else{
//				// Covers MANY_TO_MANY or ONE_TO_ONE
//				Relationship rel = nLeft.createRelationshipTo(nRight, RelTypes.JOINS);
//				rel.setProperty("cardinality_type", j.getCardinalityType().toString());
//				rel.setProperty("join_type", j.getJoinType().toString());
//				rel.setProperty("join_formula", j.getJoinFormula());
//				rel.setProperty("cost", j.getCost());
//				rel.setProperty("allow_rolldown",j.getAllowRollDown());
//
//				// do reverse direction too
//				switchSides(j);
//				Relationship rel2 = nRight.createRelationshipTo(nLeft, RelTypes.JOINS);
//				rel2.setProperty("cardinality_type", j.getCardinalityType().toString());
//				rel2.setProperty("join_type", j.getJoinType().toString());
//				rel2.setProperty("join_formula", j.getJoinFormula());
//				rel2.setProperty("cost", j.getCost());
//				rel2.setProperty("allow_rolldown",j.getAllowRollDown());
//			}
//
//			tx.success();
//		}
//	}
//
//	/**
//	 * Deletes a relationship between any two graphables.  This is
//	 * primarily used to remove tables linked to expressions.
//	 *
//	 * @param g1 - Graphable
//	 * @param g2 - Graphable
//	 * @return
//	 */
//	public static boolean deleteRelationship(Graphable g1, Graphable g2){
//		GraphDatabaseService gdb = GraphService.getDb();
//		Node n1 = g1.getNode();
//		Node n2 = g2.getNode();
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			ExecutionResult res;
//			// delete columns
//		    res = GraphService.getEng().execute(
//					"match (n1)-[r]-(n2) where ID(n1)=" +
//								n1.getId()+" and ID(n2)="+ n2.getId()+" delete r;");
//		    logger.debug("delete relationship between "+ g1 +" and " + g2 + ": \n" + res.dumpToString());
//
//			tx.success();
//			if(res.getQueryStatistics().getDeletedRelationships()>0){
//				return true;
//			}else{
//				return false;
//			}
//		}
//	}
//
//	/**
//	 * This method will delete all tables, columns, expressions attached
//	 * to this datasource including the datasource itself.
//	 *
//	 * @param d
//	 * @throws QEException
//	 * @throws PersistenceException
//	 */
//	public static void deleteAll(Datasource d) throws QEException, PersistenceException{
//		GraphDatabaseService gdb = GraphService.getDb();
//		Node deletable = d.getNode();
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			try{
//				ResourceIterable<Node> f = gdb.findNodesByLabelAndProperty(d.getNodeLabel(), "id", d.getId());
//				deletable = f.iterator().next();
//			}catch(NoSuchElementException er){
//				logger.error("Could not delete "+ d + " because it was not found in the graph.  ID: " +d.getId());
//				throw new PersistenceException(er);
//			}
//
//			ExecutionResult res;
//			// delete columns
//		    res = GraphService.getEng().execute(
//					"match (d:Datasource)-[r:HAS_TABLE]->(t:Table)-[x]-(c:Column) where ID(d)=" +
//								deletable.getId()+" delete x,c;");
//		    logger.debug("delete cols: \n" + res.dumpToString());
//		    // delete tables
//		    GraphService.getEng().execute(
//					"match (d:Datasource)-[r:HAS_TABLE]->(t:Table)-[x]-() "
//					+ "where ID(d)=" + deletable.getId()+" delete x,r,t;");
//		    logger.debug("delete linked tables: \n" + res.dumpToString());
//		    res = GraphService.getEng().execute(
//					"match (d:Datasource)-[r:HAS_TABLE]->(t:Table) "
//					+ "where ID(d)=" + deletable.getId()+" delete r,t;");
//		    logger.debug("delete unlinked tables: \n" + res.dumpToString());
//
//		 // delete dangling entities
//		    boolean del = true;
//		    while(del){
//			res = GraphService.getEng().execute(
//						"match ()-[r]->(e) where NOT e-->() and ( e:Measure or e:Dimension or e:Expression  ) delete r,e ;");
//			logger.debug("delete linked entities with no outgoing links: \n" + res.dumpToString());
//			ExecutionResult res2= GraphService.getEng().execute(
//						"match (e) where NOT e--() and ( e:Measure or e:Dimension or e:Expression  ) delete e ;");
//			logger.debug("delete dangling entities with no relationships: \n" + res2.dumpToString());
//			if(res.getQueryStatistics().getDeletedNodes() == 0)
//				del = false;
//		    }
//
//			deletable.delete();
//			tx.success();
//		}
//	}
//
//	public static void delete(JoinDef j){
//		Table left = (Table) j.getLeft();
//		Table right = (Table) j.getRight();
//
//		Node nLeft = left.getNode();
//		Node nRight = right.getNode();
//
//		GraphDatabaseService gdb = GraphService.getDb();
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			for(Relationship rel : nLeft.getRelationships(RelTypes.JOINS)){
//				if(rel.getEndNode().equals(nRight) ||
//						(rel.getStartNode().equals(nRight) && rel.getEndNode().equals(nLeft))){
//					rel.delete();
//				}
//			}
//
//			tx.success();
//		}
//	}
//
//	public static List<Graphable> getExpressionsUsingColumn(Column c){
//		GraphDatabaseService gdb = GraphService.getDb();
//		List<Graphable> exp = Lists.newArrayList();
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			ExecutionResult res = GraphService.getEng().execute( "match (e:Expression) where has(e.column_set) and "
//					+ "any(x in e.column_set where x in "
//					+ "[\"" + c.getId()+ "\"]) return e.id as eid;" );
//
//			for(Map<String, Object> row : res){
//				Expression e;
//				try {
//					e = Expression.find((String)row.get("eid"));
//					exp.add(e);
//				} catch (PersistenceException e1) {
//					logger.error("Unabled to find exp with id:  "+(String)row.get("id"));
//					continue;
//				}
//			}
//		}
//		return exp;
//	}
//
//	public static List<Graphable> delete(Table t) throws PersistenceException{
//		GraphDatabaseService gdb = GraphService.getDb();
//		List<Graphable> danglers = Lists.newArrayList();
//		Node deletable ;
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			try{
//				ResourceIterable<Node> f = gdb.findNodesByLabelAndProperty(t.getNodeLabel(), "id", t.getId());
//				deletable = f.iterator().next();
//			}catch(NoSuchElementException er){
//				logger.error("Could not delete table " + t.getName() + " because it was not found in the graph");
//				throw new PersistenceException(er);
//			}
//
//			for(Relationship rel : deletable.getRelationships()){
//				if(rel.isType(RelTypes.QUERIES_FROM)){
//					Node start = rel.getStartNode();
//					danglers.add(Expression.find((String) start.getProperty("id")));
//				}else if(rel.isType(RelTypes.COLUMN_OF)){
//					rel.getStartNode().delete();
//				}
//				rel.delete();
//			}
//			deletable.delete();
//			tx.success();
//		}
//
//		return danglers;
//	}
//
//	public static List<Graphable> delete(Column c) throws PersistenceException{
//		GraphDatabaseService gdb = GraphService.getDb();
//		List<Graphable> danglers = getExpressionsUsingColumn(c);
//		Node deletable = c.getNode();
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			try{
//				ResourceIterable<Node> f = gdb.findNodesByLabelAndProperty(c.getNodeLabel(), "id", c.getId());
//				deletable = f.iterator().next();
//			}catch(NoSuchElementException e){
//				logger.error("Could not delete column " + c.getName() + " because it was not found in the graph");
//				throw new PersistenceException(e);
//			}
//
//			if(deletable == null)
//				return danglers;
//
//			for(Relationship rel : deletable.getRelationships()){
//				rel.delete();
//			}
//
//			deletable.delete();
//			tx.success();
//		}
//
//		return danglers;
//	}
//
//	public static List<Graphable> delete(Measure m) throws PersistenceException{
//		GraphDatabaseService gdb = GraphService.getDb();
//		List<Graphable> danglers = Lists.newArrayList();
//		Node deletable = m.getNode();
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			try{
//				ResourceIterable<Node> f = gdb.findNodesByLabelAndProperty(m.getNodeLabel(), "id", m.getId());
//				deletable = f.iterator().next();
//			}catch(NoSuchElementException er){
//				logger.error("Could not delete measure " + m.getName() + " because it was not found in the graph");
//				throw new PersistenceException(er);
//			}
//
//			for(Relationship rel : deletable.getRelationships()){
//				if(rel.getStartNode().equals(deletable)){
//					Node exp = rel.getEndNode();
//					for(Relationship erel : exp.getRelationships()){
//						erel.delete();
//					}
//					exp.delete();
//				}else{
//					rel.delete();
//					Node start = rel.getStartNode();
//					danglers.add(Expression.find((String) start.getProperty("id")));
//				}
//			}
//			deletable.delete();
//			tx.success();
//		}
//
//		return danglers;
//	}
//
//	public static List<Graphable> delete(Dimension d) throws PersistenceException{
//		GraphDatabaseService gdb = GraphService.getDb();
//		List<Graphable> danglers = Lists.newArrayList();
//		Node deletable = d.getNode();
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			try{
//				ResourceIterable<Node> f = gdb.findNodesByLabelAndProperty(d.getNodeLabel(), "id", d.getId());
//				deletable = f.iterator().next();
//			}catch(NoSuchElementException e){
//				logger.error("Could not delete dimension " + d.getName() + " because it was not found in the graph");
//				throw new PersistenceException(e);
//			}
//
//			for(Relationship rel : deletable.getRelationships(Direction.OUTGOING)){
//				Node exp = rel.getEndNode();
//				for(Relationship erel : exp.getRelationships()){
//					erel.delete();
//				}
//				// delete exp here since we do jpa delete first
//				exp.delete();;
//			}
//			deletable.delete();
//			tx.success();
//		}
//
//		return danglers;
//	}
//
//	public static List<Graphable> delete(Expression e) throws PersistenceException{
//		GraphDatabaseService gdb = GraphService.getDb();
//		List<Graphable> danglers = Lists.newArrayList();
//		Node deletable;
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			try{
//				ResourceIterable<Node> f = gdb.findNodesByLabelAndProperty(e.getNodeLabel(), "id", e.getId());
//				deletable = f.iterator().next();
//			}catch(NoSuchElementException er){
//				logger.error("Could not delete expression " + e.getFormula() + " because it was not found in the graph");
//				throw new PersistenceException(er);
//			}
//
//			for(Relationship rel : deletable.getRelationships()){
//				if(!rel.getStartNode().equals(deletable)){
//					Node start = rel.getStartNode();
//					if(Iterables.size(start.getRelationships(Direction.OUTGOING))==1){
//						if(start.hasLabel(NodeLabels.DIMENSION)){
//							danglers.add(Dimension.find((String) start.getProperty("id")));
//						}else{
//							danglers.add(Measure.find((String) start.getProperty("id")));
//						}
//					}
//				}
//				rel.delete();
//			}
//			deletable.delete();
//			tx.success();
//		}
//
//		return danglers;
//	}
//
//	public static List<Graphable> delete(Datasource d) throws PersistenceException{
//		GraphDatabaseService gdb = GraphService.getDb();
//		Node deletable ;
//		List<Graphable> l = Lists.newArrayList();
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			try{
//				ResourceIterable<Node> f = gdb.findNodesByLabelAndProperty(d.getNodeLabel(), "id", d.getId());
//				deletable = f.iterator().next();
//			}catch(NoSuchElementException er){
//				logger.error("Could not delete "+ d + " because it was not found in the graph.  ID: " +d.getId());
//				throw new PersistenceException(er);
//			}
//
//			if(Iterables.size(deletable.getRelationships(Direction.OUTGOING))>0){
//				throw new PersistenceException("Many tables refer to this datasource.  Try deleteAll.");
//			}
//
//			for(Relationship rel : deletable.getRelationships()){
//				rel.delete();
//			}
//
//			deletable.delete();
//			tx.success();
//		}
//
//		return l;
//	}
//
//	public static List<Graphable> delete(Graphable g) throws PersistenceException{
//		if(g instanceof Column){
//			return delete((Column)g);
//		}else if(g instanceof Table){
//			return delete((Table)g);
//		}else if(g instanceof Measure){
//			return delete((Measure)g);
//		}else if(g instanceof Dimension){
//			return delete((Dimension)g);
//		}else if(g instanceof Expression){
//			return delete((Expression)g);
//		}else if(g instanceof Datasource){
//			return delete((Datasource)g);
//		}else{
//			throw new PersistenceException("Unknown instance type.");
//		}
//	}
//
//	public static boolean hasOutgoingRelBetween(Node start, Node end) {
//		boolean hasRel = false;
//		if(start == null)
//			return hasRel;
//		GraphDatabaseService gdb = GraphService.getDb();
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			for(Relationship rel : start.getRelationships(Direction.OUTGOING)){
//				if(rel.getEndNode().equals(end))
//					hasRel = true;
//			}
//		}
//		return hasRel;
//	}
//
//	public static boolean hasAnyRelBetween(Node start, Node end) {
//		boolean hasRel = false;
//		if(start == null)
//			return hasRel;
//
//		GraphDatabaseService gdb = GraphService.getDb();
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			for(Relationship rel : start.getRelationships()){
//				Node ended = rel.getEndNode();
//				if(ended.equals(end))
//					hasRel = true;
//			}
//		}
//		return hasRel;
//	}
//
//	/**
//	 * Returns a chain of tables that have outgoing relationship from the
//	 * input t and relationship type Joins.  Typically outgoing relationships
//	 * identify m to 1 which is likely a hierarchy.
//	 *
//	 * @param t - Alias Target Table
//	 * @return
//	 * @throws QEException - If a path with different start table is received.
//	 */
//	public static VeroAliasPath getAliasPath(Table t) throws QEException{
//		VeroAliasPath vap = new VeroAliasPath(t);
//		GraphDatabaseService gdb = GraphService.getDb();
//		logger.debug("Generating alias paths.");
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			TraversalDescription td = gdb.traversalDescription()
//				.relationships(RelTypes.JOINS, Direction.OUTGOING)
//				.evaluator(PathEvaluators.END_NODE_IS_TABLE);
//
//			for(Path p : td.traverse(t.getNode()))
//			{
//				vap.addPath(p);
//				logger.debug(p.toString());
//			}
//		}
//
//		return vap;
//	}
//
//	public static Label getLabel(Node node) throws QEException {
//		Label l;
//		GraphDatabaseService gdb = GraphService.getDb();
//		try ( Transaction tx = gdb.beginTx() )
//		{
//			l = NodeLabels.get(Iterables.get(node.getLabels(),0));
//
//		}
//		return l;
//	}
//
//	public static String pathsToString(List<Path> paths) {
//		String out = "";
//		try ( Transaction tx = GraphService.getDb().beginTx() )
//		{
//			for(Path p : paths){
//				out += p.toString() +"\n";
//			}
//		}
//		return out;
//
//	}
//
//	public static void writeDataModelJson(JsonGenerator g, Datasource ds) throws JsonGenerationException, IOException {
//
//		GraphDatabaseService gdb = GraphService.getDb();
//		Transaction tx = gdb.beginTx();
//		try
//		{
//			TraversalDescription td = gdb.traversalDescription()
//				 .relationships(RelTypes.HAS_TABLE, Direction.OUTGOING)
//				 .relationships(RelTypes.JOINS);
//
//			Node dsn = ds.getNode();
//			List<Node> nodes = Lists.newArrayList();
//			List<Relationship> rels = Lists.newArrayList();
//			List<Path> paths = Lists.newArrayList();
//			for(Path p : td.traverse(dsn)){
//				paths.add(p);
//			}
//
//			// start nodes
//			g.writeArrayFieldStart("nodes");
//			for(Path p : paths){
//				for(Node n : p.nodes()){
//					if(nodes.contains(n) || n.equals(dsn))
//						continue;
//
//					nodes.add(n);
//
//					g.writeStartObject();
//					g.writeStringField("label", Iterables.get(n.getLabels(),0).toString());
//					g.writeNumberField("nodeId", n.getId());
//
//					n.getPropertyKeys().forEach((key) ->{
//						try {
//							Object v = n.getProperty(key);
//							if(v instanceof String)
//								g.writeStringField(key, (String)v);
//							if(v instanceof Number)
//								g.writeObjectField(key, v);
//						} catch (Exception e) {
//							logger.error("Runtime exception while writing properties to json from node.");
//							throw new RuntimeException(e);
//						}
//					});
//					g.writeEndObject();
//				}
//			}
//			// end nodes
//			g.writeEndArray();
//
//			// start links
//			g.writeArrayFieldStart("links");
//			for(Path p : paths){
//				for(Relationship r : p.relationships()){
//					if(rels.contains(r) || r.getStartNode().equals(dsn))
//						continue;
//
//					rels.add(r);
//					g.writeStartObject();
//					g.writeStringField("type", r.getType().name());
//
//					r.getPropertyKeys().forEach((key) ->{
//						try {
//							Object v = r.getProperty(key);
//							if(v instanceof String)
//								g.writeStringField(key, (String)v);
//							if(v instanceof Number)
//								g.writeObjectField(key, v);
//						} catch (Exception e) {
//							logger.error("Runtime exception while writing properties to json from relationship.");
//							throw new RuntimeException(e);
//						}
//					});
//
//					g.writeNumberField("source", nodes.indexOf(r.getStartNode()));
//					g.writeNumberField("target",nodes.indexOf(r.getEndNode()));
//
//					g.writeNumberField("sourceId",r.getStartNode().getId());
//					g.writeNumberField("targetId",r.getEndNode().getId());
//					g.writeEndObject();
//				}
//			}
//			// end links
//			g.writeEndArray();
//		}finally{
//			tx.close();
//		}
//
//	}
//
//	private static void writePathNodesToJSON(JsonGenerator g, OptimizedPlan plan, List<Node> nodes, Set<Path> paths, String groupName) throws JsonGenerationException, IOException{
//		QueryBlock qb = plan.getSourceBlock();
//
//		GraphDatabaseService gdb = GraphService.getDb();
//		Transaction tx = gdb.beginTx();
//		try
//		{
//			for(Path p : paths){
//				for(Node n : p.nodes()){
//					if(nodes.contains(n))
//						continue;
//
//					nodes.add(n);
//					g.writeStartObject();
//					g.writeStringField("label", Iterables.get(n.getLabels(),0).toString());
//					g.writeStringField("nodeGroup",groupName);
//					g.writeNumberField("nodeId", n.getId());
//
//					n.getPropertyKeys().forEach((key) ->{
//						try {
//							Object v = n.getProperty(key);
//							if(v instanceof String)
//								g.writeStringField(key, (String)v);
//							if(v instanceof Number)
//								g.writeObjectField(key, v);
//						} catch (Exception e) {
//							logger.error("Runtime exception while writing properties to json from node.");
//							throw new RuntimeException(e);
//						}
//					});
//
//					// set if the table is hint or not
//					if(n.hasLabel(NodeLabels.TABLE)){
//						boolean isHint = false;
//						for(Table t : qb.getTableHints()){
//							if(t.getNode().equals(n)) isHint=true;
//						}
//						g.writeBooleanField("isHint", isHint);
//					}
//
//					// set cross join or suppressed indicator
//					if(n.hasLabel(NodeLabels.DIMENSION)){
//						DisjointType dt = DisjointType.NONE;
//						for(DisjointPlan dp : plan.getDisjointedPlans()){
//							if(dp.getDisjointType(n) != null){
//								dt = dp.getDisjointType(n);
//								break;
//							}
//						}
//						g.writeStringField("disjointType", dt.toString());
//					}
//
//
//					// end a single node
//					g.writeEndObject();
//				}
//			}//outer for loop
//		}finally{
//			tx.close();
//		}
//	}
//
//	/**
//	 * Takes a join formula and switches tleft to tright
//	 * and tright to tleft.
//	 *
//	 * @param formula
//	 * @return
//	 */
//	public static void switchSides(JoinDef j) {
//		String f = j.getJoinFormula();
//		f = f.replaceAll("tleft", "x--r");
//		f = f.replaceAll("tright", "tleft");
//		f = f.replaceAll("x--r", "tright");
//		j.setJoinFormula(f);
//
//		switch (j.getCardinalityType()) {
//		case MANY_TO_ONE:
//			j.setCardinalityType(CardinalityType.ONE_TO_MANY);
//			break;
//		case ONE_TO_MANY:
//			j.setCardinalityType(CardinalityType.MANY_TO_ONE);
//			break;
//		default:
//			break;
//		}
//
//		switch (j.getJoinType()){
//		case LEFT_OUTER_JOIN:
//			j.setJoinType(JoinType.RIGHT_OUTER_JOIN);
//			break;
//		case RIGHT_OUTER_JOIN:
//			j.setJoinType(JoinType.LEFT_OUTER_JOIN);
//			break;
//		default:
//			break;
//		}
//	}
}