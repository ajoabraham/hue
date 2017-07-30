package com.hue.planner;

public class PathEvaluators {


	/**
	 * Prunes paths where a traversal is being made across a dimension.
	 * For example a path like this would normally be illegal:
	 * 	(table)<--(expression)<--(Dimension)-->(expression2)-->(table2)
	 *
	 * Notice the walk up to a dimension and the walk down to table2. We
	 * wish to prune this to stop at the Dimension.
	 */
//	public static final Evaluator HAS_EXPRESSION_EVAL = new Evaluator()
//    {
//        @Override
//        public Evaluation evaluate( final Path path )
//        {
//            if ( path.length() == 0 )
//            {
//                return Evaluation.EXCLUDE_AND_CONTINUE;
//            }
//
//            int numRels=0;
//            for(Relationship r : path.reverseRelationships())
//            {
//		if(r.isType(RelTypes.HAS_EXPRESSION)){
//			numRels ++;
//		}
//		if(numRels >1){
//			break;
//		}
//            }
//            if(numRels <2)
//            {
//		return Evaluation.INCLUDE_AND_CONTINUE;
//            }else {
//		return Evaluation.INCLUDE_AND_PRUNE;
//            }
//        }
//    };
//
//	public static final Evaluator END_NODE_IS_TABLE = new Evaluator()
//    {
//        @Override
//        public Evaluation evaluate( final Path path )
//        {
//            if ( path.length() == 0 )
//            {
//                return Evaluation.EXCLUDE_AND_CONTINUE;
//            }
//
//            if(path.endNode().hasLabel(NodeLabels.TABLE)){
//		return Evaluation.INCLUDE_AND_CONTINUE;
//            }else{
//		return Evaluation.EXCLUDE_AND_CONTINUE;
//            }
//
//        }
//    };
//
//	public static final Evaluator END_NODE_IS_EXPRESSIBLE = new Evaluator()
//    {
//        @Override
//        public Evaluation evaluate( final Path path )
//        {
//            if ( path.length() == 0 )
//            {
//                return Evaluation.EXCLUDE_AND_CONTINUE;
//            }
//
//            if(	  ( path.endNode().hasLabel(NodeLabels.DIMENSION) ||
//			path.endNode().hasLabel(NodeLabels.MEASURE) )
//			&& path.endNode().hasRelationship(RelTypes.HAS_EXPRESSION, Direction.OUTGOING)){
//		return Evaluation.INCLUDE_AND_CONTINUE;
//            }else{
//		return Evaluation.EXCLUDE_AND_CONTINUE;
//            }
//
//        }
//    };
//
//    public static final Evaluator END_NODE_IS_EXP_OR_VIRTUAL_EXP = new Evaluator()
//    {
//        @Override
//        public Evaluation evaluate( final Path path )
//        {
//            if ( path.length() == 0 )
//            {
//                return Evaluation.EXCLUDE_AND_CONTINUE;
//            }
//
//            if(	  ( path.endNode().hasLabel(NodeLabels.DIMENSION) ||
//			path.endNode().hasLabel(NodeLabels.MEASURE) )
//			){
//		return Evaluation.INCLUDE_AND_CONTINUE;
//            }else{
//		return Evaluation.EXCLUDE_AND_CONTINUE;
//            }
//
//        }
//    };
//
//    public static final Evaluator ENDS_WITH_TABLE_EVAL = new Evaluator()
//    {
//        @Override
//        public Evaluation evaluate( final Path path )
//        {
//            if ( path.length() == 0 )
//            {
//                return Evaluation.EXCLUDE_AND_CONTINUE;
//            }else{
//		Node nd = path.endNode();
//		if(nd.hasLabel(NodeLabels.TABLE)){
//			return Evaluation.INCLUDE_AND_CONTINUE;
//		}else{
//			return Evaluation.EXCLUDE_AND_CONTINUE;
//		}
//            }
//        }
//    };
//
//    public static final Evaluator ENDS_WITH_DIM_TABLE_EVAL = new Evaluator()
//    {
//        @Override
//        public Evaluation evaluate( final Path path )
//        {
//            if ( path.length() == 0 )
//            {
//                return Evaluation.EXCLUDE_AND_CONTINUE;
//            }else{
//		Node nd = path.endNode();
//		if(nd.hasLabel(NodeLabels.TABLE) &&
//				(Integer)nd.getProperty("table_type")==1){
//			return Evaluation.INCLUDE_AND_CONTINUE;
//		}else{
//			return Evaluation.EXCLUDE_AND_CONTINUE;
//		}
//            }
//        }
//    };
//
//    public static final Evaluator ENDS_WITH_NON_DIM_TABLE_EVAL = new Evaluator()
//    {
//        @Override
//        public Evaluation evaluate( final Path path )
//        {
//            if ( path.length() == 0 )
//            {
//                return Evaluation.EXCLUDE_AND_CONTINUE;
//            }else{
//		Node nd = path.endNode();
//		if(nd.hasLabel(NodeLabels.TABLE) &&
//				(Integer)nd.getProperty("table_type")!=1){
//			return Evaluation.INCLUDE_AND_CONTINUE;
//		}else{
//			return Evaluation.EXCLUDE_AND_CONTINUE;
//		}
//            }
//        }
//    };
//
//    public static final Evaluator ALLOW_ROLLDOWNS = new Evaluator()
//    {
//        @Override
//        public Evaluation evaluate( final Path path )
//        {
//            if ( path.length() == 0 )
//            {
//                return Evaluation.EXCLUDE_AND_CONTINUE;
//            }else{
//		for(Relationship r : path.relationships()){
//			int rollDownEnabled = -1;
//			if(r.isType(RelTypes.JOINS)){
//				// default to initially allow rolldown
//				rollDownEnabled = (int) r.getProperty("allow_rolldown", -1);
//			}
//
//			if(rollDownEnabled==0){
//				return Evaluation.EXCLUDE_AND_CONTINUE;
//			}
//		}
//		return Evaluation.INCLUDE_AND_CONTINUE;
//            }
//        }
//    };
//
//	public static Evaluator endsWith(Node neoNode) {
//		Evaluator pe = new Evaluator()
//	    {
//	        @Override
//	        public Evaluation evaluate( final Path path )
//	        {
//	            if ( path.length() == 0 )
//	            {
//	                return Evaluation.EXCLUDE_AND_CONTINUE;
//	            }else{
//			Node nd = path.endNode();
//			if(nd.equals(neoNode)){
//				return Evaluation.INCLUDE_AND_CONTINUE;
//			}else{
//				return Evaluation.EXCLUDE_AND_CONTINUE;
//			}
//	            }
//	        }
//	    };
//		return pe;
//	}
//
//	public static Evaluator endsWithAny(List<Node> targetTables) {
//		Evaluator pe = new Evaluator()
//	    {
//	        @Override
//	        public Evaluation evaluate( final Path path )
//	        {
//	            if ( path.length() == 0 )
//	            {
//	                return Evaluation.EXCLUDE_AND_CONTINUE;
//	            }else{
//			Node nd = path.endNode();
//			if(targetTables.contains(nd)){
//				return Evaluation.INCLUDE_AND_CONTINUE;
//			}else{
//				return Evaluation.EXCLUDE_AND_CONTINUE;
//			}
//	            }
//	        }
//	    };
//		return pe;
//	}



}
