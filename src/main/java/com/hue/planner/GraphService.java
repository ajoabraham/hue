package com.hue.planner;

public class GraphService {
//    private static String DB_PATH = "graph.db";
//    private static GraphDatabaseService graphDb = null;
//    private static ExecutionEngine engine = null;
//
//    static {
//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            @Override
//            public void run() {
//                if (graphDb != null) {
//                    graphDb.shutdown();
//                }
//            }
//        });
//    }
//
//    public synchronized static GraphDatabaseService getDb() {
//        if (graphDb == null) {
//            graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(DB_PATH).newGraphDatabase();
//        }
//        return graphDb;
//    }
//
//    public synchronized static ExecutionEngine getEng() {
//        if (engine == null) {
//            engine = new ExecutionEngine(getDb(), StringLogger.SYSTEM);
//        }
//        return engine;
//    }
//
//    public static synchronized void shutdown() {
//        if (graphDb != null) {
//            graphDb.shutdown();
//            graphDb = null;
//        }
//    }
//
//    public synchronized static void switchDb(String path) {
//        shutdown();
//        DB_PATH = path;
//    }
}