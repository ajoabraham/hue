package com.hue.planner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.OptionalDouble;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.vero.common.constant.CardinalityType;
import com.vero.common.constant.JoinType;
import com.vero.server.common.FuzzyMatch;
import com.vero.model.Column;
import com.vero.model.Table;
import com.vero.model.graph.GraphException;

public class JoinDetector {
	private static final Logger logger = LoggerFactory.getLogger(JoinDetector.class.getName());
	private final String KEY_DIV = "-lr-";

	private ResultSet left;
	private ResultSet right;
	private JoinDef jd;
	private HashMap<String,Integer> ratioMap = Maps.newHashMap();
	private Connection conn;

	public JoinDetector(Table tableLeft, ResultSet left, Table tableRight, ResultSet right, Connection conn) throws SQLException{
		this.left = left;
		this.right = right;
		this.conn = conn;
		this.jd = new JoinDef();
		jd.setLeft(tableLeft);
		jd.setRight(tableRight);
		jd.setJoinType(JoinType.INNER_JOIN);
	}

	public ResultSet getLeft() {
		return left;
	}

	public ResultSet getRight() {
		return right;
	}

	public JoinDef getJoin() throws GraphException{
		if(jd.getJoinFormula() != null){
			return jd;
		}
		boolean res = getJoinByColumnName();
		if(!res){
			res = getJoinByPrimaryKeyQuery();
		}
		if(!res){
			List<Entry<String, Integer>> ratios = sortMapByRatios();
			if(ratios.get(0).getValue() >= 50){
				String[] keys = ratios.get(0).getKey().split(KEY_DIV);
				ArrayList<String> l = Lists.newArrayList(keys[0]);
				ArrayList<String> r = Lists.newArrayList(keys[1]);
				setFormula(l,r);
				try {
					setCardinality(l,r);
				} catch (SQLException e) {
					logger.warn(e.getMessage());;
				}
			}else{
				res = getJoinByData();
			}
		}
		return jd;
	}

	private boolean getJoinByPrimaryKeyQuery() throws GraphException{
		logger.debug("Searching for a join by primary key queries..");
		List<Entry<String, Integer>> ratios = sortMapByRatios();

		Table tl = (Table)jd.getLeft();
		Table tr = (Table)jd.getRight();
		List<Column> plc = tl.getPrimaryKeys();

		ArrayList<String> l = Lists.newArrayList();
		ArrayList<String> r = Lists.newArrayList();
		plc.stream().forEach(c -> {
			try {
				if(l.size()>0) return;

				left.beforeFirst();
				List<String> v = Lists.newArrayList();
				int type = 12;
				while(left.next()){
					v.add(left.getString(c.getName()));
					type = left.getMetaData().getColumnType(left.findColumn(c.getName()));
					if(v.size()>10) break;
				}
				for(int i=0;i<ratios.size();i++){
					String[] keys = ratios.get(i).getKey().split(KEY_DIV);
					if(!keys[0].equalsIgnoreCase(c.getName())){
						continue;
					}

					String query = "select count(*) from " + quoteIdent(tr.getSchemaName()) + "." + quoteIdent(tr.getPhysicalName())
							+ " where " + quoteIdent(keys[1]) + " in "
							+ literalize(v, type) + ";";
					Statement st = conn.createStatement();
					ResultSet res;
					try{
						res = st.executeQuery(query);
						res.next();
					}catch(Exception e){
						logger.debug(e.getMessage());
						continue;
					}
					int count = res.getInt(1);
					if(count>0){
						l.add(c.getName());
						r.add(keys[1]);
						break;
					}
				}
			} catch (Exception e) {
				logger.warn(e.getMessage());
			}
		});

		List<Column> prc = tr.getPrimaryKeys();
			prc.stream().forEach(c -> {
				try {
					if(l.size()>0) return;

					right.beforeFirst();
					List<String> v = Lists.newArrayList();
					int type = 12;
					while(right.next()){
						v.add(right.getString(c.getName()));
						type = right.getMetaData().getColumnType(right.findColumn(c.getName()));
						if(v.size()>10) break;
					}
					for(int i=0;i<ratios.size();i++){
						String[] keys = ratios.get(i).getKey().split(KEY_DIV);
						if(!keys[1].equalsIgnoreCase(c.getName())){
							continue;
						}
						String query = "select count(*) from " + quoteIdent(tl.getSchemaName()) + "." + quoteIdent(tl.getPhysicalName())
								+ " where " + quoteIdent(keys[0]) + " in "
								+ literalize(v, type) + ";";
						Statement st = conn.createStatement();
						ResultSet res;
						try{
							res = st.executeQuery(query);
							res.next();
						}catch(Exception e){
							continue;
						}
						int count = res.getInt(1);
						if(count>0){
							r.add(c.getName());
							l.add(keys[0]);
							break;
						}
					}
				} catch (Exception e) {
					logger.warn(e.getMessage());
				}
			});

		if(l.size()>0){

			setFormula(l,r);
			try {
				setCardinality(l,r);
			} catch (SQLException e) {
				logger.warn(e.getMessage());;
			}

			return true;
		}
		return false;
	}

	private String literalize(List<String> v, int type){
		StringJoiner sj = new StringJoiner(",", "(",")");
		if(type == Types.NUMERIC || type == Types.BIGINT || type == Types.BIT || type == Types.DECIMAL
				|| type == Types.DOUBLE || type == Types.FLOAT || type == Types.INTEGER || type == Types.REAL || type == Types.ROWID
				|| type == Types.SMALLINT || type == Types.TINYINT){


			v.forEach(s -> sj.add(s) );
		}else{
			v.forEach(s -> sj.add("'" + s + "'") );
		}
		return sj.toString();
	}

	private String quoteIdent(String ident) throws SQLException {
		String q = conn.getMetaData().getIdentifierQuoteString();
		return q+ident+q;
	}

	private boolean getJoinByData() {
		logger.debug("Searching for a join by matching data..");
		try {
			ArrayList<String> l = Lists.newArrayList();
			ArrayList<String> r = Lists.newArrayList();
			for(int i =1; i<=left.getMetaData().getColumnCount(); i++){
				String lname = left.getMetaData().getColumnName(i);
				List<String> lvalues = Lists.newArrayList();
				left.beforeFirst();
				while(left.next()){
					lvalues.add(left.getString(i));
				}

				for(int j =1; j<=right.getMetaData().getColumnCount(); j++){
					String rname = right.getMetaData().getColumnName(j);
					List<String> rvalues = Lists.newArrayList();
					right.beforeFirst();
					while(right.next()){
						rvalues.add(right.getString(j));
					}
					List<String> rinter = Lists.newArrayList(rvalues);
					boolean thereAreSomeNoMatches = rinter.retainAll(lvalues);
					if(thereAreSomeNoMatches){
						double x = rinter.size()/(rvalues.size()*1.000);
						if(x > 0.1){
							l.add(lname);
							r.add(rname);
							break;
						}
					}else{
						l.add(lname);
						r.add(rname);
						break;
					}
				}
				if(l.size()>0) break;
			}
			if(l.size()>0){
				setCardinality(l,r);
				setFormula(l,r);
				return true;
			}else{
				return false;
			}

		} catch (SQLException e) {
			logger.error("Somethign went wrong while searching for a join"
					+ " between " + jd.getLeft() + " and " + jd.getRight() +"\n"+e.getMessage());
			return false;
		}

	}

	private boolean getJoinByColumnName(){
		logger.debug("Searching for a join by matching column names..");
		try {
			ArrayList<String> lnames = Lists.newArrayList();
			ArrayList<String> rnames = Lists.newArrayList();
			for(int i =1; i<=left.getMetaData().getColumnCount(); i++){
				String lname = left.getMetaData().getColumnName(i);
				for(int j =1; j<=right.getMetaData().getColumnCount(); j++){
					String rname = right.getMetaData().getColumnName(j);
					int ratio = FuzzyMatch.getRatio(lname, rname);
					ratioMap.put(lname+KEY_DIV+rname, ratio);
					if(ratio > 80){
						lnames.add(lname);
						rnames.add(rname);
					}
				}
			}
			if(lnames.size()>0){
				setCardinality(lnames,rnames);
				setFormula(lnames,rnames);
				return true;
			}
		} catch (SQLException e) {
			logger.error("Somethign went wrong while searching for a join"
					+ " between " + jd.getLeft() + " and " + jd.getRight() +"\n"+e.getMessage());
			return false;
		}
		return false;
	}

	private void setFormula(ArrayList<String> lnames, ArrayList<String> rnames) {
		String j = "";
		for(int i = 0; i<lnames.size() ; i++){
			j += "tleft." +lnames.get(i) + " = tright." + rnames.get(i);
			if(i<lnames.size()-1){
				j += " AND ";
			}
		}
		jd.setJoinFormula(j);
	}

	private void setCardinality(ArrayList<String> lnames, ArrayList<String> rnames) throws SQLException {
		HashMap<String,Integer> lv = Maps.newHashMap();
		HashMap<String,Integer> rv = Maps.newHashMap();

		left.beforeFirst();
		int lcount =0;
		right.beforeFirst();
		int rcount=0;
		
		while(left.next()){
			String lkey="";
			for(int i =0; i< lnames.size(); i++){
				lkey += left.getString(lnames.get(i));
				if(i<lnames.size()-1){
					lkey += "-";
				}
			}
			Integer v = lv.get(lkey);
			if(v==null){
				lv.put(lkey, 1);
			}else{
				lv.put(lkey,v+1);
			}
			lcount++;
		}

		while(right.next()){
			String rkey="";
			for(int i =0; i< rnames.size(); i++){
				rkey += right.getString(rnames.get(i));
				if(i<rnames.size()-1){
					rkey += "-";
				}
			}
			Integer v = rv.get(rkey);
			if(v==null){
				rv.put(rkey, 1);
			}else{
				rv.put(rkey,v+1);
			}
			rcount++;
		}
		if(lcount==0 || rcount==0){
			jd.setCardinalityType(CardinalityType.ONE_TO_MANY);
			if(rcount==0){
			}
		}else{
			OptionalDouble ra = rv.values().stream().mapToInt(a -> a).average();
			OptionalDouble la = lv.values().stream().mapToInt(a -> a).average();
			double lperc = la.getAsDouble();
			double rperc = ra.getAsDouble();

			if(lperc != 1 && rperc != 1){
				jd.setCardinalityType(CardinalityType.MANY_TO_MANY);
			}else if(lperc==1 && rperc==1){
				jd.setCardinalityType(CardinalityType.ONE_TO_ONE);
			}else if(lperc==1 && rperc!=1){
				jd.setCardinalityType(CardinalityType.ONE_TO_MANY);
			}else if(lperc!=1 && rperc==1){
				jd.setCardinalityType(CardinalityType.MANY_TO_ONE);
			}
		}		
	}

	private List<Entry<String, Integer>> sortMapByRatios(){
		Comparator<Entry<String, Integer>> byValue = (entry1, entry2) -> entry1.getValue().compareTo(
	            entry2.getValue());

	    List<Entry<String, Integer>> l = ratioMap
	            .entrySet()
	            .stream()
	            .sorted(byValue.reversed())
	            .collect(Collectors.toList());
	    logger.debug("Sorted ratioMap: /n" + l);

	    return l;
	}

}
