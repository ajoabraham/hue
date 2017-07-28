package com.hue.model;

import java.io.File;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.hue.common.CardinalityType;
import com.hue.common.JoinType;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Join{

	private IJoinable left;
	private IJoinable right;

	private int cost = -1;
    private String sql;
	private CardinalityType cardinalityType;
	private JoinType joinType = JoinType.INNER_JOIN;
	private int allowRollDown = -1;
	private String leftTableName = null;
	private String rightTableName = null;
	
	@JsonIgnore
	private Datasource ds;
	@JsonIgnore
	private File file;
	@JsonIgnore
	public Vertex v;

	public Join(IJoinable left, IJoinable right, String sql, JoinType type) {
		this.left = left;
		this.right = right;
		setSql(sql);
		setJoinType(type);
		this.leftTableName = left == null ? null : String.join(".", getLeft().getPhysicalNameSegments());
		this.rightTableName = right == null ? null : String.join(".", getRight().getPhysicalNameSegments());
	}

	public Join() {
		this(null, null, null, null);
	}

    @JsonIgnore
    public String getName() {
        return String.format("%s_%s_%s", String.join(".", getLeft().getPhysicalNameSegments()), joinType.name(), 
        		String.join(".", getRight().getPhysicalNameSegments()));
    }
    
	public File getFile() {
		return file;
	}
	
	public void setFile(File file) {
		this.file = file;
	}
    
    public Datasource getDatasource() {
    		return ds;
    }
    
    public void setDatasource(Datasource ds) {
    		this.ds = ds;
    }

	public IJoinable getLeft() {
		return left;
	}

	public void setLeft(IJoinable left) {
		this.left = left;
		setLeftTableName(left == null ? null : left.getName());
	}

	public IJoinable getRight() {
		return right;
	}

	public void setRight(IJoinable right) {
		this.right = right;

		setRightTableName(right == null ? null : right.getName());
	}

	public int getCost() {
		if (cost <= 0) {
			if (getLeft() instanceof Table) {
				cost = ((Table) getLeft()).getRowCount();
			}

			if (getRight() instanceof Table) {
				int right = (((Table) getRight()).getRowCount());
				
				if(getCardinalityType() == CardinalityType.MANY_TO_MANY){
					cost += right;
				}else{
					cost = Math.max(cost, right);
				}				
			}
		}
		return cost;
	}

	public void setCost(int cost) {
		this.cost = cost;
	}

	public CardinalityType getCardinalityType() {
		return cardinalityType;
	}

	public void setCardinalityType(CardinalityType cardinalityType) {
		this.cardinalityType = cardinalityType;
	}

	public JoinType getJoinType() {
		return joinType;
	}

	public void setJoinType(JoinType joinType) {
		if(joinType != null)
			this.joinType = joinType;
	}

	public int getAllowRollDown() {
		return allowRollDown;
	}

	public void setAllowRollDown(boolean allow){
		if(allow){
			allowRollDown = 1;
		}else{
			allowRollDown = 0;
		}
	}

	public String getLeftTableName() {
		return leftTableName;
	}

	public void setLeftTableName(String leftTableName) {
		this.leftTableName = leftTableName;
	}

	public String getRightTableName() {
		return rightTableName;
	}

	public void setRightTableName(String rightTableName) {
		this.rightTableName = rightTableName;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		if (sql != null)
				this.sql = sql.trim();
	}

	public boolean samePair(Join j) {
		if(j.getLeft() == null || j.getRight() == null)
			return false;

		boolean isSamePair = false;
		int numMatches = 0;

		if (j.getLeft().equals(getLeft()) || j.getLeft().equals(getRight()))
			numMatches += 1;

		if (j.getRight().equals(getRight()) || j.getRight().equals(getLeft()))
			numMatches += 1;

		if (numMatches == 2)
			isSamePair = true;

		return isSamePair;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((left == null) ? 0 : left.hashCode());
		result = prime * result + ((right == null) ? 0 : right.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Join other = (Join) obj;
		
		if (left == null) {
			return false;
		}
		
		if (right == null) {
			return false;
		} 
		
		if(!samePair(other)) {
			return false;
		}
		return true;
	}
	
	@Override
	public String toString() {
		return getName();
	}
}
