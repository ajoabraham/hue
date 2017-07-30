package com.hue.planner;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.vero.common.constant.CardinalityType;
import com.vero.common.constant.JoinType;
import com.vero.common.sql.parser.nodeextractor.EnhancedNodeExtractor;
import com.vero.common.sql.parser.nodeextractor.ExtractResult;
import com.vero.common.sql.parser.nodeextractor.NodeResult;
import com.vero.model.IJoinable;
import com.vero.model.JoinExpression;
import com.vero.model.Table;
import com.vero.model.report.IJoinExpression;
import com.vero.model.validations.ValidationException;
import com.vero.server.engine.JoinTree.JoinPath;;

public class JoinDef implements IJoinExpression {
	private IJoinable left;
	private IJoinable right;
	private String joinFormula;
	private int cost = -1;
	private CardinalityType cardinalityType;
	private JoinType joinType = JoinType.INNER_JOIN;
	private int allowRollDown = -1;

	public JoinDef(IJoinable left, IJoinable right, String joinExp, JoinType type) {
		this.left = left;
		this.right = right;
		this.joinFormula = joinExp;
		setJoinType(type);
	}

	public JoinDef() {
		this(null, null, null, null);
	}

	public static List<JoinDef> getJoinDefs(JoinPath jp) {
		long startTime = System.nanoTime();

		List<JoinDef> jd = Lists.newArrayList();
		int i = 0;
		
		Table lastTableRight = null;
		for (Iterator<Table> it = jp.getTableNodes().iterator(); it.hasNext();) {
			Table tbl = it.next();
			if (it.hasNext() && lastTableRight == null) {
				lastTableRight = it.next();

				JoinExpression joins = jp.getRelationships().get(i);
				String formula = joins.getFormula();
//					formula = switchSides(formula);
//					if(!joins.getStartNode().equals(tbl)){
//						formula = switchSides(formula);
//					}

				JoinDef jdi = new JoinDef(tbl, lastTableRight,
						formula, joins.getJoinType());
				jdi.setCardinalityType(joins.getCardinalityType());
				jd.add(jdi);
			}
			else if (!it.hasNext() && lastTableRight == null) {
				jd.add(new JoinDef(tbl, null, null, null));
			}
			else {
				JoinExpression joins = jp.getRelationships().get(i);
				String formula = joins.getFormula();
//					if(!joins.getStartNode().equals(tbl)){
//						formula = switchSides(formula);
//					}

				JoinDef jdi = new JoinDef(lastTableRight, tbl,
										formula, joins.getJoinType());
				jdi.setCardinalityType(joins.getCardinalityType());
				jd.add(jdi);
				lastTableRight = tbl;
			}
			i++;
		}
	
		// long startTime = System.nanoTime();
		System.out.println("getJoinDefs: " + (System.nanoTime() - startTime) / 1000000.0 + "md");
		return jd;
	}

	/* (non-Javadoc)
	 * @see com.vero.server.engine.IJoinDef#getLeft()
	 */
	@Override
	public IJoinable getLeft() {
		return left;
	}

	/* (non-Javadoc)
	 * @see com.vero.server.engine.IJoinDef#setLeft(com.vero.model.IJoinable)
	 */
	@Override
	public void setLeft(IJoinable left) {
		this.left = left;
	}

	/* (non-Javadoc)
	 * @see com.vero.server.engine.IJoinDef#getRight()
	 */
	@Override
	public IJoinable getRight() {
		return right;
	}

	/* (non-Javadoc)
	 * @see com.vero.server.engine.IJoinDef#setRight(com.vero.model.IJoinable)
	 */
	@Override
	public void setRight(IJoinable right) {
		this.right = right;
	}

	/* (non-Javadoc)
	 * @see com.vero.server.engine.IJoinDef#getJoinFormula()
	 */
	@Override
	public String getJoinFormula() {
		return joinFormula;
	}

	/* (non-Javadoc)
	 * @see com.vero.server.engine.IJoinDef#setJoinFormula(java.lang.String)
	 */
	@Override
	public void setJoinFormula(String joinExpression) {
		this.joinFormula = joinExpression;
	}

	/* (non-Javadoc)
	 * @see com.vero.server.engine.IJoinDef#getJoinType()
	 */
	@Override
	public JoinType getJoinType() {
		return joinType;
	}

	/* (non-Javadoc)
	 * @see com.vero.server.engine.IJoinDef#setJoinType(com.vero.common.constant.JoinType)
	 */
	@Override
	public void setJoinType(JoinType type) {
		this.joinType = type;
	}

	/* (non-Javadoc)
	 * @see com.vero.server.engine.IJoinDef#getCost()
	 */
	@Override
	public int getCost() {
		if (cost <= 0) {
			if (getLeft() instanceof Table) {
				cost = ((Table) getLeft()).getRowCount();
			}

			if (getRight() instanceof Table) {
				int right = (((Table) getRight()).getRowCount());
				cost = Math.max(cost, right);
			}
		}
		return cost;
	}

	/* (non-Javadoc)
	 * @see com.vero.server.engine.IJoinDef#setCost(int)
	 */
	@Override
	public void setCost(int cost) {
		this.cost = cost;
	}

	/* (non-Javadoc)
	 * @see com.vero.server.engine.IJoinDef#getCardinalityType()
	 */
	@Override
	public CardinalityType getCardinalityType() {
		return cardinalityType;
	}

	/* (non-Javadoc)
	 * @see com.vero.server.engine.IJoinDef#setCardinalityType(com.vero.common.constant.CardinalityType)
	 */
	@Override
	public void setCardinalityType(CardinalityType cardinalityType) {
		this.cardinalityType = cardinalityType;
	}

	/* (non-Javadoc)
	 * @see com.vero.server.engine.IJoinDef#setAllowRollDown(boolean)
	 */
	@Override
	public void setAllowRollDown(boolean allow){
		if(allow){
			allowRollDown = 1;
		}else{
			allowRollDown = 0;
		}
	}

	/* (non-Javadoc)
	 * @see com.vero.server.engine.IJoinDef#getAllowRollDown()
	 */
	@Override
	public int getAllowRollDown(){
		return allowRollDown;
	}

	/* (non-Javadoc)
	 * @see com.vero.server.engine.IJoinDef#samePair(com.vero.server.engine.JoinDef)
	 */
	@Override
	public boolean samePair(IJoinExpression j) {
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

	public void validate() throws ValidationException {
		if (getLeft() == null) {
			throw new ValidationException("Validation failed: Missing left table in JoinDef.");
		}
		else if (getRight() == null) {
			throw new ValidationException("Validation failed: Missing right table in JoinDef.");
		}
		else if (getJoinFormula() == null) {
			throw new ValidationException("Validation failed: Missing join formula in JoinDef.");
		}
		else if (getJoinType() == null) {
			throw new ValidationException("Validation failed: Missing join type in JoinDef.");
		}
		else if (getCardinalityType() == null) {
			throw new ValidationException("Validation failed: Missing join type in JoinDef.");
		}
		else if (!(getLeft() instanceof Table)) {
			throw new ValidationException("Validation failed: Left table in this join is not a Table.  Only joins between tables are saved in the graph.");
		}
		else if (!(getRight() instanceof Table)) {
			throw new ValidationException("Validation failed: Right table in this join is not a Table.  Only joins between tables are saved in the graph.");
		}

		ExtractResult extractResult = EnhancedNodeExtractor.extract(getJoinFormula());
		List<NodeResult> nodeResults = extractResult.getQualifiedNames();
		
		for (NodeResult nodeResult : nodeResults) {
		    System.out.println(nodeResult.getName());
		}
	}

	/* (non-Javadoc)
	 * @see com.vero.server.engine.IJoinDef#toString()
	 */
	@Override
	public String toString() {
		String s = getLeft().getPhysicalName();
		if (getRight() != null) {
			s += " <-" + getJoinType() + "-> " + getRight().getPhysicalName() + " Exp: " + getJoinFormula();
		}
		return s;
	}

}
