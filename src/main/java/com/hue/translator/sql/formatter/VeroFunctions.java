package com.hue.translator.sql.formatter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NegativeExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.WhenClause;
import com.vero.common.constant.DBType;
import com.vero.common.sql.parser.VeroSqlParser;
import com.vero.server.engine.sql.formatter.VeroDateTimeUtils.DateTimeFormat;
import com.vero.server.engine.sql.formatter.VeroGenExpFormatter.Formatter;

class VeroFunctions {
	protected static String processFuncQuarter(Formatter formatter, FunctionCall node) {
		FunctionCall month = new FunctionCall(new QualifiedName("month"), node.getArguments());
		ArithmeticExpression substract = new ArithmeticExpression(ArithmeticExpression.Type.SUBTRACT, month, new LongLiteral("1"));
		ArithmeticExpression divide = new ArithmeticExpression(ArithmeticExpression.Type.DIVIDE, substract, new LongLiteral("3"));
		FunctionCall floor = new FunctionCall(new QualifiedName("floor"), Arrays.asList(divide));
		ArithmeticExpression add = new ArithmeticExpression(ArithmeticExpression.Type.ADD, floor, new LongLiteral("1"));
		return formatter.process(add, null);
	}

	protected static String processFuncSinh(Formatter formatter, FunctionCall node) {
		NegativeExpression negExp = new NegativeExpression(node.getArguments().get(0));
		FunctionCall termA = new FunctionCall(new QualifiedName("exp"), node.getArguments());
		FunctionCall termB = new FunctionCall(new QualifiedName("exp"), Arrays.asList(negExp));
		ArithmeticExpression substract = new ArithmeticExpression(ArithmeticExpression.Type.SUBTRACT, termA, termB);
		ArithmeticExpression divide = new ArithmeticExpression(ArithmeticExpression.Type.DIVIDE, substract, new LongLiteral("2"));
		return formatter.process(divide, null);
	}

	protected static String processFuncCosh(Formatter formatter, FunctionCall node) {
		NegativeExpression negExp = new NegativeExpression(node.getArguments().get(0));
		FunctionCall termA = new FunctionCall(new QualifiedName("exp"), node.getArguments());
		FunctionCall termB = new FunctionCall(new QualifiedName("exp"), Arrays.asList(negExp));
		ArithmeticExpression add = new ArithmeticExpression(ArithmeticExpression.Type.ADD, termA, termB);
		ArithmeticExpression divide = new ArithmeticExpression(ArithmeticExpression.Type.DIVIDE, add, new LongLiteral("2"));
		return formatter.process(divide, null);
	}

	protected static String processFuncTanh(Formatter formatter, FunctionCall node, DBType dbType) {
		/*
		 * if (dbType == DBType.ACCESS) { // 20150803: ToDo Access doesn't like
		 * using iif() to guard against division by 0 so I can only write plain
		 * formula StringBuilder builder = new StringBuilder();
		 * builder.append("((exp(")
		 * .append(formatter.process(node.getArguments().get(0),
		 * null)).append(")") .append(" - ") .append("exp(-(")
		 * .append(formatter.process(node.getArguments().get(0),
		 * null)).append(")") .append("))") .append(" / ") .append("((exp(")
		 * .append(formatter.process(node.getArguments().get(0),
		 * null)).append(")") .append(" + ") .append("exp(-(")
		 * .append(formatter.process(node.getArguments().get(0),
		 * null)).append(")") .append("))))"); return builder.toString(); } else
		 * { NegativeExpression negExp = new
		 * NegativeExpression(node.getArguments().get(0)); FunctionCall termA =
		 * new FunctionCall(new QualifiedName("exp"), node.getArguments());
		 * FunctionCall termB = new FunctionCall(new QualifiedName("exp"),
		 * Arrays.asList(negExp)); ArithmeticExpression subtract = new
		 * ArithmeticExpression(ArithmeticExpression.Type.SUBTRACT, termA,
		 * termB); ArithmeticExpression add = new
		 * ArithmeticExpression(ArithmeticExpression.Type.ADD, termA, termB);
		 * ArithmeticExpression divide = new
		 * ArithmeticExpression(ArithmeticExpression.Type.DIVIDE, subtract,
		 * add); return formatter.process(divide, null); }
		 */
		NegativeExpression negExp = new NegativeExpression(node.getArguments().get(0));
		FunctionCall termA = new FunctionCall(new QualifiedName("exp"), node.getArguments());
		FunctionCall termB = new FunctionCall(new QualifiedName("exp"), Arrays.asList(negExp));
		ArithmeticExpression subtract = new ArithmeticExpression(ArithmeticExpression.Type.SUBTRACT, termA, termB);
		ArithmeticExpression add = new ArithmeticExpression(ArithmeticExpression.Type.ADD, termA, termB);
		ArithmeticExpression divide = new ArithmeticExpression(ArithmeticExpression.Type.DIVIDE, subtract, add);
		return formatter.process(divide, null);
	}

	protected static String processFuncAsin(Formatter formatter, FunctionCall node, DBType dbType) {
		/*
		 * if (dbType == DBType.ACCESS) { // 20150803: ToDo Access doesn't like
		 * using iif() to guard against division by 0 so I can only write plain
		 * formula StringBuilder builder = new StringBuilder();
		 * builder.append("atan(")
		 * .append(formatter.process(node.getArguments().get(0), null))
		 * .append(" / ")
		 * .append("sqrt(1-power(").append(formatter.process(node.getArguments()
		 * .get(0), null)).append(", 2))") .append(')'); return
		 * builder.toString(); } else { FunctionCall xx = new FunctionCall(new
		 * QualifiedName("power"), Arrays.asList(node.getArguments().get(0), new
		 * LongLiteral("2"))); ArithmeticExpression subtract = new
		 * ArithmeticExpression(ArithmeticExpression.Type.SUBTRACT, new
		 * LongLiteral("1"), xx); FunctionCall sqrt = new FunctionCall(new
		 * QualifiedName("sqrt"), Arrays.asList(subtract)); ArithmeticExpression
		 * divide = new ArithmeticExpression(ArithmeticExpression.Type.DIVIDE,
		 * node.getArguments().get(0), sqrt); FunctionCall atan = new
		 * FunctionCall(new QualifiedName("atan"), Arrays.asList(divide));
		 * return formatter.process(atan, null); }
		 */
		FunctionCall xx = new FunctionCall(new QualifiedName("power"), Arrays.asList(node.getArguments().get(0),
				new LongLiteral("2")));
		ArithmeticExpression subtract = new ArithmeticExpression(ArithmeticExpression.Type.SUBTRACT, new LongLiteral("1"), xx);
		FunctionCall sqrt = new FunctionCall(new QualifiedName("sqrt"), Arrays.asList(subtract));
		ArithmeticExpression divide = new ArithmeticExpression(ArithmeticExpression.Type.DIVIDE, node.getArguments().get(
				0), sqrt);
		FunctionCall atan = new FunctionCall(new QualifiedName("atan"), Arrays.asList(divide));
		return formatter.process(atan, null);
	}

	protected static String processFuncAsinh(Formatter formatter, FunctionCall node) {
		ArithmeticExpression zSquare = new ArithmeticExpression(ArithmeticExpression.Type.MULTIPLY, node.getArguments().get(
				0), node.getArguments().get(0));
		ArithmeticExpression zSquareAddOne = new ArithmeticExpression(ArithmeticExpression.Type.ADD, zSquare, new LongLiteral("1"));
		FunctionCall sqrt = new FunctionCall(new QualifiedName("sqrt"), Arrays.asList(zSquareAddOne));
		ArithmeticExpression zAddSqrt = new ArithmeticExpression(ArithmeticExpression.Type.ADD, node.getArguments().get(
				0), sqrt);
		FunctionCall ln = new FunctionCall(new QualifiedName("ln"), Arrays.asList(zAddSqrt));
		return formatter.process(ln, null);
	}

	protected static String processFuncAcosh(Formatter formatter, FunctionCall node) {
		ArithmeticExpression zAddOne = new ArithmeticExpression(ArithmeticExpression.Type.ADD, node.getArguments().get(
				0), new LongLiteral("1"));
		FunctionCall sqrtZAddOne = new FunctionCall(new QualifiedName("sqrt"), Arrays.asList(zAddOne));
		ArithmeticExpression zSubOne = new ArithmeticExpression(ArithmeticExpression.Type.SUBTRACT, node.getArguments().get(
				0), new LongLiteral("1"));
		FunctionCall sqrtZSubOne = new FunctionCall(new QualifiedName("sqrt"), Arrays.asList(zSubOne));
		ArithmeticExpression sqrtMultiply = new ArithmeticExpression(ArithmeticExpression.Type.MULTIPLY, sqrtZAddOne, sqrtZSubOne);
		ArithmeticExpression zAddSqrtMultiply = new ArithmeticExpression(ArithmeticExpression.Type.ADD, node.getArguments().get(
				0), sqrtMultiply);
		FunctionCall ln = new FunctionCall(new QualifiedName("ln"), Arrays.asList(zAddSqrtMultiply));
		return formatter.process(ln, null);
	}

	protected static String processFuncAtanh(Formatter formatter, FunctionCall node) {
		ArithmeticExpression oneAddZ = new ArithmeticExpression(ArithmeticExpression.Type.ADD, new LongLiteral("1"), node.getArguments().get(
				0));
		ArithmeticExpression oneSubZ = new ArithmeticExpression(ArithmeticExpression.Type.SUBTRACT, new LongLiteral("1"), node.getArguments().get(
				0));
		ArithmeticExpression divide = new ArithmeticExpression(ArithmeticExpression.Type.DIVIDE, oneAddZ, oneSubZ);
		FunctionCall ln = new FunctionCall(new QualifiedName("ln"), Arrays.asList(divide));
		ArithmeticExpression multiply = new ArithmeticExpression(ArithmeticExpression.Type.MULTIPLY, new DoubleLiteral("0.5"), ln);
		return formatter.process(multiply, null);
	}

	protected static String processFuncPower(Formatter formatter, FunctionCall node) {
		FunctionCall ln = new FunctionCall(new QualifiedName("ln"), Arrays.asList(node.getArguments().get(0)));
		ArithmeticExpression multiply = new ArithmeticExpression(ArithmeticExpression.Type.MULTIPLY, node.getArguments().get(
				1), ln);
		FunctionCall exp = new FunctionCall(new QualifiedName("exp"), Arrays.asList(multiply));
		return formatter.process(exp, null);
	}

	protected static String processFuncAtan2(Formatter formatter, FunctionCall node) {
		Expression x = node.getArguments().get(0);
		Expression y = node.getArguments().get(1);

		FunctionCall xx = new FunctionCall(new QualifiedName("power"), Arrays.asList(x, new LongLiteral("2")));
		FunctionCall yy = new FunctionCall(new QualifiedName("power"), Arrays.asList(y, new LongLiteral("2")));
		ArithmeticExpression xxAddyy = new ArithmeticExpression(ArithmeticExpression.Type.ADD, xx, yy);
		FunctionCall sqrt_xxAddyy = new FunctionCall(new QualifiedName("sqrt"), Arrays.asList(xxAddyy));
		ArithmeticExpression substract = new ArithmeticExpression(ArithmeticExpression.Type.SUBTRACT, sqrt_xxAddyy, x);
		ArithmeticExpression divide = new ArithmeticExpression(ArithmeticExpression.Type.DIVIDE, substract, y);
		FunctionCall arctan = new FunctionCall(new QualifiedName("atan"), Arrays.asList(divide));
		ArithmeticExpression multiply = new ArithmeticExpression(ArithmeticExpression.Type.MULTIPLY, new DoubleLiteral("2"), arctan);
		return formatter.process(multiply, null);
	}

	protected static String processFuncNullifzero(Formatter formatter, FunctionCall node) {
		Expression x = node.getArguments().get(0);

		List<WhenClause> listWhen = new ArrayList<WhenClause>();
		ComparisonExpression ce = new ComparisonExpression(ComparisonExpression.Type.EQUAL, x, new LongLiteral("0"));
		WhenClause wc = new WhenClause(ce, new NullLiteral());
		listWhen.add(wc);
		SearchedCaseExpression sce = new SearchedCaseExpression(listWhen, x);
		return formatter.process(sce, null);
	}

	protected static String processFuncTier(Formatter formatter, FunctionCall node) {
		List<Expression> expressions = node.getArguments();
		int numArg = expressions.size();
		Expression mainExp = VeroSqlParser.createExpression(
				VeroSqlParser.parseExpression(expressions.get(0).toString()));
		List<WhenClause> listWhen = new ArrayList<WhenClause>();

		if (numArg == 2) {
			// only one number argument
			WhenClause wc = null;
			String min = expressions.get(1).toString();
			Literal minl = null;
			if (min.contains(".")) {
				minl = new DoubleLiteral(min);
			}
			else {
				minl = new LongLiteral(min);
			}

			ComparisonExpression ce = new ComparisonExpression(ComparisonExpression.Type.EQUAL, mainExp, minl);
			String represent = "equal " + min;
			wc = new WhenClause(ce, new StringLiteral(represent));
			listWhen.add(wc);
		}
		else {
			for (int i = 1; i < numArg; i++) {
				WhenClause wc = null;

				String min = "0";
				String max = expressions.get(i).toString();
				String represent = null;
				if (i == 1) {
				}
				else {
					min = expressions.get(i - 1).toString();
				}
				Literal minl = null;
				Literal maxl = null;
				if (min.contains(".")) {
					minl = new DoubleLiteral(min);
				}
				else {
					minl = new LongLiteral(min);
				}
				if (max.contains(".")) {
					maxl = new DoubleLiteral(max);
				}
				else {
					maxl = new LongLiteral(max);
				}

				if (i == 1) {
					ComparisonExpression ce1 = new ComparisonExpression(ComparisonExpression.Type.LESS_THAN, mainExp, maxl);
					represent = "< " + max;
					wc = new WhenClause(ce1, new StringLiteral(represent));
					listWhen.add(wc);
				}
				else {
					ComparisonExpression ce1 = new ComparisonExpression(ComparisonExpression.Type.GREATER_THAN_OR_EQUAL, mainExp, minl);
					ComparisonExpression ce2 = new ComparisonExpression(ComparisonExpression.Type.LESS_THAN, mainExp, maxl);
					LogicalBinaryExpression lbe = new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND, ce1, ce2);
					// represent = "[" + min + "-" + max + ")";
					represent = min + "-" + max;
					wc = new WhenClause(lbe, new StringLiteral(represent));
					listWhen.add(wc);

					if (i == numArg - 1) {
						ce1 = new ComparisonExpression(ComparisonExpression.Type.GREATER_THAN_OR_EQUAL, mainExp, maxl);
						represent = ">= " + max;
						wc = new WhenClause(ce1, new StringLiteral(represent));
						listWhen.add(wc);
					}
				}
			}
		}

		SearchedCaseExpression sce = new SearchedCaseExpression(listWhen, new StringLiteral("Unknown"));

		return formatter.process(sce, null);
	}

	protected static Expression processFuncLast(ComparisonExpression node) {
		System.out.println("Processing last()");
		Expression rightNode = node.getRight();
		Expression leftNode = node.getLeft();
		FunctionCall last = (FunctionCall) rightNode;
		// # of arguments are already checked outside 1 or 2
		String number = last.getArguments().get(0).toString();
		String format = "DAY"; // default
		if (last.getArguments().size() == 2) {
			format = last.getArguments().get(1).toString().replaceAll("\"", "");
		}

		IntervalLiteral.Sign sign;
		if (number.startsWith("-")) {
			sign = IntervalLiteral.Sign.NEGATIVE;
			number = number.substring(1);
		}
		else {
			sign = IntervalLiteral.Sign.POSITIVE;
		}

		CurrentTime cTime = new CurrentTime(CurrentTime.Type.DATE);
		IntervalLiteral interval = new IntervalLiteral(number, sign, format);
		ArithmeticExpression arithmOp = new ArithmeticExpression(ArithmeticExpression.Type.SUBTRACT, cTime, interval);

		BetweenPredicate bPredicate = new BetweenPredicate(leftNode, arithmOp, cTime);
		return bPredicate;
	}

	protected static String processFuncSet(Formatter formatter, FunctionCall node) {
		StringBuilder builder = new StringBuilder();
		String functionName = getFunctionName(node);
		int numArguments = node.getArguments().size();

		builder.append(functionName).append('(').append(formatter.process(node.getArguments().get(0), null)).append(
				')');

		if (numArguments > 1) {
			builder.append(" ON ");
		}

		for (int i = 1; i < numArguments; i++) {
			Expression item = node.getArguments().get(i);

			if (i == 1) {
				builder.append(formatter.process(item, null));
			}
			else {
				builder.append(", ").append(formatter.process(item, null));
			}
		}

		return builder.toString();
	}

	protected static String processFuncDateDiff(Formatter formatter, FunctionCall node, String arguments,
			DBType dbType) {
		StringBuilder builder = new StringBuilder();
		String functionName = getFunctionName(node);
		int numArguments = node.getArguments().size();
		IntervalLiteral.IntervalField intervalField = null;

		// check # of arguments
		if (numArguments > 3 || numArguments < 2) {
			builder.append(functionName).append('(').append(arguments).append(')');
			return builder.toString();
		}

		// ToDo: check argument and make literal date to DATE

		Expression startDate = node.getArguments().get(0);
		Expression endDate = node.getArguments().get(1);

		String format = null;
		if (numArguments == 2) {
			format = "DAY";
		}
		else {
			format = node.getArguments().get(2).toString().replaceAll("\"", "");
		}

		switch (dbType) {
		case TERADATA:
		case POSTGRESQL:
		case ORACLE:
		case NETEZZA:
			ArithmeticExpression substract = new ArithmeticExpression(ArithmeticExpression.Type.SUBTRACT, endDate, startDate);
			return formatter.process(substract, null);
		case MYSQL:
		case HIVE:
			builder.append(functionName).append('(').append(formatter.process(endDate, null)).append(", ").append(
					formatter.process(startDate, null)).append(')');
			return builder.toString();
		case MSSQL:
		case REDSHIFT:
		case VERTICA:
		case AZURE:
			builder.append(functionName).append('(').append(format).append(", ").append(
					formatter.process(startDate, null)).append(", ").append(formatter.process(endDate, null)).append(
							')');
			return builder.toString();
		case PRESTO:
			intervalField = dateFormat(format);
			builder.append("date_diff(").append("'").append(dateAccessFormat(intervalField)).append("'").append(
					", ").append(formatter.process(startDate, null)).append(", ").append(
							formatter.process(endDate, null)).append(")");			
		case ACCESS:
			intervalField = dateFormat(format);
			builder.append("datediff(").append("'").append(dateAccessFormat(intervalField)).append("'").append(
					", ").append(formatter.process(startDate, null)).append(", ").append(
							formatter.process(endDate, null)).append(")");
			break;
		case DERBY_LOCAL:
			intervalField = dateFormat(format);
			builder.append("{fn timestampdiff(").append(dateDerbyFormat(intervalField)).append(", ").append(
					formatter.process(startDate, null)).append(", ").append(formatter.process(endDate, null)).append(
							")}");
			return builder.toString();
		case DRILL:
			builder.append("extract(day from ").append("date_diff").append('(').append(
					formatter.process(endDate, null)).append(", ").append(formatter.process(startDate, null)).append(
							"))");
			return builder.toString();
		case SALESFORCE:
		case GOOGLEANALYTICS:
			builder.append(functionName).append('(').append(dateSalesforceGoogleAnalyticsFormat(intervalField)).append(
					", ").append(formatter.process(startDate, null)).append(", ").append(
							formatter.process(endDate, null)).append(')');
			return builder.toString();
		default:
			builder.append(functionName).append('(').append(arguments).append(')');
			break;
		}

		return builder.toString();
	}

	protected static String processFuncDateAddSub(Formatter formatter, FunctionCall node, String arguments,
			ArithmeticExpression.Type arithmType, DBType dbType) {
		StringBuilder builder = new StringBuilder();
		String functionName = getFunctionName(node);
		int numArguments = node.getArguments().size();

		// check # of arguments
		if ((numArguments > 3) || (numArguments < 2)) {
			builder.append(functionName).append('(').append(arguments).append(')');
			return builder.toString();
		}

		// ToDo: check argument and make literal date to DATE

		Expression dateExp = node.getArguments().get(0);
		Expression numberExp = node.getArguments().get(1);
		String numberStr = formatter.process(numberExp, null);

		// System.out.println("Expression instance==> " +
		// node.getArguments().get(1).getClass().getName() );

		String format;
		if (numArguments == 2) {
			format = "DAY";
		}
		else {
			format = node.getArguments().get(2).toString().replaceAll("\"", "");
		}

		IntervalLiteral.IntervalField intervalField = dateFormat(format);
		IntervalLiteral interval = null;
		ArithmeticExpression arithmOp = null;
		Expression numberExpression = null;

		switch (dbType) {
		case TERADATA:
			IntervalLiteral.Sign sign;
			if (numberStr.startsWith("-")) {
				sign = IntervalLiteral.Sign.NEGATIVE;
				numberStr = numberStr.substring(1);
			}
			else {
				sign = IntervalLiteral.Sign.POSITIVE;
			}
			if (intervalField == null) {
				interval = new IntervalLiteral(numberStr, sign, format);
			}
			else {
				interval = new IntervalLiteral(numberStr, sign, intervalField);
			}

			arithmOp = new ArithmeticExpression(arithmType, dateExp, interval);
			return formatter.process(arithmOp, null);
		case POSTGRESQL:
		case ORACLE:
		case MYSQL:
		case NETEZZA:
		case VERTICA:
		case PRESTO:
			if (intervalField == null) {
				interval = new IntervalLiteral(numberStr, IntervalLiteral.Sign.POSITIVE, format);
			}
			else {
				interval = new IntervalLiteral(numberStr, IntervalLiteral.Sign.POSITIVE, intervalField);
			}

			arithmOp = new ArithmeticExpression(arithmType, dateExp, interval);
			return formatter.process(arithmOp, null);
		case REDSHIFT:
		case MSSQL:
		case AZURE:
			if (arithmType == ArithmeticExpression.Type.SUBTRACT) {
				numberExpression = new NegativeExpression(numberExp);
			}
			else {
				numberExpression = numberExp;
			}

			if (intervalField == null) {
				builder.append("dateadd").append('(').append(format).append(", ").append(
						formatter.process(numberExpression, null)).append(", ").append(
								formatter.process(dateExp, null)).append(')');
			}
			else {
				builder.append("dateadd").append('(').append(intervalField).append(", ").append(
						formatter.process(numberExpression, null)).append(", ").append(
								formatter.process(dateExp, null)).append(')');
			}

			return builder.toString();
		case ACCESS:
			if (arithmType == ArithmeticExpression.Type.SUBTRACT) {
				numberExpression = new NegativeExpression(numberExp);
			}
			else {
				numberExpression = numberExp;
			}

			builder.append("dateadd(").append("'").append(dateAccessFormat(intervalField)).append("'").append(
					", ").append(formatter.process(numberExpression, null)).append(", ").append(
							formatter.process(dateExp, null)).append(")");

			return builder.toString();
		case DERBY_LOCAL:
			// ToDo: when date is a literal for derby, neet to use timestamp()
			// transformation
			if (arithmType == ArithmeticExpression.Type.SUBTRACT) {
				numberExpression = new NegativeExpression(numberExp);
			}
			else {
				numberExpression = numberExp;
			}

			builder.append("{fn timestampadd(").append(dateDerbyFormat(intervalField)).append(", ").append(
					formatter.process(numberExpression, null)).append(", ").append(
							formatter.process(dateExp, null)).append(")}");

			return builder.toString();
		case HIVE:
			String funcName = null;

			if (arithmType == ArithmeticExpression.Type.SUBTRACT) {
				funcName = "date_sub";
				if (numberStr.startsWith("-")) {
					numberExpression = numberExp;
				}
				else {
					numberExpression = new NegativeExpression(numberExp);
				}
			}
			else {
				funcName = "date_add";
				numberExpression = numberExp;
			}

			builder.append(funcName).append('(').append(formatter.process(dateExp, null)).append(", ").append(
					formatter.process(numberExpression, null)).append(')');

			return builder.toString();
		case DRILL:
			if (arithmType == ArithmeticExpression.Type.SUBTRACT) {
				funcName = "date_sub";
			}
			else {
				funcName = "date_add";
			}

			if (intervalField == null) {
				interval = new IntervalLiteral(numberStr, IntervalLiteral.Sign.POSITIVE, format);
			}
			else {
				interval = new IntervalLiteral(numberStr, IntervalLiteral.Sign.POSITIVE, intervalField);
			}

			builder.append(funcName).append('(').append(formatter.process(dateExp, null)).append(", ").append(
					interval).append(')');

			return builder.toString();
		case SALESFORCE:
		case GOOGLEANALYTICS:
			if (arithmType == ArithmeticExpression.Type.SUBTRACT) {
				numberExpression = new NegativeExpression(numberExp);
			}
			else {
				numberExpression = numberExp;
			}

			builder.append("dateadd").append('(').append(dateSalesforceGoogleAnalyticsFormat(intervalField)).append(
					", ").append(formatter.process(numberExpression, null)).append(", ").append(
							formatter.process(dateExp, null)).append(')');

			return builder.toString();
		default:
			builder.append(functionName).append('(').append(arguments).append(')');
			break;
		}

		return null;
	}

	protected static String processFuncDate(Formatter formatter, FunctionCall node, String arguments, DBType dbType) {
		StringBuilder builder = new StringBuilder();
		String functionName = getFunctionName(node);
		int numArguments = node.getArguments().size();
		Boolean isTimeStamp = false;

		// check # of arguments
		if (numArguments < 1 || numArguments > 2) {
			builder.append(functionName).append('(').append(arguments).append(')');
			return builder.toString();
		}

		Expression dateExpr = node.getArguments().get(0);
		String dateString = null;
		if (dateExpr instanceof StringLiteral) {
			dateString = dateExpr.toString().replaceAll("'", "");
		}
		else {
		}
		String formatString = null;
		VeroDateTimeUtils.DateTimeFormat dateTimeFormat = null;
		Expression formatExpr = null;
		FunctionCall toDate = null;

		if (numArguments == 1) {
			// auto-detect format or use db-default format (check what db can
			// allow omit of the format argument)
			dateTimeFormat = VeroDateTimeUtils.determineDateFormat(dateString, dbType);
			formatString = VeroDateTimeUtils.convertDateTimeFormat(dateTimeFormat.getFormat(), dbType);
			if (dateTimeFormat.getType() == DateTimeFormat.Type.DATETIME) {
				isTimeStamp = true;
			}
		}
		else if (numArguments == 2) {
			// use provided format
			formatExpr = node.getArguments().get(1);
			formatString = node.getArguments().get(1).toString().replaceAll("'", "");
		}

		if (formatExpr == null) {
			formatExpr = new StringLiteral(formatString);
		}

		if ((dbType == DBType.POSTGRESQL) || (dbType == DBType.REDSHIFT) || (dbType == DBType.DRILL)
				|| (dbType == DBType.NETEZZA) || (dbType == DBType.VERTICA)) {
			if ((dbType == DBType.POSTGRESQL) || (dbType == DBType.DRILL) || (dbType == DBType.NETEZZA)
					|| (dbType == DBType.VERTICA)) {
				if (isTimeStamp == true) {
					toDate = new FunctionCall(new QualifiedName("to_timestamp"), Arrays.asList(dateExpr, formatExpr));
				}
				else {
					toDate = new FunctionCall(new QualifiedName("to_date"), Arrays.asList(dateExpr, formatExpr));
				}
			}
			else {
				// only REDSHIFT
				toDate = new FunctionCall(new QualifiedName("to_date"), Arrays.asList(dateExpr, formatExpr));
			}
			return formatter.process(toDate, null);
		}
		else if ((dbType == DBType.ORACLE) || (dbType == DBType.TERADATA)) {
			String usedFunctionName = null;
			if (isTimeStamp == true) {
				usedFunctionName = "to_timestamp";
			}
			else {
				usedFunctionName = "to_date";
			}

			if (numArguments == 1) {
				// toDate = new FunctionCall(new
				// QualifiedName(usedFunctionName), Arrays.asList(dateExpr));
				// use vero default format
				toDate = new FunctionCall(new QualifiedName(usedFunctionName), Arrays.asList(dateExpr, formatExpr));
			}
			else {
				toDate = new FunctionCall(new QualifiedName("to_date"), Arrays.asList(dateExpr, formatExpr));
			}
			return formatter.process(toDate, null);
		}
		else if (dbType == DBType.MYSQL) {
			toDate = new FunctionCall(new QualifiedName("str_to_date"), Arrays.asList(dateExpr, formatExpr));
			return formatter.process(toDate, null);
		}
		else if (dbType == DBType.PRESTO) {
			toDate = new FunctionCall(new QualifiedName("date_parse"), Arrays.asList(dateExpr, formatExpr));
			return formatter.process(toDate, null);
	    } else if (dbType == DBType.MSSQL || dbType == DBType.AZURE) {
			builder.append("convert").append('(').append("datetime").append(", ").append(
					formatter.process(dateExpr, null)).append(')');
			return builder.toString();
		}
		else if (dbType == DBType.DERBY_LOCAL) {
			String usedFunctionName = null;
			if (isTimeStamp == true) {
				usedFunctionName = "timestamp";
			}
			else {
				usedFunctionName = "date";
			}

			toDate = new FunctionCall(new QualifiedName(usedFunctionName), Arrays.asList(dateExpr));
			return formatter.process(toDate, null);
		}
		else if (dbType == DBType.HIVE) {
			toDate = new FunctionCall(new QualifiedName("to_date"), Arrays.asList(dateExpr));
			return formatter.process(toDate, null);
		}
		else if (dbType == DBType.ACCESS) {
			FunctionCall dateValueFunc = new FunctionCall(new QualifiedName("datevalue"), Arrays.asList(dateExpr));
			toDate = new FunctionCall(new QualifiedName("format"), Arrays.asList(dateValueFunc, formatExpr));

			return formatter.process(toDate, null);
		}
		else if ((dbType == DBType.SALESFORCE) || (dbType == DBType.GOOGLEANALYTICS)) {
			Date date = null;
			try {
				if (numArguments == 1) {
					date = VeroDateTimeUtils.parse(dateString, dateTimeFormat.getJavaFormat());
				}
				else {
					date = VeroDateTimeUtils.parse(dateString, formatString);
				}
			}
			catch (Exception e) {
			}

			if (date != null) {
				String year = new String(Integer.toString(date.getYear() + 1900));
				String month = new String(Integer.toString(date.getMonth() + 1));
				String day = new String(Integer.toString(date.getDate()));
				String hour = new String(Integer.toString(date.getHours()));
				String minute = new String(Integer.toString(date.getMinutes()));
				String second = new String(Integer.toString(date.getSeconds()));

				if (isTimeStamp == true) {
					toDate = new FunctionCall(new QualifiedName("DATETIMEFROMPARTS"), Arrays.asList(
							new LongLiteral(year), new LongLiteral(month), new LongLiteral(day), new LongLiteral(hour),
							new LongLiteral(minute), new LongLiteral(second), new LongLiteral(Integer.toString(0))));
				}
				else {
					toDate = new FunctionCall(new QualifiedName("SMALLDATETIMEFROMPARTS"), Arrays.asList(
							new LongLiteral(year), new LongLiteral(month), new LongLiteral(day), new LongLiteral(hour),
							new LongLiteral(minute)));
				}
				return formatter.process(toDate, null);
			}
			else {
				builder.append(functionName).append('(').append(arguments).append(')');
				return builder.toString();
			}
		}
		else {
			builder.append(functionName).append('(').append(arguments).append(')');
			return builder.toString();
		}
	}

	private static String dateSalesforceGoogleAnalyticsFormat(IntervalLiteral.IntervalField format) {
		String retStr = null;

		if (format == null) {
			return "'d'";
		}

		switch (format) {
		case YEAR:
			retStr = "'yyyy'";
			break;
		case QUARTER:
			retStr = "'q'";
			break;
		case MONTH:
			retStr = "'m'";
			break;
		case DAY:
			retStr = "'d'";
			break;
		case WEEK:
			retStr = "'wk'";
			break;
		case HOUR:
			retStr = "'hh'";
			break;
		case MINUTE:
			retStr = "'mi'";
			break;
		case SECOND:
			retStr = "'ss'";
			break;
		case MILLISECOND:
			retStr = "'ms'";
			break;
		case DAYOFYEAR:
			retStr = "'dy'";
			break;
		case MICROSECOND:
		case NANOSECOND:
		default:
			retStr = "'d'";
			break;
		}

		return retStr;
	}

	private static String dateDerbyFormat(IntervalLiteral.IntervalField format) {
		String retStr = null;

		if (format == null) {
			return "SQL_TSI_DAY";
		}

		switch (format) {
		case YEAR:
			retStr = "SQL_TSI_YEAR";
			break;
		case QUARTER:
			retStr = "SQL_TSI_QUARTER";
			break;
		case MONTH:
			retStr = "SQL_TSI_MONTH";
			break;
		case DAY:
			retStr = "SQL_TSI_DAY";
			break;
		case WEEK:
			retStr = "SQL_TSI_WEEK";
			break;
		case HOUR:
			retStr = "SQL_TSI_HOUR";
			break;
		case MINUTE:
			retStr = "SQL_TSI_MINUTE";
			break;
		case SECOND:
			retStr = "SQL_TSI_SECOND";
			break;
		case MILLISECOND:
			retStr = "SQL_TSI_FRAC_SECOND";
			break;
		case MICROSECOND:
		case NANOSECOND:
		case DAYOFYEAR:
		default:
			retStr = "SQL_TSI_DAY";
			break;
		}

		return retStr;
	}

	private static String dateAccessFormat(IntervalLiteral.IntervalField format) {
		String retStr = null;

		if (format == null) {
			return "d";
		}

		switch (format) {
		case YEAR:
			retStr = "yyyy";
			break;
		case QUARTER:
			retStr = "q";
			break;
		case MONTH:
			retStr = "m";
			break;
		case DAYOFYEAR:
			retStr = "y";
			break;
		case DAY:
			retStr = "d";
			break;
		case WEEK:
			retStr = "w";
			break;
		case HOUR:
			retStr = "h";
			break;
		case MINUTE:
			retStr = "n";
			break;
		case SECOND:
			retStr = "s";
			break;
		case MILLISECOND:
		case MICROSECOND:
		case NANOSECOND:
		default:
			retStr = "d";
			break;
		}

		return retStr;
	}

	private static IntervalLiteral.IntervalField dateFormat(String format) {
		IntervalLiteral.IntervalField retIntervalField = null;
		format = format.toLowerCase();

		switch (format) {
		case "year":
		case "yy":
		case "yyyy":
			retIntervalField = IntervalLiteral.IntervalField.YEAR;
			break;
		case "quarter":
		case "qq":
		case "q":
			retIntervalField = IntervalLiteral.IntervalField.QUARTER;
			break;
		case "month":
		case "mm":
		case "m":
			retIntervalField = IntervalLiteral.IntervalField.MONTH;
			break;
		case "dayofyear":
		case "dy":
		case "y":
			retIntervalField = IntervalLiteral.IntervalField.DAYOFYEAR;
			break;
		case "day":
		case "dd":
		case "d":
			retIntervalField = IntervalLiteral.IntervalField.DAY;
			break;
		case "week":
		case "wk":
		case "ww":
			retIntervalField = IntervalLiteral.IntervalField.WEEK;
			break;
		case "hour":
		case "hh":
			retIntervalField = IntervalLiteral.IntervalField.HOUR;
			break;
		case "minute":
		case "mi":
		case "n":
			retIntervalField = IntervalLiteral.IntervalField.MINUTE;
			break;
		case "second":
		case "ss":
		case "s":
			retIntervalField = IntervalLiteral.IntervalField.SECOND;
			break;
		case "millisecond":
		case "ms":
			retIntervalField = IntervalLiteral.IntervalField.MILLISECOND;
			break;
		case "microsecond":
		case "mcs":
			retIntervalField = IntervalLiteral.IntervalField.MICROSECOND;
			break;
		case "nanosecond":
		case "ns":
			retIntervalField = IntervalLiteral.IntervalField.NANOSECOND;
			break;
		default:
			retIntervalField = null;
			break;
		}

		return retIntervalField;
	}

	protected static String getFunctionName(FunctionCall node) {
		return node.getName().getParts().get(0).toLowerCase();
	}
}
