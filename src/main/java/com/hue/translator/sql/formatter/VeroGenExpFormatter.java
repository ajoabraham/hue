/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hue.translator.sql.formatter;

import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.InputReference;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NegativeExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.Pass;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.vero.common.constant.DBType;

import static com.vero.server.engine.sql.formatter.VeroFunctions.*;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Iterables.transform;

public class VeroGenExpFormatter
{
    public VeroGenSqlFormatter sqlFormatter = null;
    public DBType _formatterType;
    public String _version = null;
    public Boolean _outputPassFunc = true;
    public Boolean _outputDivideByZeroGuard = false;

    public VeroGenExpFormatter() { _formatterType = DBType.UNKNOWN; }

    public void setSqlFormatter(VeroGenSqlFormatter sqlFormatter) {
        this.sqlFormatter = sqlFormatter;
    }

    public void setVersion(String version) {
        this._version = version;
    }

    public void setOutputPassFunc(Boolean outputPassFunc) {
        this._outputPassFunc = outputPassFunc;
    }
    
    public void setOutputDivideByZeroGuard(Boolean outputDivideByZeroGuard) {
        this._outputDivideByZeroGuard = outputDivideByZeroGuard;
    }

    public DBType getFormatterType() { return _formatterType; }
    public String getVersion() { return _version; }

    public String formatExpression(Expression expression)
    {
        return new Formatter().process(expression, null);
    }

    public Function<Expression, String> expressionFormatterFunction()
    {
        return new Function<Expression, String>()
        {
            @Override
            public String apply(Expression input)
            {
                return formatExpression(input);
            }
        };
    }

    public class Formatter
            extends AstVisitor<String, Void>
    {
        @Override
        protected String visitNode(Node node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException(String.format("not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected String visitCurrentTime(CurrentTime node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append(node.getType().getName());

            if (node.getPrecision() != null) {
                builder.append('(')
                        .append(node.getPrecision())
                        .append(')');
            }

            return builder.toString();
        }

        @Override
        protected String visitExtract(Extract node, Void context)
        {
            return "EXTRACT(" + node.getField() + " FROM " + process(node.getExpression(), context) + ")";
        }

        @Override
        protected String visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return String.valueOf(node.getValue());
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Void context)
        {
            return formatStringLiteral(node.getValue());
        }

        @Override
        protected String visitArrayConstructor(ArrayConstructor node, Void context)
        {
            ImmutableList.Builder<String> valueStrings = ImmutableList.builder();
            for (Expression value : node.getValues()) {
                valueStrings.add(sqlFormatter.formatSql(value));
            }
            return "ARRAY[" + Joiner.on(",").join(valueStrings.build()) + "]";
        }

        @Override
        protected String visitSubscriptExpression(SubscriptExpression node, Void context)
        {
            return sqlFormatter.formatSql(node.getBase()) + "[" + sqlFormatter.formatSql(node.getIndex()) + "]";
        }

        @Override
        protected String visitLongLiteral(LongLiteral node, Void context)
        {
            return Long.toString(node.getValue());
        }

        @Override
        protected String visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return Double.toString(node.getValue());
        }

        @Override
        protected String visitGenericLiteral(GenericLiteral node, Void context)
        {
            return node.getType() + " '" + node.getValue() + "'";
        }

        @Override
        protected String visitTimeLiteral(TimeLiteral node, Void context)
        {
            return "TIME '" + node.getValue() + "'";
        }

        @Override
        protected String visitTimestampLiteral(TimestampLiteral node, Void context)
        {
            return "TIMESTAMP '" + node.getValue() + "'";
        }

        @Override
        protected String visitNullLiteral(NullLiteral node, Void context)
        {
            return "null";
        }

        @Override
        protected String visitIntervalLiteral(IntervalLiteral node, Void context)
        {
            String sign = (node.getSign() == IntervalLiteral.Sign.NEGATIVE) ? "-" : "";
            
            Boolean isNumeric = isNumeric(node.getValue());
            //System.out.println("############ node.getValue()==> " + node.getValue() + " numeric==> " + isNumeric);
            
            StringBuilder builder = new StringBuilder()
                .append("INTERVAL ")
                .append(sign);
                
            if (isNumeric == true) {
                builder.append(" '").append(node.getValue()).append("' ");
            } else {
                builder.append(node.getValue()).append(" ");
            }

            if (node.getStartField() != null) {
            	builder.append(node.getStartField());
            } else {
            	builder.append(node.getStartText());
            }

            if (node.getEndField() != null)  {
                builder.append(" TO ").append(node.getEndField());
            }
            return builder.toString();
        }

        @Override
        protected String visitSubqueryExpression(SubqueryExpression node, Void context)
        {
            return "(" + sqlFormatter.formatSql(node.getQuery()) + ")";
        }

        @Override
        protected String visitExists(ExistsPredicate node, Void context)
        {
            return "EXISTS (" + sqlFormatter.formatSql(node.getSubquery()) + ")";
        }

        @Override
        protected String visitQualifiedNameReference(QualifiedNameReference node, Void context)
        {
            return formatQualifiedName(node.getName());
        }

        private String formatQualifiedName(QualifiedName name)
        {
            List<String> parts = new ArrayList<>();
            for (String part : name.getParts()) {
                parts.add(formatIdentifier(sqlFormatter.caseSensitivityName(part)));
            }
            return Joiner.on('.').join(parts);
        }

        @Override
        public String visitInputReference(InputReference node, Void context)
        {
            // add colon so this won't parse
            return ":input(" + node.getChannel() + ")";
        }

        @Override
        protected String visitFunctionCall(FunctionCall node, Void context)
        {
            StringBuilder builder = new StringBuilder();
            String functionName = getFunctionName(node);

            int numArguments = node.getArguments().size();
            String arguments = joinExpressions(node.getArguments());
            if (node.getArguments().isEmpty() && "count".equalsIgnoreCase(node.getName().getSuffix())) {
                arguments = "*";
            }
            if (node.isDistinct()) {
                arguments = "DISTINCT " + arguments;
            }

            if (_outputPassFunc == false) {
                if (functionName.equals("year")) {
                    builder.append("extract").append('(').append("year").append(" from ").append(arguments).append(')');
                } else if (functionName.equals("quarter")) {
                    builder.append("extract").append('(').append("quarter").append(" from ").append(arguments).append(')');
                } else if (functionName.equals("month")) {
                    builder.append("extract").append('(').append("month").append(" from ").append(arguments).append(')');
                } else if (functionName.equals("day")) {
                    builder.append("extract").append('(').append("day").append(" from ").append(arguments).append(')');
                } else if (functionName.equals("hour")) {
                    builder.append("extract").append('(').append("hour").append(" from ").append(arguments).append(')');
                } else if (functionName.equals("minute")) {
                    builder.append("extract").append('(').append("minute").append(" from ").append(arguments).append(')');
                } else if (functionName.equals("second")) {
                    builder.append("extract").append('(').append("second").append(" from ").append(arguments).append(')');
                } else if (functionName.equals("trim") && (numArguments==1)) {
                    builder.append("trim").append('(').append("both ' ' from ").append(arguments).append(')');
                } else if (functionName.equals("left_trim") && (numArguments==1)) {
                    builder.append("trim").append('(').append("leading ' ' from ").append(arguments).append(')');
                } else if (functionName.equals("right_trim") && (numArguments==1)) {
                    builder.append("trim").append('(').append("trailing ' ' from ").append(arguments).append(')');
                } else if (functionName.equals("position") && (numArguments==2)) {
                    builder.append("position").append('(').append(node.getArguments().get(1)).append(" in ").append(node.getArguments().get(0)).append(')');
                } else if (functionName.equals("tier")) {
                    builder.append(processFuncTier(this, node));
                } else if (functionName.equals("datediff")) {
                    builder.append(processFuncDateDiff(this, node, arguments, getFormatterType()));
                } else if (functionName.equals("dateadd")) {
                    builder.append(processFuncDateAddSub(this, node, arguments, ArithmeticExpression.Type.ADD, getFormatterType()));
                } else if (functionName.equals("datesub")) {
                    builder.append(processFuncDateAddSub(this, node, arguments, ArithmeticExpression.Type.SUBTRACT, getFormatterType()));
                } else if (functionName.equals("d")) {
                    builder.append(processFuncDate(this, node, arguments, getFormatterType()));
                } else if ((functionName.equalsIgnoreCase("include") || functionName.equalsIgnoreCase("exclude")) && (numArguments >= 1)){
                    builder.append(processFuncSet(this, node));
                } else {
                    builder.append(formatQualifiedFunctionName(node.getName())).append('(').append(arguments).append(')');
                }
            } else {
                // leave as is
                builder.append(formatQualifiedFunctionName(node.getName())).append('(').append(arguments).append(')');
            }

            if (node.getWindow().isPresent()) {
                builder.append(" OVER ").append(visitWindow(node.getWindow().get(), null));
            }

            return builder.toString();
        }

        protected String formatQualifiedFunctionName(QualifiedName name)
        {
            List<String> parts = new ArrayList<>();
            for (String part : name.getParts()) {
                parts.add(sqlFormatter.caseSensitivityName(part));
            }
            return Joiner.on('.').join(parts);
        }

        @Override
        protected String visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            return formatBinaryExpression(node.getType().toString(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitNotExpression(NotExpression node, Void context)
        {
            return "(NOT " + process(node.getValue(), null) + ")";
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, Void context)
        {
            return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            return "(" + process(node.getValue(), null) + " IS NULL)";
        }

        @Override
        protected String visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            return "(" + process(node.getValue(), null) + " IS NOT NULL)";
        }

        @Override
        protected String visitNullIfExpression(NullIfExpression node, Void context)
        {
            return "NULLIF(" + process(node.getFirst(), null) + ", " + process(node.getSecond(), null) + ')';
        }

        @Override
        protected String visitIfExpression(IfExpression node, Void context)
        {
            StringBuilder builder = new StringBuilder();
            builder.append("IF(")
                    .append(process(node.getCondition(), context))
                    .append(", ")
                    .append(process(node.getTrueValue(), context));
            if (node.getFalseValue().isPresent()) {
                builder.append(", ")
                        .append(process(node.getFalseValue().get(), context));
            }
            builder.append(")");
            return builder.toString();
        }

        @Override
        protected String visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            return "COALESCE(" + joinExpressions(node.getOperands()) + ")";
        }

        @Override
        protected String visitNegativeExpression(NegativeExpression node, Void context)
        {
            String value = process(node.getValue(), null);
            String separator = value.startsWith("-") ? " " : "";
            return "-" + separator + value;
        }

        @Override
        protected String visitArithmeticExpression(ArithmeticExpression node, Void context)
        {
            if (node.getType().equals(ArithmeticExpression.Type.DIVIDE)) {
                if (_outputDivideByZeroGuard == true) {
                    if (node.getRight() instanceof FunctionCall) {
                        if (getFunctionName((FunctionCall) node.getRight()).equals("nullifzero")) {
                            // bypass appending nullifzero
                            return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight());
                        }
                    } else if (node.getRight() instanceof Literal) {
            			// purely literal
            			return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight());
                    }
    
                    List<Expression> arguments = new ArrayList<Expression>();
                    arguments.add(node.getRight());
                    FunctionCall nullifzeroFunc = new FunctionCall(new QualifiedName("nullifzero"), arguments);
                    return formatBinaryExpression(node.getType().getValue(), node.getLeft(), nullifzeroFunc);
                } else {
                    return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight());
                }
            } else {
                return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight());
            }
        }

        @Override
        protected String visitLikePredicate(LikePredicate node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append('(')
                    .append(process(node.getValue(), null))
                    .append(" LIKE ")
                    .append(process(node.getPattern(), null));

            if (node.getEscape() != null) {
                builder.append(" ESCAPE ")
                        .append(process(node.getEscape(), null));
            }

            builder.append(')');

            return builder.toString();
        }

        @Override
        protected String visitAllColumns(AllColumns node, Void context)
        {
            if (node.getPrefix().isPresent()) {
                return node.getPrefix().get() + ".*";
            }

            return "*";
        }

        @Override
        public String visitCast(Cast node, Void context)
        {
            return (node.isSafe() ? "TRY_CAST" : "CAST") +
                    "(" + process(node.getExpression(), context) + " AS " + node.getType() + ")";
        }

        @Override
        protected String visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
        {
            ImmutableList.Builder<String> parts = ImmutableList.builder();
            parts.add("CASE").add("\n");
            for (WhenClause whenClause : node.getWhenClauses()) {
                parts.add(process(whenClause, context)).add("\n");
            }
            if (node.getDefaultValue() != null) {
                parts.add("ELSE")
                        .add(process(node.getDefaultValue(), context)).add("\n");
            }
            parts.add("END").add("\n");

            return "(" + Joiner.on(' ').join(parts.build()) + ")";
        }

        @Override
        protected String visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
            ImmutableList.Builder<String> parts = ImmutableList.builder();

            parts.add("CASE").add(process(node.getOperand(), context));

            for (WhenClause whenClause : node.getWhenClauses()) {
                parts.add(process(whenClause, context));
            }
            if (node.getDefaultValue() != null) {
                parts.add("ELSE").add(process(node.getDefaultValue(), context));
            }
            parts.add("END");

            return "(" + Joiner.on(' ').join(parts.build()) + ")";
        }

        @Override
        protected String visitWhenClause(WhenClause node, Void context)
        {
            return "WHEN " + process(node.getOperand(), context) + " THEN " + process(node.getResult(), context);
        }

        @Override
        protected String visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            return "(" + process(node.getValue(), context) + " BETWEEN " +
                    process(node.getMin(), context) + " AND " + process(node.getMax(), context) + ")";
        }

        @Override
        protected String visitInPredicate(InPredicate node, Void context)
        {
            return "(" + process(node.getValue(), context) + " IN " + process(node.getValueList(), context) + ")";
        }

        @Override
        protected String visitInListExpression(InListExpression node, Void context)
        {
            return "(" + joinExpressions(node.getValues()) + ")";
        }

        // TODO: add tests for window clause formatting, as these are not really expressions
        @Override
        public String visitWindow(Window node, Void context)
        {
            List<String> parts = new ArrayList<>();

            if (!node.getPartitionBy().isEmpty()) {
                parts.add("PARTITION BY " + joinExpressions(node.getPartitionBy()));
            }
            if (!node.getOrderBy().isEmpty()) {
                parts.add("ORDER BY " + formatSortItems(node.getOrderBy()));
            }
            if (node.getFrame().isPresent()) {
                parts.add(process(node.getFrame().get(), null));
            }

            return '(' + Joiner.on(' ').join(parts) + ')';
        }

        @Override
        public String visitWindowFrame(WindowFrame node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append(node.getType().toString()).append(' ');

            if (node.getEnd().isPresent()) {
                builder.append("BETWEEN ")
                        .append(process(node.getStart(), null))
                        .append(" AND ")
                        .append(process(node.getEnd().get(), null));
            }
            else {
                builder.append(process(node.getStart(), null));
            }

            return builder.toString();
        }

        @Override
        public String visitFrameBound(FrameBound node, Void context)
        {
            switch (node.getType()) {
                case UNBOUNDED_PRECEDING:
                    return "UNBOUNDED PRECEDING";
                case PRECEDING:
                    return process(node.getValue().get(), null) + " PRECEDING";
                case CURRENT_ROW:
                    return "CURRENT ROW";
                case FOLLOWING:
                    return process(node.getValue().get(), null) + " FOLLOWING";
                case UNBOUNDED_FOLLOWING:
                    return "UNBOUNDED FOLLOWING";
            }
            throw new IllegalArgumentException("unhandled type: " + node.getType());
        }

        @Override
        public String visitPass(Pass node, Void context)
        {
            // 20150805: don't try to parse vero idents inside pass for now
            /*
            List<Expression> expanded = new ArrayList<Expression>();
            String literal = node.getActualContents();

            Pattern veroPattern = Pattern.compile(VeroIdent.VeroIdentPattern);
            Matcher veroMatcher = veroPattern.matcher(literal);

            int last_end = 0;
            while (veroMatcher.find()) {
                String matched = veroMatcher.group(0);

                int start = veroMatcher.start();
                int end = veroMatcher.end();

                if (start != 0) {
                    //System.out.println("Forming pass1==> " + literal.substring(last_end, start));
                    expanded.add(new Pass("(".concat(literal.substring(last_end, start)).concat(")")));
                }
                //System.out.println("Forming pass2==> " + matched);
                expanded.add(new QualifiedNameReference(new QualifiedName(matched)));

                last_end = end;
            }

            if (last_end != literal.length()) {
                //System.out.println("Forming pass3==> " + literal.substring(last_end, literal.length()));
                expanded.add(new Pass("(".concat(literal.substring(last_end, literal.length())).concat(")")));
            }

            if (expanded.size() != 1) {
                if (_outputPassFunc != true) {
                    return joinPassExpressions("", expanded);
                } else {
                    return joinPassExpressions(" +VPC+ ", expanded);
                }
            } else {
                Expression curExp = expanded.get(0);
                if (curExp instanceof QualifiedNameReference) {
                    // exact match
                    return '(' + process(curExp, null) + ')';
                } else {
                    // Pass: no match, only itself
                    if (_outputPassFunc != true) {
                        return node.getActualContents();
                    } else {
                        return "PASS(".concat(node.getActualContents()).concat(")");
                    }
                }
            }
            */
            
            // Pass: no match, only itself
            if (_outputPassFunc != true) {
                return node.getActualContents();
            } else {
                return "PASS(".concat(node.getActualContents()).concat(")");
            }
            
        }

        protected String formatBinaryExpression(String operator, Expression left, Expression right)
        {
            if (operator.equals("")) {
                // 20150709: +VPC+
                return process(left, null) + process(right, null);
            } else {
                return '(' + process(left, null) + ' ' + operator + ' ' + process(right, null) + ')';
            }
        }

        protected String joinPassExpressions(String on, List<Expression> expressions)
        {
            return Joiner.on(on).join(transform(expressions, new Function<Expression, Object>()
            {
                @Override
                public Object apply(Expression input)
                {
                    if (input instanceof QualifiedNameReference) {
                        // 20150709: enclose vero ident in () in case association matters
                        return '(' + process(input, null) + ')';
                    } else {
                        return process(input, null);
                    }
                }
            }));
        }

        protected String joinExpressions(List<Expression> expressions)
        {
            return Joiner.on(", ").join(transform(expressions, new Function<Expression, Object>()
            {
                @Override
                public Object apply(Expression input)
                {
                    return process(input, null);
                }
            }));
        }

        protected String joinExpressions(String on, List<Expression> expressions)
        {
            return Joiner.on(on).join(transform(expressions, new Function<Expression, Object>()
            {
                @Override
                public Object apply(Expression input)
                {
                    return process(input, null);
                }
            }));
        }

        protected String formatIdentifier(String s)
        {
            // TODO: handle escaping properly
            return sqlFormatter.getIdenSurroundingSymbolLeft() + s + sqlFormatter.getIdenSurroundingSymbolRight();
        }
    }

    public String formatStringLiteral(String s)
    {
        return "'" + s.replace("'", "''") + "'";
    }

    public String formatSortItems(List<SortItem> sortItems)
    {
        return Joiner.on(", ").join(transform(sortItems, sortItemFormatterFunction()));
    }

    private Function<SortItem, String> sortItemFormatterFunction()
    {
        return input -> {
            StringBuilder builder = new StringBuilder();

            builder.append(formatExpression(input.getSortKey()));

            switch (input.getOrdering()) {
                case ASCENDING:
                    builder.append(" ASC");
                    break;
                case DESCENDING:
                    builder.append(" DESC");
                    break;
                default:
                    throw new UnsupportedOperationException("unknown ordering: " + input.getOrdering());
            }

            switch (input.getNullOrdering()) {
                case FIRST:
                    builder.append(" NULLS FIRST");
                    break;
                case LAST:
                    builder.append(" NULLS LAST");
                    break;
                case UNDEFINED:
                    // no op
                    break;
                default:
                    throw new UnsupportedOperationException("unknown null ordering: " + input.getNullOrdering());
            }

            return builder.toString();
        };
    }
    
    private boolean isNumeric(String s) {  
        return s.matches("[-+]?\\d*\\.?\\d+");  
    }
}
