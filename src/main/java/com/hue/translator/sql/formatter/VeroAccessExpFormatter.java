package com.hue.translator.sql.formatter;

import java.util.Arrays;

import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Function;
import com.vero.common.constant.DBType;

import static com.vero.server.engine.sql.formatter.VeroFunctions.*;

public class VeroAccessExpFormatter extends VeroGenExpFormatter
{
    public VeroAccessExpFormatter() { _formatterType = DBType.ACCESS; }

    public void setSqlFormatter(VeroGenSqlFormatter sqlFormatter) {
        super.setSqlFormatter(sqlFormatter);
    }

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
            extends VeroGenExpFormatter.Formatter
    {
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

            if (functionName.equals("sinh")) {
                builder.append(processFuncSinh(this, node));
            } else if (functionName.equals("cosh")) {
                builder.append(processFuncCosh(this, node));
            } else if (functionName.equals("tanh")) {
                builder.append(processFuncTanh(this, node, getFormatterType()));
            } else if (functionName.equals("asin")) {
                builder.append(processFuncAsin(this, node, getFormatterType()));
            } else if (functionName.equals("acosh")) {
                builder.append(processFuncAcosh(this, node));
            } else if (functionName.equals("asinh")) {
                builder.append(processFuncAsinh(this, node));
            } else if (functionName.equals("atanh")) {
                builder.append(processFuncAtanh(this, node));
            } else if (functionName.equals("atan2") && (numArguments==2)) {
                builder.append("atan2").append('(').append(node.getArguments().get(1)).append(", ").append(node.getArguments().get(0)).append(')');
            } else if (functionName.equals("ln")) {
                builder.append("log").append('(').append(arguments).append(')');
            } else if (functionName.equals("log")) {
                builder.append("log10").append('(').append(arguments).append(')');
            } else if (functionName.equals("nullifzero") && (numArguments==1)) {
                Expression expression = node.getArguments().get(0);
                ComparisonExpression compExp = new ComparisonExpression(ComparisonExpression.Type.EQUAL, expression, new LongLiteral("0"));
                NullLiteral nullLit = new NullLiteral();
                builder.append("iif").append('(').append(joinExpressions(Arrays.asList(compExp, nullLit, expression))).append(')');
            } else if (functionName.equals("zeroifnull") && (numArguments==1)) {
                Expression expression = node.getArguments().get(0);
                FunctionCall funcIsNull = new FunctionCall(new QualifiedName("isnull"), Arrays.asList(expression));
                builder.append("iif").append('(').append(joinExpressions(Arrays.asList(funcIsNull, new LongLiteral("0"), expression))).append(')');
            } else if (functionName.equals("left_trim") && (numArguments==1)) {
                builder.append("ltrim").append('(').append(arguments).append(')');
            } else if (functionName.equals("right_trim") && (numArguments==1)) {
                builder.append("rtrim").append('(').append(arguments).append(')');
            } else if (functionName.equals("position") && (numArguments==2)) {
                builder.append("locate").append('(').append(node.getArguments().get(1)).append(", ").append(node.getArguments().get(0)).append(')');
            } else if (functionName.equals("year")) {
                builder.append(formatQualifiedFunctionName(node.getName())).append('(').append(arguments).append(')');
            } else if (functionName.equals("quarter")) {
                builder.append(formatQualifiedFunctionName(node.getName())).append('(').append(arguments).append(')');
            } else if (functionName.equals("month")) {
                builder.append(formatQualifiedFunctionName(node.getName())).append('(').append(arguments).append(')');
            } else if (functionName.equals("day")) {
                builder.append(formatQualifiedFunctionName(node.getName())).append('(').append(arguments).append(')');
            } else if (functionName.equals("hour")) {
                builder.append(formatQualifiedFunctionName(node.getName())).append('(').append(arguments).append(')');
            } else if (functionName.equals("minute")) {
                builder.append(formatQualifiedFunctionName(node.getName())).append('(').append(arguments).append(')');
            } else if (functionName.equals("second")) {
                builder.append(formatQualifiedFunctionName(node.getName())).append('(').append(arguments).append(')');
            } else {
                // use super
                return super.visitFunctionCall(node, context);
            }

            if (node.getWindow().isPresent()) {
                builder.append(" OVER ").append(visitWindow(node.getWindow().get(), null));
            }

            return builder.toString();
        }
        
        @Override
        protected String visitArithmeticExpression(ArithmeticExpression node, Void context)
        {
            if (node.getType() == ArithmeticExpression.Type.MODULUS) {
                return "mod(" + process(node.getLeft(), null) + ", " + process(node.getRight(), null) + ')';
            } else {
                // Access jdbc has issues when guarding divide by 0
                return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight());
            }
        }
    }
}
