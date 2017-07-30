package com.hue.translator.sql.formatter;

import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.base.Function;
import com.vero.common.constant.DBType;

import static com.vero.server.engine.sql.formatter.VeroFunctions.*;

public class VeroPrestoExpFormatter extends VeroGenExpFormatter
{
    public VeroPrestoExpFormatter() { _formatterType = DBType.PRESTO; }

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
        protected String visitCurrentTime(CurrentTime node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append(node.getType().getName());

            return builder.toString();
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

            if (functionName.equals("trim") && (numArguments==1)) {
                builder.append("trim").append('(').append(arguments).append(')');
            } else if (functionName.equals("left_trim") && (numArguments==1)) {
                builder.append("ltrim").append('(').append(arguments).append(')');
            } else if (functionName.equals("right_trim") && (numArguments==1)) {
                builder.append("rtrim").append('(').append(arguments).append(')');
            } else if (functionName.equals("sinh")) {
                builder.append(processFuncSinh(this, node));
            } else if (functionName.equals("acosh")) {
                builder.append(processFuncAcosh(this, node));
            } else if (functionName.equals("asinh")) {
                builder.append(processFuncAsinh(this, node));
            } else if (functionName.equals("atanh")) {
                builder.append(processFuncAtanh(this, node));
            } else if (functionName.equals("atan2") && (numArguments==2)) {
                builder.append("atan2").append('(').append(node.getArguments().get(1)).append(", ").append(node.getArguments().get(0)).append(')');
            } else if (functionName.equals("nullifzero")) {
                builder.append("nullif").append('(').append(arguments).append(", 0").append(')');
            } else if (functionName.equals("log") && (numArguments == 1)) {
                builder.append("log10").append('(').append(arguments).append(')');
            } else if (functionName.equals("zeroifnull")) {
                builder.append("coalesce").append('(').append(arguments).append(", 0").append(')');
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
    }
}
