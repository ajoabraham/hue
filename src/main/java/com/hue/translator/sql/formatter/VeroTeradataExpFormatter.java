package com.hue.translator.sql.formatter;

import static com.vero.server.engine.sql.formatter.VeroFunctions.*;

import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.base.Function;
import com.vero.common.constant.DBType;

public class VeroTeradataExpFormatter extends VeroGenExpFormatter
{
    public VeroTeradataExpFormatter() { _formatterType = DBType.TERADATA; }

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
            String functionName = VeroFunctions.getFunctionName(node);

            //int numArguments = node.getArguments().size();
            String arguments = joinExpressions(node.getArguments());
            if (node.getArguments().isEmpty() && "count".equalsIgnoreCase(node.getName().getSuffix())) {
                arguments = "*";
            }
            if (node.isDistinct()) {
                arguments = "DISTINCT " + arguments;
            }

            if (functionName.equals("quarter")) {
				builder.append(processFuncQuarter(this, node));
            } else if (functionName.equals("concat")) {
            	builder.append(joinExpressions(" || ", node.getArguments()));
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
                return '(' + process(node.getLeft(), null) + ' ' + "mod" + ' ' + process(node.getRight(), null) + ')';
            } else {
                return super.visitArithmeticExpression(node, context);
            }
        }
    }
}
