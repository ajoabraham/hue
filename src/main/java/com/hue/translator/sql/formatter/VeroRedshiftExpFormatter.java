package com.hue.translator.sql.formatter;

import java.util.ArrayList;
import java.util.List;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.vero.common.constant.DBType;

import static com.vero.server.engine.sql.formatter.VeroFunctions.*;

public class VeroRedshiftExpFormatter extends VeroGenExpFormatter
{
    public VeroRedshiftExpFormatter() { _formatterType = DBType.REDSHIFT; }

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
	private String windowFunctionName = null;

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

            if (functionName.equals("concat")) {
                builder.append(joinExpressions(" || ", node.getArguments()));
            } else if (functionName.equals("sinh")) {
                builder.append(processFuncSinh(this, node));
            } else if (functionName.equals("cosh")) {
                builder.append(processFuncCosh(this, node));
            } else if (functionName.equals("tanh")) {
                builder.append(processFuncTanh(this, node, getFormatterType()));
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
            } else if (functionName.equals("zeroifnull")) {
                builder.append("coalesce").append('(').append(arguments).append(", 0").append(')');
            } else if (functionName.equals("substr")) {
                if (numArguments==2) {
                    builder.append("substring").append('(').append(node.getArguments().get(0)).append(", ").append(node.getArguments().get(1))
                    .append(", ").append("len").append('(').append(process(node.getArguments().get(0), null)).append(')').append(')');
                } else {
                    builder.append("substring").append('(').append(arguments).append(')');
                }
            } else {
                if (node.getWindow().isPresent()) {
                    //System.out.println("WindowFuncName: " + getFunctionName(node));
                    windowFunctionName = getFunctionName(node);
                }

                // use super
                return super.visitFunctionCall(node, context);
            }

            if (node.getWindow().isPresent()) {
                builder.append(" OVER ").append(visitWindow(node.getWindow().get(), null));
            }

            return builder.toString();
        }

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
            } else {
                if (!windowFunctionName.equalsIgnoreCase("rank")) {
                    if (!node.getOrderBy().isEmpty()) {
            			// Redshift needs to specify frame explicitly if there is order by and the function is not rank()
            			FrameBound fb = new FrameBound(FrameBound.Type.UNBOUNDED_PRECEDING);
            			WindowFrame wf = new WindowFrame(WindowFrame.Type.ROWS, fb, null);
            			parts.add(process(wf, null));
        			}
        		}
            }

            return '(' + Joiner.on(' ').join(parts) + ')';
        }
    }
}
