package com.hue.translator.sql.formatter;

import com.facebook.presto.sql.tree.*;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.vero.common.constant.DBType;
import com.vero.server.engine.sql.formatter.VeroDateTimeUtils.DateTimeFormat;

import java.util.ArrayList;
import java.util.List;

import static com.vero.server.engine.sql.formatter.VeroFunctions.*;

public class VeroHiveExpFormatter extends VeroGenExpFormatter
{
    public VeroHiveExpFormatter() { _formatterType = DBType.HIVE; }

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

            if (node.getType() == CurrentTime.Type.DATE) {
                String format = VeroDateTimeUtils.convertDateTimeFormat(VeroDateTimeUtils.getDefaultDateTimeFormat(DateTimeFormat.Type.DATE).getFormat(), DBType.HIVE);
                builder.append("from_unixtime(unix_timestamp(), ").append("\"").append(format).append("\"").append(")");
            } else if (node.getType() == CurrentTime.Type.TIME) {
                String format = VeroDateTimeUtils.convertDateTimeFormat(VeroDateTimeUtils.getDefaultDateTimeFormat(DateTimeFormat.Type.DATETIME).getFormat(), DBType.HIVE);
                builder.append("from_unixtime(unix_timestamp(), ").append("\"").append(format).append("\"").append(")");
            } else {
                String format = VeroDateTimeUtils.convertDateTimeFormat(VeroDateTimeUtils.getDefaultDateTimeFormat(DateTimeFormat.Type.DATETIME).getFormat(), DBType.HIVE);
                builder.append("from_unixtime(unix_timestamp(), ").append("\"").append(format).append("\"").append(")");
            }

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

            if (functionName.equals("log") && (numArguments == 1)) {
                builder.append("log10").append('(').append(arguments).append(')');
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
            } else if (functionName.equals("atan2")) {
                builder.append(processFuncAtan2(this, node));
            } else if (functionName.equals("year")) {
                builder.append(formatQualifiedFunctionName(node.getName())).append('(').append(arguments).append(')');
            } else if (functionName.equals("quarter")) {
                builder.append(processFuncQuarter(this, node));
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
            } else if (functionName.equals("nullifzero")) {
                builder.append(processFuncNullifzero(this, node));
            } else if (functionName.equals("zeroifnull")) {
                builder.append("coalesce").append('(').append(arguments).append(", 0").append(')');
            } else if (functionName.equals("trim") && (numArguments==1)) {
                builder.append("trim").append('(').append(arguments).append(')');
            } else if (functionName.equals("left_trim") && (numArguments==1)) {
                builder.append("ltrim").append('(').append(arguments).append(')');
            } else if (functionName.equals("right_trim") && (numArguments==1)) {
                builder.append("rtrim").append('(').append(arguments).append(')');
            } else if (functionName.equals("position") && (numArguments==2)) {
                builder.append("instr").append('(').append(node.getArguments().get(0)).append(", ").append(node.getArguments().get(1)).append(')');
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
        public String visitWindow(Window node, Void context)
        {
            Boolean patchWindowFrame = false;

            List<String> parts = new ArrayList<>();

            if (!node.getPartitionBy().isEmpty()) {
                parts.add("PARTITION BY " + joinExpressions(node.getPartitionBy()));
            }
            if (!node.getOrderBy().isEmpty()) {
                parts.add("ORDER BY " + formatSortItems(node.getOrderBy()));
                if (node.getOrderBy().size() > 1) {
                    patchWindowFrame = true;
                }
            }
            if (node.getFrame().isPresent()) {
                parts.add(process(node.getFrame().get(), null));
            } else if (patchWindowFrame == true) {
                WindowFrame windowFrame = new WindowFrame(WindowFrame.Type.ROWS, new FrameBound(FrameBound.Type.UNBOUNDED_PRECEDING), null);
                parts.add(process(windowFrame, null));
            }

            return '(' + Joiner.on(' ').join(parts) + ')';
        }

        /*
        @Override
        protected String visitArithmeticExpression(ArithmeticExpression node, Void context)
        {
            if (node.getType() == ArithmeticExpression.Type.MODULUS) {
                return "mod(" + process(node.getLeft(), null) + ", " + process(node.getRight(), null) + ')';
            } else {
                return super.visitArithmeticExpression(node, context);
            }
        }
        */
    }
}
