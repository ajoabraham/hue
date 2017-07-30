package com.hue.translator.sql.formatter;

import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.TableSubquery;

public class VeroMysqlSqlFormatter extends VeroGenSqlFormatter
{
    public VeroMysqlSqlFormatter(VeroGenExpFormatter expFormatter) {
        super(expFormatter);
        setIdenSurroundingSymbolLeft('`');
        setIdenSurroundingSymbolRight('`');
        setIsIdentSurrounded(true);
        setCaseSensitivity(CaseSensitivity.CASE_SENSITIVE);
    }

    public String formatSql(Node root) {
        StringBuilder builder = new StringBuilder();

        if (root instanceof Statement) {
            setOutputPassFunc(false);
            setOutputDivideByZeroGuard(true);
        } else {
            // Expression
            setOutputPassFunc(true);
            setOutputDivideByZeroGuard(true);
        }

        new Formatter(builder).process(root, 0);
        return builder.toString();
    }

    public String formatSql(String version, Node root) {
	setVersion(version);
	return formatSql(root);
    }

    public class Formatter
            extends VeroGenSqlFormatter.Formatter
    {
        public Formatter(StringBuilder builder) {
            super(builder);
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Integer indent)
        {
            // 20141121: remove "()" around select
            //builder.append('(').append('\n');
            builder.append('\n');

            process(node.getQuery(), indent + 1);

            // 20141121: remove "()" around select
            //append(indent, ") ");
            append(indent, " ");

            return null;
        }
    }
}