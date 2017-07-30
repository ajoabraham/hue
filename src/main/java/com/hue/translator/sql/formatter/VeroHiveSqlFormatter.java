package com.hue.translator.sql.formatter;

import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Statement;

public class VeroHiveSqlFormatter extends VeroMysqlSqlFormatter
{
    public VeroHiveSqlFormatter(VeroGenExpFormatter expFormatter) {
        super(expFormatter);
        setIsParenAroundJoin(false);
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
        extends VeroMysqlSqlFormatter.Formatter
    {
        public Formatter(StringBuilder builder) {
            super(builder);
        }
    }
}