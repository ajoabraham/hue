package com.hue.translator.sql.formatter;

import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTempTable;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Statement;

public class VeroTeradataSqlFormatter extends VeroGenSqlFormatter
{
    public VeroTeradataSqlFormatter(VeroGenExpFormatter expFormatter) {
        super(expFormatter);
        setIdenSurroundingSymbolLeft('"');
        setIdenSurroundingSymbolRight('"');
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
        protected Void visitCreateTable(CreateTable node, Integer indent)
        {
            builder.append("CREATE TABLE ")
                .append(genDottedName(node.getName()))
                .append(" AS ");

            process(node.getQuery(), indent);

            builder.append("WITH DATA");

            return null;
        }

        @Override
        protected Void visitCreateTempTable(CreateTempTable node, Integer indent)
        {
            builder.append("CREATE VOLATILE TABLE ")
                .append(genDottedName(node.getName()))
                .append(" AS ");

            process(node.getQuery(), indent);

            builder.append("WITH DATA ON COMMIT PRESERVE ROWS");

            return null;
        }
    }
}