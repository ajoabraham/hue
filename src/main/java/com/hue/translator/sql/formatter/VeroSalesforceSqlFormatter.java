package com.hue.translator.sql.formatter;

import com.facebook.presto.sql.tree.CreateTempTable;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.TableSubquery;

public class VeroSalesforceSqlFormatter extends VeroGenSqlFormatter
{
    public VeroSalesforceSqlFormatter(VeroGenExpFormatter expFormatter) {
        super(expFormatter);
        setIdenSurroundingSymbolLeft('[');
        setIdenSurroundingSymbolRight(']');
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
        private boolean _createTempTable = false;
        private QualifiedName _tempTableName = null;

        public Formatter(StringBuilder builder) {
            super(builder);
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Integer indent)
        {
            if (_createTempTable == true) {
                process(node.getQuery(), indent);
            } else {
                super.visitTableSubquery(node, indent);
            }

            return null;
        }

        @Override
        protected Void visitCreateTempTable(CreateTempTable node, Integer indent)
        {
            _createTempTable = true;
            _tempTableName = node.getName();
            process(node.getQuery(), indent);
            return null;
        }

        @Override
        protected Void visitSelect(Select node, Integer indent)
        {
            super.visitSelect(node,  indent);

            if (_createTempTable == true) {
                builder.append("INTO ").append(getIdenSurroundingSymbolLeft())
                .append(caseSensitivityName(_tempTableName)).append(getIdenSurroundingSymbolRight())
                .append("\n");
            }

            return null;
        }
    }
}