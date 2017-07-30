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

import com.facebook.presto.sql.tree.CreateTempTable;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Statement;

public class VeroOracleSqlFormatter extends VeroGenSqlFormatter
{
    public VeroOracleSqlFormatter(VeroGenExpFormatter expFormatter) {
        super(expFormatter);
        setIdenSurroundingSymbolLeft('"');
        setIdenSurroundingSymbolRight('"');
        setIsIdentSurrounded(true);
        //setCaseSensitivity(CaseSensitivity.CASE_INSENSITIVE_UPPER);
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
        protected Void visitCreateTempTable(CreateTempTable node, Integer indent)
        {
            builder.append("CREATE GLOBAL TEMPORARY TABLE ")
                .append(getIdenSurroundingSymbolLeft())
                .append(caseSensitivityName(node.getName()))
                .append(getIdenSurroundingSymbolRight())
                .append(" ON COMMIT PRESERVE ROWS")
                //.append(" ON COMMIT DELETE ROWS")
                .append(" AS ");

            process(node.getQuery(), indent);

            return null;
        }
    }
}