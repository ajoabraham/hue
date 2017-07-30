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

public class VeroNetezzaSqlFormatter extends VeroGenSqlFormatter
{
    public VeroNetezzaSqlFormatter(VeroGenExpFormatter expFormatter) {
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
    }
}
