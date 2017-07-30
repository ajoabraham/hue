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
package com.hue.translator.sql;

import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.vero.common.sql.parser.VeroSqlParser;
import com.vero.server.engine.sql.formatter.FormatterFactory;

public final class TreePrinter
{
    public static String treeToPureString(Tree tree) {
        if (tree.getChildCount() == 0) {
            return quotedString(tree.toString());
        }
        StringBuilder sb = new StringBuilder();

        for (Tree t : children(tree)) {
            sb.append(treeToPureString(t));
        }

        return sb.toString();
    }

    public static String treeToString(Tree tree)
    {
        return treeToString(tree, 1);
    }

    private static String treeToString(Tree tree, int depth)
    {
        if (tree.getChildCount() == 0) {
            return quotedString(tree.toString());
        }
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        sb.append(tree.toString());
        for (Tree t : children(tree)) {
            if (hasSubtree(t) && (leafCount(tree) > 2)) {
                sb.append("\n");
                sb.append(repeat("   ", depth));
            }
            else {
                sb.append(" ");
            }
            sb.append(treeToString(t, depth + 1));
        }
        sb.append(")");
        return sb.toString();
    }

    private static String quotedString(String s)
    {
        return s.contains(" ") ? ('"' + s + '"') : s;
    }

    private static boolean hasSubtree(Tree tree)
    {
        for (Tree t : children(tree)) {
            if (t.getChildCount() > 0) {
                return true;
            }
        }
        return false;
    }

    private static int leafCount(Tree tree)
    {
        if (tree.getChildCount() == 0) {
            return 1;
        }

        int n = 0;
        for (Tree t : children(tree)) {
            n += leafCount(t);
        }
        return n;
    }

    private static List<Tree> children(Tree tree)
    {
	List<Tree> list = new ArrayList<Tree>();

        for (int i = 0; i < tree.getChildCount(); i++) {
            list.add(tree.getChild(i));
        }
        return list;
    }

    public static String repeat(String str, int num) {
        int len = num * str.length();
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < num; i++) {
            sb.append(str);
        }
        return sb.toString();
    }

    public static void printExpression(String sql)
    {
        println(sql.trim());
        println("");

        System.out.println("EXP Printing CommonTree toString...");
        CommonTree tree = VeroSqlParser.parseExpression(sql);
        println(TreePrinter.treeToString(tree));
        println("");

        System.out.println("EXP Printing AST toString...");
        Expression expression = VeroSqlParser.createExpression(tree);
        println(expression.toString());
        println("");

        System.out.println("EXP Printing Format sql toString...");
        // TODO: support formatting all statement types
        println(FormatterFactory.getSqlFormatter().formatSql(expression));
        println("");

        println(repeat("=", 60));
        println("");
    }

    public static void printStatement(String sql)
    {
        println(sql.trim());
        println("");

        System.out.println("STATE Printing CommonTree toString...");
        CommonTree tree = VeroSqlParser.parseStatement(sql);
        println(TreePrinter.treeToString(tree));
        println("");

        System.out.println("STATE Printing AST toString...");
        Statement statement = VeroSqlParser.createStatement(tree);
        println(statement.toString());
        println("");

        System.out.println("STATE Printing Format sql toString...");
        // TODO: support formatting all statement types
        if (statement instanceof Query) {
            println(FormatterFactory.getSqlFormatter().formatSql(statement));
            println("");
        }

        println(repeat("=", 60));
        println("");
    }

    private static void println(String s)
    {
        System.out.println(s);
    }
}
