using System;
using System.Diagnostics;
using System.Linq.Expressions;
using Dapper;
using DapperAid;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DapperAidTest
{
    /// <summary>
    /// Where条件組み立ての動作サンプル
    /// </summary>
    [TestClass]
    public class WhereSample
    {

        class T
        {
            public int? IntCol { get; set; }
            public int? OtherCol { get; set; }
            public string TextCol { get; set; }
        }

        string Select<T>(Expression<Func<T, bool>> where)
        {
            var parameters = new DynamicParameters();
            var sql = QueryBuilder.DefaultInstance.BuildWhere(parameters, where);
            Trace.WriteLine(sql);
            return sql;
        }

        [TestMethod]
        public void Tutorial()
        {
            int? val1 = 100; // (bound to @IntCol)
            Select<T>(t => t.IntCol == val1);
            Select<T>(t => t.IntCol != val1);
            Select<T>(t => t.IntCol < val1);
            Select<T>(t => t.IntCol > val1);
            Select<T>(t => t.IntCol <= val1);
            Select<T>(t => t.IntCol >= val1);

            int? val2 = null;
            Select<T>(t => t.IntCol == val2);
            Select<T>(t => t.IntCol != val2);

            Select<T>(t => t.IntCol == t.OtherCol);

            string[] inValues = { "111", "222", "333" }; // (bound to @TextCol)
            Select<T>(t => t.TextCol == SqlExpr.In(inValues));
            Select<T>(t => t.TextCol != SqlExpr.In(inValues));
            Select<T>(t => t.IntCol != SqlExpr.In(new[] { 1, 2, 3 }));

            string likeValue = "%test%"; // (bound to @TextCol)
            Select<T>(t => t.TextCol == SqlExpr.Like(likeValue));
            Select<T>(t => t.TextCol != SqlExpr.Like(likeValue));

            int b1 = 1; // (bound to @IntCol)
            int b2 = 99; // (bound to @P01)
            Select<T>(t => t.IntCol == SqlExpr.Between(b1, b2));

            Select<T>(t => t.TextCol == "111" && t.IntCol < 200);
            Select<T>(t => t.TextCol == "111" || t.IntCol < 200);
            Select<T>(t => !(t.TextCol == "111" || t.IntCol < 200));

            string text1 = "111";
            string text2 = null;
            Select<T>(t => text1 == null || t.TextCol == text1);
            Select<T>(t => text1 != null && t.TextCol == text1);
            Select<T>(t => text2 == null || t.TextCol == text2);
            Select<T>(t => text2 != null && t.TextCol == text2);

            Select<T>(t => t.TextCol == SqlExpr.In<string>("select text from otherTable where..."));
            Select<T>(t => t.IntCol == SqlExpr.In<int?>("select text from otherTable where..."));

            Assert.AreEqual(" where \"TextCol\" in(select text from otherTable where a=@P00 and b=@P01)",
                Select<T>(t => t.TextCol == SqlExpr.In<string>("select text from otherTable where a=", 1, " and b=", 2))
            );

            Select<T>(t => SqlExpr.Eval("exists(select * from otherTable where...)"));

            var idText = "userIdText";
            var pwText = "passswordText";
            var salt = "hashsalt";
            Select<T>(t => SqlExpr.Eval("id=", idText, " AND pw=CRYPT(", pwText, ", pw)"));
            Select<T>(t => t.TextCol == SqlExpr.Eval<string>("CRYPT(", pwText, ", pw)"));
            Select<T>(t => t.TextCol == SqlExpr.Eval<string>("CRYPT(", pwText, ",", salt, ")"));
        }

        [TestMethod]
        public void ErrorTest1()
        {
            Assert.ThrowsException<ArgumentException>(() =>
            {
                Select<T>(t => t.OtherCol == SqlExpr.Eval<int?>("errortest", 1, "引数5でエラーになることを確認", false, 3.5));
            });
        }

        [TestMethod]
        public void ConditionalExpressionTest()
        {   // 三項演算子のテスト
            bool trueValue = true;
            bool falseValue = false;
            QueryBuilder.DefaultInstance = new QueryBuilder.SQLite(); 

            var sql1 = Select<T>(t => (trueValue) ? SqlExpr.Eval("<true時の条件>") : SqlExpr.Eval("<false時の条件>"));
            var sql1b = Select<T>(t => (trueValue) ? trueValue : SqlExpr.Eval("<false時の条件>"));
            var sql2 = Select<T>(t => (falseValue) ? SqlExpr.Eval("<true時の条件>") : SqlExpr.Eval("<false時の条件>"));
            var sql2b = Select<T>(t => (falseValue) ? SqlExpr.Eval("<true時の条件>") : falseValue);
            Assert.AreEqual(" where <true時の条件>", sql1);
            Assert.AreEqual(" where " + QueryBuilder.DefaultInstance.TrueLiteral, sql1b);
            Assert.AreEqual(" where <false時の条件>", sql2);
            Assert.AreEqual(" where " + QueryBuilder.DefaultInstance.FalseLiteral, sql2b);

            var sql11 = Select<T>(t => SqlExpr.Eval("<分岐条件>") ? trueValue : trueValue);
            var sql12 = Select<T>(t => SqlExpr.Eval("<分岐条件>") ? trueValue : falseValue);
            var sql13 = Select<T>(t => SqlExpr.Eval("<分岐条件>") ? trueValue : SqlExpr.Eval("<false時の条件>"));
            var sql21 = Select<T>(t => SqlExpr.Eval("<分岐条件>") ? falseValue : trueValue);
            var sql22 = Select<T>(t => SqlExpr.Eval("<分岐条件>") ? falseValue : falseValue);
            var sql23 = Select<T>(t => SqlExpr.Eval("<分岐条件>") ? falseValue : SqlExpr.Eval("<false時の条件>"));
            var sql31 = Select<T>(t => SqlExpr.Eval("<分岐条件>") ? SqlExpr.Eval("<true時の条件>") : trueValue);
            var sql32 = Select<T>(t => SqlExpr.Eval("<分岐条件>") ? SqlExpr.Eval("<true時の条件>") : falseValue);
            var sql33 = Select<T>(t => SqlExpr.Eval("<分岐条件>") ? SqlExpr.Eval("<true時の条件>") : SqlExpr.Eval("<false時の条件>"));
            Assert.AreEqual(" where " + QueryBuilder.DefaultInstance.TrueLiteral, sql11);
            Assert.AreEqual(" where <分岐条件>", sql12);
            Assert.AreEqual(" where (<分岐条件> or <false時の条件>)", sql13);
            Assert.AreEqual(" where not(<分岐条件>)", sql21);
            Assert.AreEqual(" where " + QueryBuilder.DefaultInstance.FalseLiteral, sql22);
            Assert.AreEqual(" where not(<分岐条件>) and <false時の条件>", sql23);
            Assert.AreEqual(" where (not(<分岐条件>) or <true時の条件>)", sql31);
            Assert.AreEqual(" where <分岐条件> and <true時の条件>", sql32);
            Assert.AreEqual(" where ((<分岐条件> and <true時の条件>) or (not(<分岐条件>) and <false時の条件>))", sql33);
        }
    }
}
