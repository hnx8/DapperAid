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

        void Select<T>(Expression<Func<T, bool>> where)
        {
            var parameters = new DynamicParameters();
            var sql = QueryBuilder.DefaultInstance.BuildWhere(parameters, where);
            Trace.WriteLine(sql);
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
    }
}
