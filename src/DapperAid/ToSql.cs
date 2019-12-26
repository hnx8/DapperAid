using System;
using System.Collections.Generic;

namespace DapperAid
{
    /// <summary>
    /// DapperAidのSQL条件式記述用メソッド定義です。
    /// </summary>
    /// <remarks>式木ではなく実際のメソッド呼び出しなどで使用した場合、例外が投げられます。</remarks>
    public static class ToSql
    {
        /// <summary>
        /// Where条件式の式木（ラムダ）で値として用いた場合、引数で指定されたパターンによりSQLのLIKE条件式が生成されます。
        /// <example>
        /// 使用例：<c>con.Select&lt;T_USER&gt;(t => t.Col1 == ToSql.Like("%abc%")); </c>→SQLは「where "Col1" like @Col1」などとなります。
        /// </example>
        /// </summary>
        /// <param name="pattern">パラメータバインドする検索文字列のパターン</param>
        public static string Like(string pattern)
        {
            throw new InvalidOperationException("It can be used only for DapperAid's where-condition");
        }

        /// <summary>
        /// Where条件式の式木（ラムダ）で値として用いた場合、引数で指定された２つの値によりBETWEEN条件式が生成されます。
        /// <example>
        /// 使用例：<c>con.Select&lt;T_USER&gt;(t => t.Col1 == ToSql.Between(val1, val2)); </c>→SQLは「where "Col1" between @P0 and @P01」などとなります。
        /// </example>
        /// </summary>
        /// <param name="value1">パラメータバインドする下限値</param>
        /// <param name="value2">パラメータバインドする上限値</param>
        /// <typeparam name="T">パラメータバインドするBETWEEN条件値の型</typeparam>
        public static T Between<T>(T value1, T value2)
        {
            throw new InvalidOperationException("It can be used only for DapperAid's where-condition");
        }

        /// <summary>
        /// Where条件式の式木（ラムダ）で値として用いた場合、引数で指定された値によりSQLのIN条件式が生成されます。
        /// <example>
        /// 使用例：<c>con.Select&lt;T_USER&gt;(t => t.Col1 == ToSql.In(values)); </c>→SQLは「where "Col1" in(@Col1)」などとなります。
        /// </example>
        /// </summary>
        /// <param name="values">パラメータバインドするIN条件値の配列</param>
        /// <typeparam name="T">パラメータバインドするIN条件値の型</typeparam>
        public static T In<T>(ICollection<T> values)
        {
            throw new InvalidOperationException("It can be used only for DapperAid's where-condition");
        }

        /// <summary>
        /// Where条件式の式木（ラムダ）で値として用いた場合、引数で指定されたサブクエリ文字列によりIN条件式が生成されます。
        /// <example>
        /// 使用例：<c>con.Select&lt;T_USER&gt;(t => t.Col1 == ToSql.In&lt;string&gt;("select x from ....")); </c>→SQLは「where "Col1" in(select x from....)」などとなります。
        /// </example>
        /// </summary>
        /// <param name="sqlSubQuery">SQLサブクエリ</param>
        /// <typeparam name="T">IN条件値の型</typeparam>
        public static T In<T>(string sqlSubQuery)
        {
            throw new InvalidOperationException("It can be used only for DapperAid's where-condition");
        }

        /// <summary>
        /// Where条件式の式木（ラムダ）で条件式として用いた場合、引数で指定されたSQL条件式文字列がそのままWhere条件に追加されます。
        /// <example>
        /// 使用例：<c>con.Select&lt;T_USER&gt;(t => Eval("rowid=xxx"))); </c>→SQLは「where rowid=xxx」などとなります。
        /// </example>
        /// </summary>
        /// <param name="sqlExpression">SQL条件式</param>
        public static bool Eval(string sqlExpression)
        {
            throw new InvalidOperationException("It can be used only for DapperAid's where-condition");
        }

        /// <summary>
        /// メソッド名定数定義
        /// </summary>
        internal class NameOf
        {
            // C#6.0未満の環境だとnameofが使用できないため、代替としてメソッド名を定数で定義しておく

            public const string Like = "Like";
            public const string Between = "Between";
            public const string In = "In";
            public const string Eval = "Eval";
        }
    }
}
