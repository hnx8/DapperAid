using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Linq.Expressions;
using System.Text;
using Dapper;
using DapperAid.Helpers;

namespace DapperAid
{
    /// <summary>
    /// DapperAidのSQL条件式記述用メソッド定義です。
    /// </summary>
    /// <remarks>式木ではなく実際のメソッド呼び出しなどで使用した場合、例外が投げられます。</remarks>
    public class SqlExpr : ISqlExpr
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
        /// <typeparam name="T">パラメータバインドするBETWEEN条件値の型</typeparam>
        /// <param name="value1">パラメータバインドする下限値</param>
        /// <param name="value2">パラメータバインドする上限値</param>
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
        /// <typeparam name="T">パラメータバインドするIN条件値の型</typeparam>
        /// <param name="values">パラメータバインドするIN条件値の配列</param>
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
        /// <typeparam name="T">IN条件値の型</typeparam>
        /// <param name="sqlSubQuery">SQLサブクエリ</param>
        public static T In<T>(string sqlSubQuery)
        {
            throw new InvalidOperationException("It can be used only for DapperAid's where-condition");
        }

        /// <summary>
        /// Where条件式の式木（ラムダ）で値として用いた場合、引数で指定されたサブクエリ文字列およびバインド値によりIN条件式が生成されます。
        /// <example>
        /// 使用例：<c>con.Select&lt;T_USER&gt;(t => t.Col1 == ToSql.In&lt;string&gt;("select x from ....")); </c>→SQLは「where "Col1" in(select x from....)」などとなります。
        /// </example>
        /// </summary>
        /// <typeparam name="T">IN条件値の型</typeparam>
        /// <param name="sqlSubQuery">SQLサブクエリ</param>
        /// <param name="bindvalueAndSubsequentsqlAsAlternately">バインドする値と後続のSQLを交互に記述</param>
        public static T In<T>(string sqlSubQuery, params object[] bindvalueAndSubsequentsqlAsAlternately)
        {
            throw new InvalidOperationException("It can be used only for DapperAid's where-condition");
        }

        /// <summary>
        /// Where条件式の式木（ラムダ）で条件式として用いた場合、引数で指定されたSQL条件式文字列がそのままWhere条件に追加されます。
        /// <example>
        /// 使用例：<c>con.Select&lt;T_USER&gt;(t => Eval("rowid=xxx")); </c>→SQLは「where rowid=xxx」などとなります。
        /// </example>
        /// </summary>
        /// <param name="sqlExpression">SQL条件式</param>
        public static bool Eval(string sqlExpression)
        {
            throw new InvalidOperationException("It can be used only for DapperAid's where-condition");
        }

        /// <summary>
        /// Where条件式の式木（ラムダ）で条件式として用いた場合、引数で指定されたSQL文字列およびバインド値がWhere条件として追加されます。
        /// <example>
        /// 使用例：<c>con.Select&lt;T_USER&gt;(t => Eval("id=", idText, "AND pw=CRYPT(", pwText, ", pw)")); </c>
        /// <para>→SQLは「where id= @P01 AND pw=CRYPT( @P02 , pw)」などとなり、変数idText,pwTextの保持値がパラメータバインドされます。</para>
        /// </example>
        /// </summary>
        /// <param name="sqlExpression">SQL条件式</param>
        /// <param name="bindvalueAndSubsequentsqlAsAlternately">バインドする値と後続のSQLを交互に記述</param>
        public static bool Eval(string sqlExpression, params object[] bindvalueAndSubsequentsqlAsAlternately)
        {
            throw new InvalidOperationException("It can be used only for DapperAid's where-condition");
        }

        /// <summary>
        /// Where条件式の式木（ラムダ）で値として用いた場合、引数で指定されたSQL文字列およびバインド値が条件式として追加されます。
        /// <example>
        /// 使用例1：<c>con.Select&lt;T_USER&gt;(t => pw == Eval&lt;string&gt;("CRYPT(", pwText, ", pw)")); </c>
        /// <para>→SQLは「where pw=CRYPT( @P01, pw)」などとなり、変数pwTextの保持値がパラメータバインドされます。</para>
        /// </example>
        /// <example>
        /// 使用例2：<c>con.Select&lt;T_USER&gt;(t => pw == Eval&lt;string&gt;("CRYPT(", pwText, ",", salt, ")")); </c>
        /// <para>→SQLは「where pw=CRYPT( @P01 , @P02 ))」などとなり、変数pwText,saltの保持値がパラメータバインドされます。</para>
        /// </example>
        /// </summary>
        /// <param name="sqlExpression">SQL条件式</param>
        /// <param name="bindvalueAndSubsequentsqlAsAlternately">バインドする値と後続のSQLを交互に記述</param>
        public static T Eval<T>(string sqlExpression, params object[] bindvalueAndSubsequentsqlAsAlternately)
        {
            throw new InvalidOperationException("It can be used only for DapperAid's where-condition");
        }



        #region SQL条件式生成処理の実体 --------------------------------------------------
        /// <summary>※外部からのインスタンス化不可</summary>
        protected SqlExpr() { }

        /// <summary>
        /// ※QueryBuilderからの呼び出し用：式木として記述されているメソッド・引数にもとづき、SQL条件式を生成しパラメータをバインドします
        /// </summary>
        /// <param name="methodName">式木のメソッド名</param>
        /// <param name="arguments">式木に指定された引数</param>
        /// <param name="builder">QueryBuilderオブジェクト</param>
        /// <param name="parameters">パラメータオブジェクト</param>
        /// <param name="column">条件式生成対象カラム（nullの場合は生成されるSQL自体が真偽値を返す）</param>
        /// <param name="opIsNot">条件式をnotで生成する場合はtrue</param>
        /// <returns>生成されたSQL条件式</returns>
        public string BuildSql(string methodName, ReadOnlyCollection<Expression> arguments, QueryBuilder builder, DynamicParameters parameters, TableInfo.Column? column, bool opIsNot)
        {
            if (methodName == nameof(Like) && column != null)
            {   // ToSql.Like(string)： Like演算子を組み立てる
                var value = ExpressionHelper.EvaluateValue(arguments[0]) ?? throw new ArgumentException($"{nameof(SqlExpr)}.{nameof(Like)}(): `pattern` must not be null.");
                return column.Name + (opIsNot ? " not" : "") + " like " + builder.AddParameter(parameters, column.PropertyInfo.Name, value);
            }
            else if (methodName == nameof(In) && column != null)
            {
                var value = ExpressionHelper.EvaluateValue(arguments[0]) ?? throw new ArgumentNullException($"{nameof(SqlExpr)}.{nameof(In)}(): Argument must not be null.");
                if (arguments[0].Type == typeof(string))
                {   // ToSql.In(string)： INサブクエリとして指定された文字列を直接埋め込む
                    var sb = new StringBuilder(column.Name + (opIsNot ? " not" : "") + " in(" + value);
                    // object[] に相当する部分が指定されていればその部分も組み立てる
                    var bindvalueAndSubsequentsql = (arguments.Count == 2 ? ExpressionHelper.EvaluateValue(arguments[1]) as object[] : null);
                    for (var i = 0; bindvalueAndSubsequentsql != null && i < bindvalueAndSubsequentsql.Length; i++)
                    {
                        var value2 = bindvalueAndSubsequentsql[i];
                        if (i % 2 == 0)
                        {   // paramsとしては偶数（メソッド引数としては2,4,6,8,…,2n番目）：指定されている値をバインド
                            sb.Append(builder.AddParameter(parameters, null, value2));
                        }
                        else if (bindvalueAndSubsequentsql[i] is string)
                        {   // paramsとしては奇数（メソッド引数としては3,5,7,9,…,2n+1番目）：指定されている文字列をSQLとみなして付加
                            sb.Append(value2);
                        }
                        else
                        {   // SQLリテラルを指定すべき箇所で誤って文字列以外が指定されている：例外をthrow
                            var badParamName = "argument" + (i + 1);
                            throw new ArgumentException(badParamName + "(" + (value2 ?? "null") + "): No SQL statemnt specified.", badParamName);
                        }
                    }
                    sb.Append(")");
                    return sb.ToString();
                }
                else
                {   // ToSql.In(コレクション)： In演算子を組み立てる
                    return builder.BuildWhereIn(parameters, column, opIsNot, value);
                }
            }
            else if (methodName == nameof(Between) && column != null)
            {    // ToSql.Between(値1, 値2)： Between演算子を組み立て、パラメータを２つバインドする。nullの可能性は考慮しない
                var value1 = ExpressionHelper.EvaluateValue(arguments[0]) ?? throw new ArgumentException($"{nameof(Between)}.{nameof(Like)}(): `value1` must not be null."); ;
                var value2 = ExpressionHelper.EvaluateValue(arguments[1]) ?? throw new ArgumentException($"{nameof(Between)}.{nameof(Like)}(): `value2` must not be null."); ;
                return column.Name + (opIsNot ? " not" : "") + " between "
                    + builder.AddParameter(parameters, column.PropertyInfo.Name, value1)
                    + " and "
                    + builder.AddParameter(parameters, column.PropertyInfo.Name, value2);
            }
            else if (methodName == nameof(Eval))
            {   // ToSql.Eval(string, [object[]])：指定されたSQL文字列を直接埋め込む
                var sb = new StringBuilder(
                    (column == null ? (opIsNot ? "not " : "") : (column.Name + (opIsNot ? "<>" : "="))) + ExpressionHelper.EvaluateValue(arguments[0])
                    );
                // object[] に相当する部分が指定されていればその部分も組み立てる
                var bindvalueAndSubsequentsql = (arguments.Count == 2 ? ExpressionHelper.EvaluateValue(arguments[1]) as object[] : null);
                for (var i = 0; bindvalueAndSubsequentsql != null && i < bindvalueAndSubsequentsql.Length; i++)
                {
                    var value = bindvalueAndSubsequentsql[i];
                    if (i % 2 == 0)
                    {   // paramsとしては偶数（メソッド引数としては2,4,6,8,…,2n番目）：指定されている値をバインド
                        sb.Append(builder.AddParameter(parameters, null, value));
                    }
                    else if (bindvalueAndSubsequentsql[i] is string)
                    {   // paramsとしては奇数（メソッド引数としては3,5,7,9,…,2n+1番目）：指定されている文字列をSQLとみなして付加
                        sb.Append(value);
                    }
                    else
                    {   // SQLリテラルを指定すべき箇所で誤って文字列以外が指定されている：例外をthrow
                        var badParamName = "argument" + (i + 1);
                        throw new ArgumentException(badParamName + "(" + (value ?? "null") + "): No SQL statemnt specified.", badParamName);
                    }
                }
                return sb.ToString();
            }
            else
            {
                throw new InvalidExpressionException(methodName);
            }
        }
        #endregion
    }

    /// <summary>
    /// DapperAidのSQL条件式記述用メソッド定義クラスを表すインターフェースです。
    /// </summary>
    interface ISqlExpr
    {
        // カスタムのSQL条件式記述用メソッドを定義する場合は、このインターフェースを実装し以下の条件を満たすよう処理を記述してください。
        // 1. private等の引数無しコンストラクタ：外部から（リフレクションを使わず）インスタンス化できないようにする。
        // 2. buildSqlメソッド：呼び出されたメソッド・引数に基づきSQLステートメントを適切に生成する。（SqlExpr.csのソースコードを参考にしてください）

        /// <summary>
        /// ※QueryBuilderからの呼び出し用：式木として記述されているメソッド・引数にもとづき、SQL条件式を生成しパラメータをバインドします
        /// </summary>
        /// <param name="methodName">式木のメソッド名</param>
        /// <param name="arguments">式木に指定された引数</param>
        /// <param name="builder">QueryBuilderオブジェクト</param>
        /// <param name="parameters">パラメータオブジェクト</param>
        /// <param name="column">条件式生成対象カラム（nullの場合は生成されるSQL自体が真偽値を返す）</param>
        /// <param name="opIsNot">条件式をnotで生成する場合はtrue</param>
        /// <returns>生成されたSQL条件式</returns>
        string BuildSql(string methodName, ReadOnlyCollection<Expression> arguments, QueryBuilder builder, DynamicParameters parameters, TableInfo.Column? column, bool opIsNot);
    }
}
