using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using Dapper;
using DapperAid.Helpers;

namespace DapperAid
{
    partial class QueryBuilder
    {
        /// <summary>
        /// PostgreSQL用のSQL/パラメータを組み立てるクラスです。
        /// </summary>
        public class Postgres : QueryBuilder
        {
            /// <summary>自動連番値を取得するSQL句として、returning句を付加します。</summary>
            protected override string GetInsertedIdReturningSql<T>(TableInfo.Column column)
            {
                return " returning " + column.Name;
            }

            /// <summary>
            /// Where条件のIn条件式について、Postgresでは「[カラム]=any([配列バインド値])」として組み立てます。
            /// </summary>
            protected override string BuildWhereIn(Dapper.DynamicParameters parameters, TableInfo.Column column, bool opIsNot, object values)
            {
                return column.Name + (opIsNot ? "<>" : "=") + "any(" + AddParameter(parameters, column.PropertyInfo.Name, values) + ")";
                // ※postgresの場合はinと同じ結果が得られるany演算子で代替する（配列パラメータサポートの副作用でin条件のパラメータが展開されず、SQLも展開されないため）
            }

            /// <summary>
            /// 標準的な一括InsertのSQLを用いて、指定されたレコードを一括挿入します。
            /// </summary>
            public override int InsertRows<T>(IEnumerable<T> data, Expression<System.Func<T, dynamic>> targetColumns, IDbConnection connection, IDbTransaction transaction, int? timeout = null)
            {
                var ret = 0;
                foreach (var sql in BulkInsertHelper.BuildBulkInsert(this, data, targetColumns, Value2SqlLiteral))
                {
                    ret += connection.Execute(sql, null, transaction, timeout);
                }
                return ret;
            }

            /// <summary>
            /// 引数で指定された値をPostgreSQLにおけるSQLリテラル値表記へと変換します（主に一括Insert用）
            /// </summary>
            /// <param name="value">エスケープ前の値</param>
            /// <returns>SQLリテラル値表記</returns>
            public static string Value2SqlLiteral(object value)
            {
                if (value == null || value is System.DBNull) { return "null"; }
                if (value is string) { return "'" + (value as string).Replace("'", "''") + "'"; }
                if (value is bool) { return ((bool)value ? "TRUE" : "FALSE"); }
                if (value is DateTime) { return "timestamp '" + ((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss") + "'"; }
                if (value is Enum) { return ((Enum)value).ToString("d"); }
                return value.ToString();
            }
        }
    }
}
