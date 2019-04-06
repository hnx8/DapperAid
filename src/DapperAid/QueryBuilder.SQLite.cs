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
        /// SQLite用のSQL/パラメータを組み立てるクラスです。
        /// </summary>
        public class SQLite : QueryBuilder
        {
            /// <summary>自動連番値を取得するSQL句として、セミコロンで区切った別のSQL文を付加します。</summary>
            protected override string GetInsertedIdReturningSql<T>(TableInfo.Column column)
            {
                return "; select LAST_INSERT_ROWID()";
            }

            /// <summary>SQLiteはTruncate使用不可のため、代替としてDeleteSQLを返します。</summary>
            public override string BuildTruncate<T>()
            {
                return base.BuildDelete<T>();
            }

            /// <summary>
            /// 標準的な一括InsertのSQLを用いて、指定されたテーブルにレコードを一括挿入します。
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
            /// 引数で指定された値をSQLiteにおけるSQLリテラル値表記へと変換します（主に一括Insert用）
            /// </summary>
            /// <param name="value">エスケープ前の値</param>
            /// <returns>SQLリテラル値表記</returns>
            public static string Value2SqlLiteral(object value)
            {
                if (value == null || value is System.DBNull) { return "null"; }
                if (value is string) { return "'" + (value as string).Replace("'", "''") + "'"; }
                if (value is bool) { return ((bool)value ? "1" : "0"); }
                if (value is DateTime) { return "'" + ((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss") + "'"; }
                if (value is Enum) { return ((Enum)value).ToString("d"); }
                return value.ToString();
            }
        }
    }
}
