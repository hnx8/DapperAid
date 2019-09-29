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
            /// <summary>SQLiteにおけるTRUEを表すSQLリテラル表記です。</summary>
            public override string TrueLiteral { get { return "1"; } }

            /// <summary>SQLiteにおけるFalseを表すSQLリテラル表記です。</summary>
            public override string FalseLiteral { get { return "0"; } }

            /// <summary>
            /// 引数で指定された日付値をSQLiteにおけるSQLリテラル値表記へと変換します。
            /// </summary>
            /// <param name="value">値</param>
            /// <returns>SQLリテラル値表記</returns>
            public override string ToSqlLiteral(DateTime value)
            {
                // SQLiteはdatetime関数でキャスト
                return "datetime('" + value.ToString("yyyy-MM-dd HH:mm:ss.fff") + "')";
            }

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
            /// 標準的な一括InsertのSQLを用いて、指定されたレコードを一括挿入します。
            /// </summary>
            public override int InsertRows<T>(IEnumerable<T> data, Expression<System.Func<T, dynamic>> targetColumns, IDbConnection connection, IDbTransaction transaction, int? timeout = null)
            {
                var ret = 0;
                foreach (var sql in BulkInsertHelper.BuildBulkInsert(this, data, targetColumns, base.ToSqlLiteral))
                {
                    ret += connection.Execute(sql, null, transaction, timeout);
                }
                return ret;
            }
        }
    }
}
