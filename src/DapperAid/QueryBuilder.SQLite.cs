using System;
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
        }
    }
}
