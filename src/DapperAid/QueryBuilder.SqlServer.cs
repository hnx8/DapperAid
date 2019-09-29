using System;
using DapperAid.Helpers;

namespace DapperAid
{
    partial class QueryBuilder
    {
        /// <summary>
        /// SQLServer用のSQL/パラメータを組み立てるクラスです。
        /// </summary>
        public class SqlServer : QueryBuilder
        {
            /// <summary>
            /// 引数で指定された日付値をSqlServerにおけるSQLリテラル値表記へと変換します。
            /// </summary>
            /// <param name="value">値</param>
            /// <returns>SQLリテラル値表記</returns>
            public override string ToSqlLiteral(DateTime value)
            {
                // datetime型へキャスト
                return "CAST('" + value.ToString("yyyy-MM-dd HH:mm:ss.fff") + "' AS datetime)";
            }

            /// <summary>自動連番値を取得するSQL句として、セミコロンで区切った別のSQL文を付加します。</summary>
            protected override string GetInsertedIdReturningSql<T>(TableInfo.Column column)
            {
                return "; select SCOPE_IDENTITY()";
            }
        }
    }
}
