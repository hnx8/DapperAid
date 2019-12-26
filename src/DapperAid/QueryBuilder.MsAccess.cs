using System;
using DapperAid.Helpers;

namespace DapperAid
{
    partial class QueryBuilder
    {
        /// <summary>
        /// Microsoft Access用(SqlCEも流用可)のSQL/パラメータを組み立てるクラスです。
        /// </summary>
        public class MsAccess : QueryBuilder
        {
            /// <summary>MSAccessでは一括Insertに対応していないため、SQLクエリ１回での挿入行数は１固定・変更不可となります。</summary>
            public override int MultiInsertRowsPerQuery
            {
                get { return 1; }
                set { }
            }

            /// <summary>MSAccessではUpsertに対応していないためfalse固定となります。</summary>
            public override bool SupportsUpsert
            {
                get { return false; }
                set { }
            }

            /// <summary>SQL識別子（テーブル名/カラム名等）をエスケープします。MsAccessでは"[","]"を使用します。</summary>
            public override string EscapeIdentifier(string identifier)
            {
                return "[" + identifier + "]";
            }

            /// <summary>
            /// 引数で指定された日付値をMicrosoft AccessにおけるSQLリテラル値表記へと変換します。
            /// </summary>
            /// <param name="value">値</param>
            /// <returns>SQLリテラル値表記</returns>
            public override string ToSqlLiteral(DateTime value)
            {
                // Microsoft Accessはミリ秒以下未対応
                return "#" + value.ToString("yyyy-MM-dd HH:mm:ss") + "#";
            }

            /// <summary>自動連番値を取得するSQL句として、セミコロンで区切った別のSQL文を付加します。</summary>
            protected override string GetInsertedIdReturningSql<T>(TableInfo.Column column)
            {
                return "; select @@IDENTITY";
            }

            /// <summary>MsAccessはTruncate使用不可のため、代替としてDeleteSQLを返します。</summary>
            public override string BuildTruncate<T>()
            {
                return base.BuildDelete<T>();
            }
        }
    }
}
