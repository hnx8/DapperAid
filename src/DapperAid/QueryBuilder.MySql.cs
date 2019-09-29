using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Text;
using Dapper;
using DapperAid.Helpers;

namespace DapperAid
{
    partial class QueryBuilder
    {
        /// <summary>
        /// MySQL系のSQL/パラメータを組み立てるクラスです。
        /// </summary>
        public class MySql : QueryBuilder
        {
            /// <summary>
            /// 文字列リテラルのエスケープにバックスラッシュを使用しない設定であれば、trueを指定します。
            /// デフォルトはfalseです。
            /// </summary>
            public bool NO_BACKSLASH_ESCAPES { get; set; }

            /// <summary>
            /// インスタンスを初期化します。
            /// </summary>
            public MySql()
            {
                NO_BACKSLASH_ESCAPES = false;
            }

            /// <summary>テーブル名/カラム名のエスケープに使用する文字</summary>
            protected readonly string EscapeMark;

            /// <summary>一括Insert等のSQLの最大文字列長</summary>
            protected readonly int SqlMaxLength;

            /// <summary>
            /// インスタンスを初期化します。
            /// </summary>
            /// <param name="isAnsiMode">ANSI_MODEではない場合、明示的にfalseを指定</param>
            /// <param name="sqlMaxLength">一括InsertのSQLの最大文字列長、既定では16MB。大量データの一括Insertを行う際はmax_allowed_packetの指定に応じた値を設定</param>
            public MySql(bool isAnsiMode = true, int sqlMaxLength = 16000000)
                : this()
            {
                EscapeMark = (isAnsiMode ? "\"" : "`");
                SqlMaxLength = sqlMaxLength;
            }

            /// <summary>SQL識別子（テーブル名/カラム名等）をエスケープします。MySQL系では「"」または「`]を使用します。</summary>
            public override string EscapeIdentifier(string identifier)
            {
                return EscapeMark + identifier.Replace(EscapeMark, EscapeMark + EscapeMark) + EscapeMark;
            }

            /// <summary>
            /// 引数で指定された文字列値をMySQL系におけるSQLリテラル値表記へと変換します。
            /// </summary>
            /// <param name="value">値</param>
            /// <returns>SQLリテラル値表記</returns>
            public override string ToSqlLiteral(string value)
            {
                if (IsNull(value)) { return "null"; }

                // NO_BACKSLASH_ESCAPES を on にしている場合は通常のエスケープ
                if (NO_BACKSLASH_ESCAPES) { return base.ToSqlLiteral(value); }

                // MySQLの仕様に基づき文字列をエスケープ
                var sb = new StringBuilder();
                sb.Append("'");
                foreach (var ch in value)
                {
                    switch (ch)
                    {
                        case '\u0000': sb.Append(@"\0"); break;
                        case '\'': sb.Append(@"\'"); break;
                        case '\"': sb.Append(@"\" + "\""); break;
                        case '\b': sb.Append(@"\b"); break;
                        case '\n': sb.Append(@"\n"); break;
                        case '\r': sb.Append(@"\r"); break;
                        case '\t': sb.Append(@"\t"); break;
                        case '\u001A': sb.Append(@"\z"); break;
                        case '\\': sb.Append(@"\\"); break;
                        default: sb.Append(ch); break;
                    }
                }
                sb.Append("'");
                return sb.ToString();
            }

            /// <summary>
            /// 引数で指定された日付値をMySQL系におけるSQLリテラル値表記へと変換します。
            /// </summary>
            /// <param name="value">値</param>
            /// <returns>SQLリテラル値表記</returns>
            public override string ToSqlLiteral(DateTime value)
            {
                // DATETIME/TIMESTAMP両方に対応させる意図であえて型を明示しない文字列表記とする
                return "'" + value.ToString("yyyy-MM-dd HH:mm:ss.ffffff") + "'";
            }

            /// <summary>自動連番値を取得するSQL句として、セミコロンで区切った別のSQL文を付加します。</summary>
            protected override string GetInsertedIdReturningSql<T>(TableInfo.Column column)
            {
                return "; select LAST_INSERT_ID()";
            }

            /// <summary>
            /// 標準的な一括InsertのSQLを用いて、指定されたレコードを一括挿入します。
            /// </summary>
            public override int InsertRows<T>(IEnumerable<T> data, Expression<System.Func<T, dynamic>> targetColumns, IDbConnection connection, IDbTransaction transaction, int? timeout = null)
            {
                var ret = 0;
                foreach (var sql in BulkInsertHelper.BuildBulkInsert(this, data, targetColumns, base.ToSqlLiteral, 999999))
                {
                    ret += connection.Execute(sql, null, transaction, timeout);
                }
                return ret;
            }
        }
    }
}
