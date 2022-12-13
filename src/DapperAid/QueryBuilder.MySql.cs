using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
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

            /// <summary>
            /// インスタンスを初期化します。
            /// </summary>
            /// <param name="isAnsiMode">未使用の引数です</param>
            /// <param name="sqlMaxLength">未使用の引数です</param>
            [Obsolete("ソースコード改善により引数付きコンストラクタは使用されなくなりました")]
            public MySql(bool isAnsiMode = true, int sqlMaxLength = 16000000)
                : this()
            {
                // 仕様変更により初期化処理消滅
            }

            /// <summary>SQL識別子（テーブル名/カラム名等）をエスケープします。MySQL系では「`]を使用します。</summary>
            public override string EscapeIdentifier(string identifier)
            {
                return "`" + identifier.Replace("`", "``") + "`";
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
            /// 指定された型のテーブルに対するUPSERT SQLを生成します。(既存レコードはUPDATE／未存在ならINSERTを行います)
            /// </summary>
            /// <param name="insertTargetColumns">insert実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2, t.col3 }</c>」</param>
            /// <param name="updateTargetColumns">update実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col2, t.Col3 }</c>」</param>
            /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
            /// <returns>MySQLでは「insert into .... on duplicate key update ....」のSQL</returns>
            public override string BuildUpsert<T>(Expression<Func<T, dynamic>>? insertTargetColumns = null, Expression<Func<T, dynamic>>? updateTargetColumns = null)
            {
                // on duplicate句を生成。insertSQLの末尾に付加する
                var postfix = BuildUpsertUpdateClause(" on duplicate key update", "values(?)", updateTargetColumns);
                return BuildInsert<T>(insertTargetColumns) + Environment.NewLine + postfix;
            }
            /// <summary>
            /// 一括Upsert用SQLを生成します。(既存レコードはUPDATE／未存在ならINSERTを行います)
            /// </summary>
            /// <param name="records">挿入または更新するレコード（複数件）</param>
            /// <param name="insertTargetColumns">insert実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2, t.col3 }</c>」</param>
            /// <param name="updateTargetColumns">update実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col2, t.Col3 }</c>」</param>
            /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
            /// <returns>MySQLでは「insert into .... on duplicate key update ....」の静的SQL。一度に挿入する行数がMultiInsertRowsPerQueryを超過しないよう分割して返されます</returns>
            public override IEnumerable<string> BuildMultiUpsert<T>(IEnumerable<T> records, Expression<Func<T, dynamic>>? insertTargetColumns = null, Expression<Func<T, dynamic>>? updateTargetColumns = null)
            {
                // on duplicate句を生成。一括insertSQLの末尾に付加する
                var postfix = BuildUpsertUpdateClause(" on duplicate key update", "values(?)", updateTargetColumns);
                foreach (var sql in this.BuildMultiInsert(records, insertTargetColumns))
                {
                    yield return sql + Environment.NewLine + postfix;
                }
            }
        }
    }
}
