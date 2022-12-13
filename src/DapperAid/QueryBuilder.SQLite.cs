using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
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

            /// <summary>
            /// 指定された型のテーブルに対するUPSERT SQLを生成します。(既存レコードはUPDATE／未存在ならINSERTを行います)
            /// </summary>
            /// <param name="insertTargetColumns">insert実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2, t.col3 }</c>」</param>
            /// <param name="updateTargetColumns">update実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col2, t.Col3 }</c>」</param>
            /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
            /// <returns>Postgres/SQLiteでは「insert into ..... on conflict(PK) do update set ...」のSQL</returns>
            public override string BuildUpsert<T>(Expression<Func<T, dynamic>>? insertTargetColumns = null, Expression<Func<T, dynamic>>? updateTargetColumns = null)
            {
                var tableInfo = GetTableInfo<T>();
                var keys = string.Join(", ", tableInfo.Columns.Where(c => c.IsKey).Select(c => c.Name));
                var postfix = (string.IsNullOrEmpty(keys))
                    ? ""
                    : BuildUpsertUpdateClause(" on conflict(" + keys + ") do update set", "excluded.?", updateTargetColumns);
                return BuildInsert<T>(insertTargetColumns) + Environment.NewLine + postfix;
            }
            /// <summary>
            /// 一括Upsert用SQLを生成します。(既存レコードはUPDATE／未存在ならINSERTを行います)
            /// </summary>
            /// <param name="records">挿入または更新するレコード（複数件）</param>
            /// <param name="insertTargetColumns">insert実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2, t.col3 }</c>」</param>
            /// <param name="updateTargetColumns">update実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col2, t.Col3 }</c>」</param>
            /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
            /// <returns>Postgres/SQLiteでは「insert into ..... on conflict(PK) do update set ...」の静的SQL。一度に挿入する行数がMultiInsertRowsPerQueryを超過しないよう分割して返されます</returns>
            public override IEnumerable<string> BuildMultiUpsert<T>(IEnumerable<T> records, Expression<Func<T, dynamic>>? insertTargetColumns = null, Expression<Func<T, dynamic>>? updateTargetColumns = null)
            {
                var tableInfo = GetTableInfo<T>();
                // PKのカラム名をカンマ区切りに加工しon conflict句を生成。一括insertSQLの末尾に付加する
                var keys = string.Join(", ", tableInfo.Columns.Where(c => c.IsKey).Select(c => c.Name));
                var postfix = (string.IsNullOrEmpty(keys))
                    ? ""
                    : BuildUpsertUpdateClause(" on conflict(" + keys + ") do update set", "excluded.?", updateTargetColumns);
                foreach (var sql in this.BuildMultiInsert(records, insertTargetColumns))
                {
                    yield return sql + Environment.NewLine + postfix;
                }
            }

            /// <summary>SQLiteはTruncate使用不可のため、代替としてDeleteSQLを返します。</summary>
            public override string BuildTruncate<T>()
            {
                return base.BuildDelete<T>();
            }
        }
    }
}
