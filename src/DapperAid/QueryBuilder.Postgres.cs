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
        /// PostgreSQL用のSQL/パラメータを組み立てるクラスです。
        /// </summary>
        public class Postgres : QueryBuilder
        {
            /// <summary>Postgres/SQLiteにおけるTRUEを表すSQLリテラル表記です。</summary>
            public override string TrueLiteral { get { return "'1'"; } }

            /// <summary>Postgres/SQLiteにおけるFalseを表すSQLリテラル表記です。</summary>
            public override string FalseLiteral { get { return "'0'"; } }

            /// <summary>
            /// 引数で指定されたblob値をPostgresにおけるSQLリテラル値表記へと変換します。
            /// </summary>
            /// <param name="value">値</param>
            /// <returns>SQLリテラル値表記</returns>
            /// <remarks>DBMSによりリテラル値表記が異なります。</remarks>
            public override string ToSqlLiteral(byte[] value)
            {
                return @"'\x" + string.Concat(value.Select(b => $"{b:X2}")) + "'";
            }

            /// <summary>自動連番値を取得するSQL句として、returning句を付加します。</summary>
            protected override string GetInsertedIdReturningSql<T>(TableInfo.Column column)
            {
                return " returning " + column.Name;
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

            /// <summary>
            /// Where条件のIn条件式について、Postgresでは「[カラム]=any([配列バインド値])」として組み立てます。
            /// </summary>
            public override string BuildWhereIn(Dapper.DynamicParameters parameters, TableInfo.Column column, bool opIsNot, object values)
            {
                return column.Name + (opIsNot ? "<>" : "=") + "any(" + AddParameter(parameters, column.PropertyInfo.Name, values) + ")";
                // ※postgresの場合はinと同じ結果が得られるany演算子で代替する（配列パラメータサポートの副作用でin条件のパラメータが展開されず、SQLも展開されないため）
            }
        }
    }
}
