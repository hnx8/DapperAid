using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using DapperAid.Helpers;

namespace DapperAid
{
    partial class QueryBuilder
    {
        /// <summary>
        /// Oracle用のSQL/パラメータを組み立てるクラスです。
        /// </summary>
        public class Oracle : QueryBuilder
        {
            /// <summary>バインドパラメータの頭に付加する文字(Oracleは「:」を使用)</summary>
            public override char ParameterMarker { get { return ':'; } }

            /// <summary>引数の値がnullに相当する値であればtrueを返します。Oracleは長さゼロの文字列もnullとみなします。</summary>
            public override bool IsNull(object? value)
            {
                return (base.IsNull(value) || (value is string s && string.IsNullOrEmpty(s)));
            }

            /// <summary>INSERT実行時の自動連番値を取得するSQL句として、outパラメータへのreturning句を付加します。</summary>
            protected override string GetInsertedIdReturningSql<T>(TableInfo.Column column)
            {
                return " returning " + column.Name + " into " + ParameterMarker + column.PropertyInfo.Name;
            }

            /// <summary>
            /// Oracle向けの一括Insert用SQLを生成します。
            /// </summary>
            /// <param name="records">挿入するレコード（複数件）</param>
            /// <param name="targetColumns">値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
            /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
            /// <returns>「insert all into [テーブル]([各カラム]) values ([各設定値]) ...」のSQL。一度に挿入する行数がMultiInsertRowsPerQueryを超過しないよう分割して返されます</returns>
            /// <remarks>Oracleの一括InsertはSQL-92の構文ではなくOracle固有のinsert-all構文を使用</remarks>
            public override IEnumerable<string> BuildMultiInsert<T>(IEnumerable<T> records, Expression<Func<T, dynamic>>? targetColumns = null)
            {
                var tableInfo = GetTableInfo<T>();
                var columns = (targetColumns == null
                    ? tableInfo.Columns.Where(c => c.Insert)
                    : tableInfo.GetColumns(ExpressionHelper.GetMemberNames(targetColumns.Body))).ToArray();
                var columnNames = string.Join(", ", columns.Select(c => c.Name));

                var sb = new StringBuilder();
                var rowCount = 0;
                foreach (var record in records)
                {
                    var values = string.Join(", ", columns.Select(column =>
                        (targetColumns != null || string.IsNullOrWhiteSpace(column.InsertSQL))
                            ? ToSqlLiteral(MemberAccessor.GetValue(record, column.PropertyInfo))
                            : column.InsertSQL)
                        );

                    if (sb.Length == 0)
                    {
                        sb.AppendLine("insert all");
                    }
                    sb.AppendLine(" into " + tableInfo.Name + "(" + columnNames + ") values" + "(" + values + ")");
                    rowCount++;
                    if (rowCount >= MultiInsertRowsPerQuery)
                    {
                        yield return sb.ToString() + " select null from dual";
                        sb.Clear();
                        rowCount = 0;
                    }
                }
                if (sb.Length > 0)
                {
                    yield return sb.ToString() + " select null from dual";
                }
            }

            /// <summary>
            /// Upsert(merge)のusing句を生成します。
            /// </summary>
            /// <param name="columns">recordsから挿入時の値を取り出すべきカラム</param>
            /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
            /// <returns>Oracleでは「using (select @パラメータ名 as カラム名, ... from dual) as s」のようなusing句</returns>
            protected override string BuildUpsertUsingClause<T>(IReadOnlyList<TableInfo.Column> columns)
            {
                return "using("
                    + Environment.NewLine
                    + "select "
                    + string.Join(",", columns.Select(column => (ParameterMarker + column.PropertyInfo.Name + " as " + column.Name)))
                    + " from dual"
                    + Environment.NewLine
                    + ") as s";
            }
            /// <summary>
            /// 一括Upsert(merge)のusing句を生成します。
            /// </summary>
            /// <param name="records">挿入または更新するレコード（複数件）</param>
            /// <param name="columns">recordsから挿入時の値を取り出すべきカラム</param>
            /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
            /// <returns>Oracleでは「using (select 値, 値, ... from dual union select ....) as s」のような静的using句</returns>
            protected override IEnumerable<string> BuildMultiUpsertUsingClause<T>(IEnumerable<T> records, IReadOnlyList<TableInfo.Column> columns)
            {
                var postfix = ") as s";

                var sb = new StringBuilder();
                var rowCount = 0;

                foreach (var record in records)
                {
                    sb.Append(sb.Length == 0
                        ? "using (" + Environment.NewLine + " select "
                        : " union all select ");
                    var values = string.Join(",", columns.Select(
                        column => ToSqlLiteral(MemberAccessor.GetValue(record, column.PropertyInfo))
                                    + (rowCount == 0 ? " as " + column.Name : "") // 先頭行に限り列名も付与する
                        ));
                    sb.AppendLine(values + " from dual ");
                    rowCount++;
                    if (rowCount >= MultiInsertRowsPerQuery)
                    {
                        yield return sb.ToString() + postfix;
                        sb.Clear();
                        rowCount = 0;
                    }
                }
                if (sb.Length > 0)
                {
                    yield return sb.ToString() + postfix;
                }
            }

            /// <summary>パラメータ値上限1000件を考慮してIn条件式を組み立てます(Oracleのみの特殊対処)</summary>
            /// <remarks>パラメータ値が1000件超の場合は「([COL] Like :P00)or([COL] Like :P01)...」といった条件式が返される。パラメータ値は1000件単位でバインドされる</remarks>
            public override string BuildWhereIn(Dapper.DynamicParameters parameters, TableInfo.Column column, bool opIsNot, object values)
            {
                var allValues = (values as System.Collections.IEnumerable)?.Cast<object>().ToArray();
                if (allValues?.Length > 1000)
                {   // in条件のパラメータ値が1000件超の場合、特殊組み立て
                    var sb = new StringBuilder();
                    var delimiter = "(";
                    for (var i = 0; i < allValues.Length; i += 1000)
                    {
                        var count = Math.Min(1000, allValues.Length - i);
                        sb.Append(delimiter);
                        sb.Append(base.BuildWhereIn(parameters, column, opIsNot, new ArraySegment<object>(allValues, i, count)));
                        delimiter = ")or(";
                    }
                    sb.Append(")");
                    return sb.ToString();
                }
                // 1000件以下なら通常通り組み立て
                return base.BuildWhereIn(parameters, column, opIsNot, values);
            }
        }
    }
}
