using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace DapperAid.Helpers
{
    /// <summary>
    /// 一括インサート用SQL生成機能を提供します。
    /// </summary>
    public class BulkInsertHelper
    {
        /// <summary>
        /// 一括Insert用SQLを生成します。
        /// </summary>
        /// <param name="builder">DBMSに応じたQueryBuilderオブジェクト</param>
        /// <param name="records">挿入するレコード（複数件）</param>
        /// <param name="targetColumns">値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <param name="escapeMethod">値をSQLリテラル値へと変換するメソッド。DBMSに応じたエスケープ処理を行うよう実装すること</param>
        /// <param name="sqlMaxLength">一括InsertのSQLの最大文字列長。省略可</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「insert into [テーブル]([各カラム]) values ([各設定値]),([各設定値])...」のSQLについて最大文字列長を超過しないよう分割して返す</returns>
        public static IEnumerable<string> BuildBulkInsert<T>(QueryBuilder builder, IEnumerable<T> records, LambdaExpression targetColumns, Func<object, string> escapeMethod, int sqlMaxLength = 100000000)
        {
            var tableInfo = builder.GetTableInfo<T>();
            var columns = (targetColumns == null
                ? tableInfo.Columns.Where(c => c.Insert)
                : tableInfo.GetColumns(ExpressionHelper.GetMemberNames(targetColumns.Body))).ToArray();
            var names = new StringBuilder();
            foreach (var column in columns)
            {
                if (names.Length > 0) { names.Append(", "); }
                names.Append(column.Name);
            }

            var data = new StringBuilder();
            foreach (var record in records)
            {
                var values = new StringBuilder();
                foreach (var column in columns)
                {
                    values.Append(values.Length == 0 ? "(" : ", ");
                    values.Append(targetColumns != null || string.IsNullOrWhiteSpace(column.InsertSQL)
                        ? escapeMethod(MemberAccessor.GetValue(record, column.PropertyInfo))
                        : column.InsertSQL);
                }
                values.Append(")");

                if (data.Length + values.Length >= sqlMaxLength)
                {
                    yield return data.ToString();
                    data.Clear();
                }
                data.AppendLine(data.Length == 0
                    ? "insert into " + tableInfo.Name + "(" + names.ToString() + ") values"
                    : ",");
                data.Append(values.ToString());
            }
            if (data.Length > 0)
            {
                yield return data.ToString();
            }
        }
    }
}
