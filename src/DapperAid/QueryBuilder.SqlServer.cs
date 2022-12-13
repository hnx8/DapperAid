using System;
using System.Collections.Generic;
using System.Linq.Expressions;
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

            /// <summary>
            /// SQLServer向けのUPSERT SQLを生成します。(既存レコードはUPDATE／未存在ならINSERTを行います)
            /// </summary>
            /// <param name="insertTargetColumns">insert実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2, t.Col3 }</c>」</param>
            /// <param name="updateTargetColumns">update実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col2, t.Col3 }</c>」</param>
            /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
            /// <returns>SQLServerでは文末セミコロン付きの「merge into TABLE using ... when matched then update ... when not matched then insert ...;」のUpsert用SQL</returns>
            public override string BuildUpsert<T>(Expression<Func<T, dynamic>>? insertTargetColumns = null, Expression<Func<T, dynamic>>? updateTargetColumns = null)
            {
                return base.BuildUpsert(insertTargetColumns, updateTargetColumns) + ";";  // SQLServerではmerge文のみ末尾のセミコロンが必須なので付加
            }

            /// <summary>
            /// SQLServer向けの一括Upsert用SQLを生成します。(既存レコードはUPDATE／未存在ならINSERTを行います)
            /// </summary>
            /// <param name="records">挿入または更新するレコード（複数件）</param>
            /// <param name="insertTargetColumns">insert実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2, t.Col3 }</c>」</param>
            /// <param name="updateTargetColumns">update実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col2, t.Col3 }</c>」</param>
            /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
            /// <returns>SQLServerでは文末セミコロン付きの「merge into TABLE using ... when matched then update ... when not matched then insert ...;」Upsert静的SQL。一度に挿入更新する行数がMultiInsertRowsPerQueryを超過しないよう分割して返されます</returns>
            public override IEnumerable<string> BuildMultiUpsert<T>(IEnumerable<T> records, Expression<Func<T, dynamic>>? insertTargetColumns = null, Expression<Func<T, dynamic>>? updateTargetColumns = null)
            {
                foreach (var sql in base.BuildMultiUpsert(records, insertTargetColumns, updateTargetColumns))
                {
                    yield return sql + ";"; // SQLServerではmerge文のみ末尾のセミコロンが必須なので付加
                }
            }
        }
    }
}
