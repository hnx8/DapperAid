﻿using DapperAid.Helpers;

namespace DapperAid
{
    partial class QueryBuilder
    {
        /// <summary>
        /// PostgreSQL用のSQL/パラメータを組み立てるクラスです。
        /// </summary>
        public class Postgres : QueryBuilder
        {
            /// <summary>自動連番値を取得するSQL句として、returning句を付加します。</summary>
            protected override string GetInsertedIdReturningSql<T>(TableInfo.Column column)
            {
                return " returning " + column.Name;
            }

            /// <summary>
            /// Where条件のIn条件式について、Postgresでは「[カラム]=any([配列バインド値])」として組み立てます。
            /// </summary>
            protected override string BuildWhereIn(Dapper.DynamicParameters parameters, TableInfo.Column column, bool opIsNot, object values)
            {
                return column.Name + (opIsNot ? "<>" : "=") + "any(" + AddParameter(parameters, column.PropertyInfo.Name, values) + ")";
                // ※postgresの場合はinと同じ結果が得られるany演算子で代替する（配列パラメータサポートの副作用でin条件のパラメータが展開されず、SQLも展開されないため）
            }
        }
    }
}
