using System;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Dapper;
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
            public override bool IsNull(object value)
            {
                return (base.IsNull(value) || (value is string && string.IsNullOrEmpty(value as string)));
            }

            /// <summary>
            /// 指定されたレコードを挿入し、[InsertSQL(RetrieveInsertedId = true)]属性で指定された自動連番カラムに採番されたIDを当該プロパティにセットします。
            /// (Oracleはoutパラメータより把握)
            /// </summary>
            public override int InsertAndRetrieveId<T>(T data, Expression<Func<T, dynamic>> targetColumns, IDbConnection connection, IDbTransaction transaction, int? timeout = null)
            {
                var tableInfo = GetTableInfo<T>();
                if (tableInfo.RetrieveInsertedIdColumn == null)
                {
                    throw new ConstraintException("RetrieveInsertedId-Column not specified");
                }

                // Oracleの自動採番に限りoutコマンドパラメータから採番されたIDを把握するため、DynamicParameterを自前で生成の上outパラメータを追加する
                var columns = (targetColumns == null)
                    ? tableInfo.Columns.Where(c => c.Insert)
                    : tableInfo.GetColumns(ExpressionHelper.GetMemberNames(targetColumns.Body));
                var parameters = new DynamicParameters();
                foreach (var column in columns)
                {
                    AddParameter(parameters, column.PropertyInfo.Name, MemberAccessor.GetValue(data, column.PropertyInfo));
                }
                var idProp = tableInfo.RetrieveInsertedIdColumn.PropertyInfo;

                var sql = BuildInsert<T>(targetColumns);
                if (!parameters.ParameterNames.Contains(idProp.Name))
                {
                    sql += " returning " + tableInfo.RetrieveInsertedIdColumn.Name + " into " + ParameterMarker + idProp.Name;
                    parameters.Add(idProp.Name, MemberAccessor.GetValue(data, idProp), null, ParameterDirection.Output);
                }
                connection.Execute(sql, parameters, transaction, timeout);
                var insertedId = Convert.ChangeType(parameters.Get<object>(idProp.Name), idProp.PropertyType);
                MemberAccessor.SetValue(data, idProp, insertedId);

                return 1;
            }

            /// <summary>パラメータ値上限1000件を考慮してIn条件式を組み立てます(Oracleのみの特殊対処)</summary>
            /// <remarks>パラメータ値が1000件超の場合は「([COL] Like :P00)or([COL] Like :P01)...」といった条件式が返される。パラメータ値は1000件単位でバインドされる</remarks>
            protected override string BuildWhereIn(Dapper.DynamicParameters parameters, TableInfo.Column column, bool opIsNot, object values)
            {
                var allValues = (values as System.Collections.IEnumerable).Cast<object>().ToArray();
                if (allValues.Length <= 1000)
                {   // 1000件以下なら通常通り組み立て
                    return base.BuildWhereIn(parameters, column, opIsNot, values);
                }

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
        }
    }
}
