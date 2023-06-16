using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Dapper;
using DapperAid.Helpers;

namespace DapperAid
{
    // Dapperを用いたDB操作を実行するクラスです。
    // （非同期実行メソッドをこのファイルに記述）
    public partial class QueryRunner
    {
        #region SQL実行(非同期) ------------------------------------------------

        /// <summary>
        /// 指定されたテーブルのレコード数を非同期で取得します。
        /// </summary>
        /// <param name="where">レコード絞り込み条件（絞り込みを行わず全件対象とする場合はnull）</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>レコード数</returns>
        public Task<ulong> CountAsync<T>(Expression<Func<T, bool>>? where = null)
            where T : notnull
        {
            var parameters = new DynamicParameters();
            var sql = this.Builder.BuildCount<T>() + this.Builder.BuildWhere(parameters, where);
            return this.Connection.ExecuteScalarAsync<ulong>(sql, parameters, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定されたレコードを非同期で取得します。
        /// </summary>
        /// <param name="keyValues">レコード特定Key値を初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Key1 = 1, Key2 = 99 }</c>」</param>
        /// <param name="targetColumns">値取得対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <param name="otherClauses">SQL文の末尾に付加するforUpdate指定などがあれば、その内容</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>取得したレコード（１件、レコード不存在の場合はnull）</returns>
        public async Task<T?> SelectAsync<T>(Expression<Func<T>> keyValues, Expression<Func<T, dynamic>>? targetColumns = null, string? otherClauses = null)
            where T : notnull
        {
            var parameters = new DynamicParameters();
            var sql = this.Builder.BuildSelect<T>(targetColumns)
                + this.Builder.BuildWhere(parameters, keyValues)
                + (string.IsNullOrWhiteSpace(otherClauses) ? "" : " " + otherClauses);
            return await this.Connection.QueryFirstOrDefaultAsync<T>(sql, parameters, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定されたテーブルからレコードを非同期で取得します。
        /// </summary>
        /// <param name="where">レコード絞り込み条件（絞り込みを行わない場合はnull）</param>
        /// <param name="targetColumns">値取得対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <param name="otherClauses">SQL文の末尾に付加するorderBy条件/limit/offset/forUpdate指定などがあれば、その内容</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>取得したレコード（１件、レコード不存在の場合はnull）</returns>
        public async Task<T?> SelectFirstOrDefaultAsync<T>(Expression<Func<T, bool>>? where = null, Expression<Func<T, dynamic>>? targetColumns = null, string? otherClauses = null)
            where T : notnull
        {
            var parameters = new DynamicParameters();
            var sql = this.Builder.BuildSelect<T>(targetColumns)
                + this.Builder.BuildWhere(parameters, where)
                + this.Builder.BuildSelectOrderByEtc(targetColumns, otherClauses);
            return await this.Connection.QueryFirstOrDefaultAsync<T>(sql, parameters, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定されたテーブルからレコードのリストを非同期で取得します。
        /// </summary>
        /// <param name="where">レコード絞り込み条件（絞り込みを行わず全件対象とする場合はnull）</param>
        /// <param name="targetColumns">値取得対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <param name="otherClauses">SQL文の末尾に付加するorderBy条件/limit/offset/forUpdate指定などがあれば、その内容</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>レコードのリスト</returns>
        public async Task<IReadOnlyList<T>> SelectAsync<T>(Expression<Func<T, bool>>? where = null, Expression<Func<T, dynamic>>? targetColumns = null, string? otherClauses = null)
            where T : notnull
        {
            var parameters = new DynamicParameters();
            var sql = this.Builder.BuildSelect(targetColumns)
                + this.Builder.BuildWhere(parameters, where)
                + this.Builder.BuildSelectOrderByEtc(targetColumns, otherClauses);
            var result = await this.Connection.QueryAsync<T>(sql, parameters, this.Transaction, this.Timeout);
            return (IReadOnlyList<T>)result;
        }

        /// <summary>
        /// 指定されたテーブルからレコードを非同期で取得します。
        /// </summary>
        /// <param name="where">レコード絞り込み条件（絞り込みを行わない場合はnull）</param>
        /// <param name="otherClauses">SQL文の末尾に付加するforUpdate指定などがあれば、その内容</param>
        /// <typeparam name="TFrom">取得対象テーブルにマッピングされた型</typeparam>
        /// <typeparam name="TColumns">取得対象列にマッピングされた型</typeparam>
        /// <returns>取得したレコード（１件、レコード不存在の場合はnull）</returns>
        public async Task<TColumns?> SelectFirstOrDefaultAsync<TFrom, TColumns>(Expression<Func<TFrom, bool>>? where = null, string? otherClauses = null)
            where TFrom : notnull
            where TColumns : notnull
        {
            var parameters = new DynamicParameters();
            var sql = this.Builder.BuildSelect<TFrom, TColumns>()
                + this.Builder.BuildWhere(parameters, where)
                + this.Builder.BuildSelectOrderByEtc<TFrom, TColumns>(otherClauses);
            return await this.Connection.QueryFirstOrDefaultAsync<TColumns>(sql, parameters, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定されたテーブルからレコードのリストを非同期で取得します。
        /// </summary>
        /// <param name="where">レコード絞り込み条件（絞り込みを行わず全件対象とする場合はnull）</param>
        /// <param name="otherClauses">SQL文の末尾に付加するorderBy条件/limit/offset/forUpdate指定などがあれば、その内容</param>
        /// <typeparam name="TFrom">取得対象テーブルにマッピングされた型</typeparam>
        /// <typeparam name="TColumns">取得対象列にマッピングされた型</typeparam>
        /// <returns>レコードのリスト</returns>
        public async Task<IReadOnlyList<TColumns>> SelectAsync<TFrom, TColumns>(Expression<Func<TFrom, bool>>? where = null, string? otherClauses = null)
            where TFrom : notnull
            where TColumns : notnull
        {
            var parameters = new DynamicParameters();
            var sql = this.Builder.BuildSelect<TFrom, TColumns>()
                + this.Builder.BuildWhere(parameters, where)
                + this.Builder.BuildSelectOrderByEtc<TFrom, TColumns>(otherClauses);
            var result = await this.Connection.QueryAsync<TColumns>(sql, parameters, this.Transaction, this.Timeout);
            return (IReadOnlyList<TColumns>)result;
        }


        /// <summary>
        /// 指定された値でレコードを非同期で挿入します。
        /// </summary>
        /// <param name="values">値設定対象カラム・設定値を初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Key1 = 1, Value = 99 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入された行数</returns>
        public Task<int> InsertAsync<T>(Expression<Func<T>> values)
            where T : notnull
        {
            var parameters = new DynamicParameters();
            var sql = this.Builder.BuildInsert<T>(parameters, values);
            return this.Connection.ExecuteAsync(sql, parameters, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定されたレコードを非同期で挿入します。
        /// </summary>
        /// <param name="data">挿入するレコード</param>
        /// <param name="targetColumns">値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入された行数</returns>
        public Task<int> InsertAsync<T>(T data, Expression<Func<T, dynamic>>? targetColumns = null)
            where T : notnull
        {
            var sql = this.Builder.BuildInsert<T>(targetColumns);
            return this.Connection.ExecuteAsync(sql, data, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定されたレコードを非同期で挿入し、[InsertSQL(RetrieveInsertedId = true)]属性の自動連番カラムで採番されたIDを当該プロパティへセットします。
        /// </summary>
        /// <param name="data">挿入するレコード</param>
        /// <param name="targetColumns">値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入された行数</returns>
        /// <remarks>
        /// 自動連番に対応していないテーブル/DBMSでは例外がスローされます。
        /// </remarks>
        public async Task<int> InsertAndRetrieveIdAsync<T>(T data, Expression<Func<T, dynamic>>? targetColumns = null)
            where T : notnull
        {
            var sql = this.Builder.BuildInsertAndRetrieveId<T>(targetColumns);

            var tableInfo = this.Builder.GetTableInfo<T>();
            var idProp = tableInfo.RetrieveInsertedIdColumn?.PropertyInfo ?? throw new ConstraintException("RetrieveInsertedId-Column not specified");
            object insertedId;
            if (sql.Contains(" into " + this.Builder.ParameterMarker + idProp.Name))
            {
                // 採番値がoutパラメータへ格納される場合(Oracleなど)、outパラメータ含む各パラメータをバインドして実行、採番値を把握
                var parameters = new DynamicParameters();
                parameters.Add(idProp.Name, MemberAccessor.GetValue(data, idProp), null, ParameterDirection.InputOutput);
                var columns = (targetColumns == null)
                    ? tableInfo.Columns.Where(c => c.Insert)
                    : tableInfo.GetColumns(ExpressionHelper.GetMemberNames(targetColumns.Body));
                foreach (var column in columns)
                {
                    this.Builder.AddParameter(parameters, column.PropertyInfo.Name, MemberAccessor.GetValue(data, column.PropertyInfo));
                }
                await this.Connection.ExecuteAsync(sql, parameters, this.Transaction, this.Timeout);
                insertedId = parameters.Get<object>(idProp.Name);
            }
            else
            {
                // 通常のDBMSについては、SQL実行結果列の値として採番値を把握
                insertedId = await this.Connection.ExecuteScalarAsync(sql, data, this.Transaction, this.Timeout);
            }
            MemberAccessor.SetValue(data, idProp, Convert.ChangeType(insertedId, idProp.PropertyType));
            return 1;
        }

        /// <summary>
        /// 指定されたレコードを非同期で一括挿入します。
        /// </summary>
        /// <param name="records">挿入するレコード（複数件）</param>
        /// <param name="targetColumns">値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入された行数</returns>
        public async Task<int> InsertRowsAsync<T>(IEnumerable<T> records, Expression<Func<T, dynamic>>? targetColumns = null)
            where T : notnull
        {
            var ret = 0;
            foreach (var sql in this.Builder.BuildMultiInsert(records, targetColumns))
            {
                ret += await this.Connection.ExecuteAsync(sql, null, this.Transaction, this.Timeout);
            }
            return ret;
        }

        /// <summary>
        /// 指定されたレコードを非同期で挿入または更新します。(既存レコードはUPDATE／未存在ならINSERTを行います)
        /// </summary>
        /// <param name="data">挿入または更新するレコード</param>
        /// <param name="insertTargetColumns">insert実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2, t.Col3 }</c>」</param>
        /// <param name="updateTargetColumns">update実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col2, t.Col3 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入または更新された行数</returns>
        public async Task<int> InsertOrUpdateAsync<T>(T data, Expression<Func<T, dynamic>>? insertTargetColumns = null, Expression<Func<T, dynamic>>? updateTargetColumns = null)
            where T : notnull
        {
            if (!this.Builder.SupportsUpsert)
            {
                return UpdateOrInsertOnebyone(new T[] { data }, insertTargetColumns, updateTargetColumns);
            }

            var sql = this.Builder.BuildUpsert(insertTargetColumns, updateTargetColumns);
            return await this.Connection.ExecuteAsync(sql, data, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定されたレコードを非同期で一括挿入または更新します。(既存レコードはUPDATE／未存在ならINSERTを行います)
        /// </summary>
        /// <param name="records">挿入または更新するレコード（複数件）</param>
        /// <param name="insertTargetColumns">insert実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2, t.Col3 }</c>」</param>
        /// <param name="updateTargetColumns">update実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col2, t.Col3 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入または更新された行数</returns>
        public async Task<int> InsertOrUpdateRowsAsync<T>(IEnumerable<T> records, Expression<Func<T, dynamic>>? insertTargetColumns = null, Expression<Func<T, dynamic>>? updateTargetColumns = null)
            where T : notnull
        {
            if (!this.Builder.SupportsUpsert)
            {   // Upsert未対応の場合は１レコードずつ処理実行
                return UpdateOrInsertOnebyone(records, insertTargetColumns, updateTargetColumns);
            }

            var ret = 0;
            foreach (var sql in this.Builder.BuildMultiUpsert(records, insertTargetColumns, updateTargetColumns))
            {
                ret += await this.Connection.ExecuteAsync(sql, null, this.Transaction, this.Timeout);
            }
            return ret;
        }

        /// <summary>
        /// Upsert未対応DBMS向けに、指定されたレコードを非同期で１件ずつ挿入または更新します（既存レコードのUPDATEを試み、未存在だった場合にはINSERTを行います）
        /// </summary>
        /// <param name="records">挿入または更新するレコード（複数件）</param>
        /// <param name="insertTargetColumns">insert実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2, t.Col3 }</c>」</param>
        /// <param name="updateTargetColumns">update実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col2, t.Col3 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入または更新された行数</returns>
        /// <remarks>１レコードずつSQLを実行するため低速です。</remarks>
        protected async Task<int> UpdateOrInsertOnebyoneAsync<T>(IEnumerable<T> records, Expression<Func<T, dynamic>>? insertTargetColumns = null, Expression<Func<T, dynamic>>? updateTargetColumns = null)
            where T : notnull
        {
            var insertSql = this.Builder.BuildInsert<T>(insertTargetColumns);
            var updateSql = this.Builder.BuildUpdate<T>(updateTargetColumns) + this.Builder.BuildWhere<T>(default(T), c => (c.IsKey));
            var ret = 0;
            foreach (var record in records)
            {
                var updated = await this.Connection.ExecuteAsync(updateSql, record, this.Transaction, this.Timeout);
                ret += (updated > 0)
                    ? updated
                    : await this.Connection.ExecuteAsync(insertSql, record, this.Transaction, this.Timeout);
            }
            return ret;
        }


        /// <summary>
        /// 指定された条件にマッチするレコードについて、指定されたカラムの値を非同期で更新します。
        /// </summary>
        /// <param name="values">更新対象カラム・更新値を初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Value1 = 99, Flg = true }</c>」</param>
        /// <param name="where">更新対象レコードの条件（全件対象とする場合はnull）</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>更新された行数</returns>
        public Task<int> UpdateAsync<T>(Expression<Func<T>> values, Expression<Func<T, bool>> where)
            where T : notnull
        {
            var parameters = new DynamicParameters();
            var sql = this.Builder.BuildUpdate<T>(parameters, values)
                + this.Builder.BuildWhere(parameters, where);
            return this.Connection.ExecuteAsync(sql, parameters, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定されたレコードを非同期で更新します。
        /// </summary>
        /// <param name="data">更新するレコード</param>
        /// <param name="targetColumns">値更新対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>更新された行数</returns>
        public Task<int> UpdateAsync<T>(T data, Expression<Func<T, dynamic>>? targetColumns = null)
            where T : notnull
        {
            // PKカラム、楽観同時実行チェック対象カラム(ただしバインド値で更新するカラムは除く)をwhere条件カラムとしてSQL生成実行
            var sql = this.Builder.BuildUpdate<T>(targetColumns)
                + this.Builder.BuildWhere<T>(data, c => (c.IsKey || (c.ConcurrencyCheck && !(c.Update && c.UpdateSQL == null))));
            return this.Connection.ExecuteAsync(sql, data, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定された条件にマッチするレコードを非同期で削除します。
        /// </summary>
        /// <param name="where">削除対象レコードの条件</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>削除された行数</returns>
        public Task<int> DeleteAsync<T>(Expression<Func<T, bool>>? where = null)
            where T : notnull
        {
            var parameters = new DynamicParameters();
            var sql = this.Builder.BuildDelete<T>()
                + this.Builder.BuildWhere(parameters, where);
            return this.Connection.ExecuteAsync(sql, parameters, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定されたレコードを非同期で削除します。
        /// </summary>
        /// <param name="data">削除するレコード</param>
        /// <returns>削除された行数</returns>
        public Task<int> DeleteAsync<T>(T data)
            where T : notnull
        {
            // PKカラム、楽観同時実行チェック対象カラムをwhere条件カラムとしてSQL生成実行
            var sql = this.Builder.BuildDelete<T>()
                + this.Builder.BuildWhere<T>(data, c => (c.IsKey || c.ConcurrencyCheck));
            return this.Connection.ExecuteAsync(sql, data, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定されたテーブルの全レコードを非同期で削除します。
        /// </summary>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        public Task TruncateAsync<T>()
            where T : notnull
        {
            var sql = this.Builder.BuildTruncate<T>();
            return this.Connection.ExecuteAsync(sql, null, this.Transaction, this.Timeout);
        }
        #endregion
    }
}
