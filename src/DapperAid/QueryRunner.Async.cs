using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Dapper;

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
        public Task<ulong> CountAsync<T>(Expression<Func<T, bool>> where = null)
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
        public Task<T> SelectAsync<T>(Expression<Func<T>> keyValues, Expression<Func<T, dynamic>> targetColumns = null, string otherClauses = null)
        {
            var parameters = new DynamicParameters();
            var sql = this.Builder.BuildSelect<T>(targetColumns)
                + this.Builder.BuildWhere(parameters, keyValues)
                + (string.IsNullOrWhiteSpace(otherClauses) ? "" : " " + otherClauses);
            return this.Connection.QueryFirstOrDefaultAsync<T>(sql, parameters, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定されたテーブルからレコードのリストを非同期で取得します。
        /// </summary>
        /// <param name="where">レコード絞り込み条件（絞り込みを行わず全件対象とする場合はnull）</param>
        /// <param name="targetColumns">値取得対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <param name="otherClauses">SQL文の末尾に付加するorderBy条件/limit/offset/forUpdate指定などがあれば、その内容</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>レコードのリスト</returns>
        public async Task<IReadOnlyList<T>> SelectAsync<T>(Expression<Func<T, bool>> where = null, Expression<Func<T, dynamic>> targetColumns = null, string otherClauses = null)
        {
            var parameters = new DynamicParameters();
            var sql = this.Builder.BuildSelect(targetColumns)
                + this.Builder.BuildWhere(parameters, where)
                + this.Builder.BuildSelectOrderByEtc(targetColumns, otherClauses);
            var result = await this.Connection.QueryAsync<T>(sql, parameters, this.Transaction, this.Timeout);
            return result as IReadOnlyList<T>;
        }


        /// <summary>
        /// 指定された値でレコードを非同期で挿入します。
        /// </summary>
        /// <param name="values">設定値を初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Key1 = 1, Value = 99 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入された行数</returns>
        public Task<int> InsertAsync<T>(Expression<Func<T>> values)
        {
            var sql = this.Builder.BuildInsert<T>(values);
            var parameters = this.Builder.BindValues(values);
            return this.Connection.ExecuteAsync(sql, parameters, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定されたレコードを非同期で挿入します。
        /// </summary>
        /// <param name="data">挿入するレコード</param>
        /// <param name="targetColumns">値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入された行数</returns>
        public Task<int> InsertAsync<T>(T data, Expression<Func<T, dynamic>> targetColumns = null)
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
        public Task<int> InsertAndRetrieveIdAsync<T>(T data, Expression<Func<T, dynamic>> targetColumns = null)
        {
            return Task.Run(() => this.InsertAndRetrieveId(data, targetColumns));
        }

        /// <summary>
        /// 指定されたレコードを非同期で一括挿入します。
        /// </summary>
        /// <param name="records">挿入するレコード（複数件）</param>
        /// <param name="targetColumns">値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入された行数</returns>
        public Task<int> InsertRowsAsync<T>(IEnumerable<T> records, Expression<Func<T, dynamic>> targetColumns = null)
        {
            return Task.Run(() => this.InsertRows(records, targetColumns));
        }

        /// <summary>
        /// 指定されたレコードを非同期で挿入または更新します。(既存レコードはUPDATE／未存在ならINSERTを行います)
        /// </summary>
        /// <param name="data">挿入または更新するレコード</param>
        /// <param name="insertTargetColumns">insert実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2, t.Col3 }</c>」</param>
        /// <param name="updateTargetColumns">update実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col2, t.Col3 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入または更新された行数</returns>
        public Task<int> InsertOrUpdateAsync<T>(T data, Expression<Func<T, dynamic>> insertTargetColumns = null, Expression<Func<T, dynamic>> updateTargetColumns = null)
        {
            return Task.Run(() => this.InsertOrUpdate(data, insertTargetColumns, updateTargetColumns));
        }

        /// <summary>
        /// 指定されたレコードを非同期で一括挿入または更新します。(既存レコードはUPDATE／未存在ならINSERTを行います)
        /// </summary>
        /// <param name="records">挿入または更新するレコード（複数件）</param>
        /// <param name="insertTargetColumns">insert実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2, t.Col3 }</c>」</param>
        /// <param name="updateTargetColumns">update実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col2, t.Col3 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入または更新された行数</returns>
        public Task<int> InsertOrUpdateRowsAsync<T>(IEnumerable<T> records, Expression<Func<T, dynamic>> insertTargetColumns = null, Expression<Func<T, dynamic>> updateTargetColumns = null)
        {
            return Task.Run(() => this.InsertOrUpdateRows(records, insertTargetColumns, updateTargetColumns));
        }


        /// <summary>
        /// 指定された条件にマッチするレコードについて、指定されたカラムの値を非同期で更新します。
        /// </summary>
        /// <param name="values">更新値を初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Value1 = 99, Flg = true }</c>」</param>
        /// <param name="where">更新対象レコードの条件（全件対象とする場合はnull）</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>更新された行数</returns>
        public Task<int> UpdateAsync<T>(Expression<Func<T>> values, Expression<Func<T, bool>> where)
        {
            var parameters = this.Builder.BindValues(values);
            var sql = this.Builder.BuildUpdate<T>(values)
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
        public Task<int> UpdateAsync<T>(T data, Expression<Func<T, dynamic>> targetColumns = null)
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
        public Task<int> DeleteAsync<T>(Expression<Func<T, bool>> where = null)
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
        {
            var sql = this.Builder.BuildTruncate<T>();
            return this.Connection.ExecuteAsync(sql, null, this.Transaction, this.Timeout);
        }
        #endregion
    }
}
