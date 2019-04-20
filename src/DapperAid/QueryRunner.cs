using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using Dapper;

namespace DapperAid
{
    /// <summary>
    /// Dapperを用いたDB操作を実行するクラスです。
    /// </summary>
    public partial class QueryRunner
    {
        #region 読み取り専用フィールド/初期化処理 ------------------------------

        /// <summary>DB接続</summary>
        protected readonly IDbConnection Connection;

        /// <summary>DBトランザクション</summary>
        protected readonly IDbTransaction Transaction;

        /// <summary>タイムアウト時間</summary>
        protected readonly int? Timeout;

        /// <summary>Dapper実行SQL/パラメータを組み立てるオブジェクト</summary>
        protected readonly QueryBuilder Builder;


        /// <summary>
        /// インスタンスを生成します。
        /// </summary>
        /// <param name="connection">DB接続</param>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <param name="builder">Dapperで実行するSQL/パラメータを組み立てるオブジェクト（省略時はシステム既定のオブジェクトを使用）</param>
        private QueryRunner(IDbConnection connection, IDbTransaction transaction, int? timeout, QueryBuilder builder)
        {
            this.Connection = connection;
            this.Transaction = transaction;
            this.Timeout = timeout;
            this.Builder = builder ?? QueryBuilder.DefaultInstance;
        }

        /// <summary>
        /// DB接続からインスタンスを生成します。
        /// </summary>
        /// <param name="connection">DB接続</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <param name="builder">Dapperで実行するSQL/パラメータを組み立てるオブジェクト（省略時はシステム既定のオブジェクトを使用）</param>
        public QueryRunner(IDbConnection connection, int? timeout = null, QueryBuilder builder = null)
            : this(connection, null, timeout, builder) { }

        /// <summary>
        /// DBトランザクションからインスタンスを生成します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <param name="builder">Dapperで実行するSQL/パラメータを組み立てるオブジェクト（省略時はシステムデフォルトのオブジェクトを使用）</param>
        public QueryRunner(IDbTransaction transaction, int? timeout = null, QueryBuilder builder = null)
            : this(transaction.Connection, transaction, timeout, builder) { }

        #endregion

        #region SQL実行 --------------------------------------------------------

        /// <summary>
        /// 指定されたテーブルのレコード数を取得します。
        /// </summary>
        /// <param name="where">レコード絞り込み条件（絞り込みを行わず全件対象とする場合はnull）</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>レコード数</returns>
        public ulong Count<T>(Expression<Func<T, bool>> where = null)
        {
            DynamicParameters parameters = null;
            var sql = this.Builder.BuildCount<T>() + this.Builder.BuildWhere(ref parameters, where);
            return this.Connection.ExecuteScalar<ulong>(sql, parameters, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定されたレコードを取得します。
        /// </summary>
        /// <param name="keyValues">レコード特定Key値を初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Key1 = 1, Key2 = 99 }</c>」</param>
        /// <param name="targetColumns">値取得対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <param name="otherClauses">SQL文の末尾に付加するforUpdate指定などがあれば、その内容</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>取得したレコード</returns>
        public T Select<T>(Expression<Func<T>> keyValues, Expression<Func<T, dynamic>> targetColumns = null, string otherClauses = null)
        {
            DynamicParameters parameters = null;
            var sql = this.Builder.BuildSelect<T>(targetColumns)
                + this.Builder.BuildWhere(ref parameters, keyValues)
                + (string.IsNullOrWhiteSpace(otherClauses) ? "" : " " + otherClauses);
            return this.Connection.QueryFirstOrDefault<T>(sql, parameters, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定されたテーブルからレコードのリストを取得します。
        /// </summary>
        /// <param name="where">レコード絞り込み条件（絞り込みを行わず全件対象とする場合はnull）</param>
        /// <param name="targetColumns">値取得対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <param name="otherClauses">SQL文の末尾に付加するorderBy条件/limit/offset指定などがあれば、その内容</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>レコードのリスト</returns>
        public IReadOnlyList<T> Select<T>(Expression<Func<T, bool>> where = null, Expression<Func<T, dynamic>> targetColumns = null, string otherClauses = null)
        {
            DynamicParameters parameters = null;
            var sql = this.Builder.BuildSelect(targetColumns)
                + this.Builder.BuildWhere(ref parameters, where)
                + this.Builder.BuildSelectOrderByEtc(targetColumns, otherClauses);
            var result = this.Connection.Query<T>(sql, parameters, this.Transaction, true, this.Timeout);
            return result as IReadOnlyList<T>;
        }


        /// <summary>
        /// 指定された値でレコードを挿入します。
        /// </summary>
        /// <param name="values">設定値を初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Key1 = 1, Value = 99 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入された行数</returns>
        public int Insert<T>(Expression<Func<T>> values)
        {
            var sql = this.Builder.BuildInsert<T>(values);
            DynamicParameters parameters = this.Builder.BindValues(values);
            return this.Connection.Execute(sql, parameters, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定されたレコードを挿入します。
        /// </summary>
        /// <param name="data">挿入するレコード</param>
        /// <param name="targetColumns">値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <param name="retrieveInsertedId">[InsertSQL(RetrieveInsertedId = true)]属性で指定された自動連番カラムについて、挿入時に採番されたIDを当該プロパティにセットする場合は、trueを指定</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入された行数</returns>
        public int Insert<T>(T data, Expression<Func<T, dynamic>> targetColumns = null, bool retrieveInsertedId = false)
        {
            if (retrieveInsertedId)
            {   // 自動連番Insert
                return this.Builder.InsertAndRetrieveId(data, targetColumns, this.Connection, this.Transaction, this.Timeout);
            }
            else
            {   // 通常Insert
                var sql = this.Builder.BuildInsert<T>(targetColumns);
                return this.Connection.Execute(sql, data, this.Transaction, this.Timeout);
            }
        }

        /// <summary>
        /// 指定されたレコードを一括挿入します。
        /// </summary>
        /// <param name="data">挿入するレコード（複数件）</param>
        /// <param name="targetColumns">値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入された行数</returns>
        public int InsertRows<T>(IEnumerable<T> data, Expression<Func<T, dynamic>> targetColumns = null)
        {
            return this.Builder.InsertRows<T>(data, targetColumns, this.Connection, this.Transaction, this.Timeout);
        }


        /// <summary>
        /// 指定された条件にマッチするレコードについて、指定されたカラムの値を更新します。
        /// </summary>
        /// <param name="values">更新値を初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Value1 = 99, Flg = true }</c>」</param>
        /// <param name="where">更新対象レコードの条件（全件対象とする場合はnull）</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>更新された行数</returns>
        public int Update<T>(Expression<Func<T>> values, Expression<Func<T, bool>> where)
        {
            DynamicParameters parameters = this.Builder.BindValues(values);
            var sql = this.Builder.BuildUpdate<T>(values)
                + this.Builder.BuildWhere(ref parameters, where);
            return this.Connection.Execute(sql, parameters, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定されたレコードを更新します。
        /// </summary>
        /// <param name="data">更新するレコード</param>
        /// <param name="targetColumns">値更新対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>更新された行数</returns>
        public int Update<T>(T data, Expression<Func<T, dynamic>> targetColumns = null)
        {
            var sql = this.Builder.BuildUpdate<T>(targetColumns)
                + this.Builder.BuildWhere<T>(data, true);
            return this.Connection.Execute(sql, data, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定された条件にマッチするレコードを削除します。
        /// </summary>
        /// <param name="where">削除対象レコードの条件</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>削除された行数</returns>
        public int Delete<T>(Expression<Func<T, bool>> where = null)
        {
            DynamicParameters parameters = null;
            var sql = this.Builder.BuildDelete<T>()
                + this.Builder.BuildWhere(ref parameters, where);
            return this.Connection.Execute(sql, parameters, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定されたレコードを削除します。
        /// </summary>
        /// <param name="data">削除するレコード</param>
        /// <returns>削除された行数</returns>
        public int Delete<T>(T data)
        {
            var sql = this.Builder.BuildDelete<T>()
                + this.Builder.BuildWhere<T>(data, true);
            return this.Connection.Execute(sql, data, this.Transaction, this.Timeout);
        }

        /// <summary>
        /// 指定されたテーブルの全レコードを削除します。
        /// </summary>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        public void Truncate<T>()
        {
            var sql = this.Builder.BuildTruncate<T>();
            this.Connection.Execute(sql, null, this.Transaction, this.Timeout);
        }
        #endregion
    }
}
