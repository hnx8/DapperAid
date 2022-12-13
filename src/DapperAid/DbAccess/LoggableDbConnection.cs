using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

#pragma warning disable CA2200 // スタックトレースを切ることについては警告としない

namespace DapperAid.DbAccess
{
    /// <summary>
    /// ログ出力機能を提供するDB接続です。
    /// </summary>
    /// <remarks>
    /// コンストラクタで引き渡されたDbConnectionを使用してDB関連処理を行い、
    /// またSQL実行のつどコンストラクタで引き渡されたログ出力メソッドを呼び出すように実装されています。
    /// </remarks>
    public class LoggableDbConnection : DbConnection
    {
        #region プロパティ -----------------------------------------------------

        /// <summary>ラップしている生のDbConnection</summary>
        private readonly DbConnection _innerConnection;

        /// <summary>エラーログ出力メソッド：引数＝例外オブジェクト, 実行したDbCommand(DbCommandではない場合はnull)</summary>
        public Action<Exception, DbCommand?> ErrorLogger { get; set; }

        /// <summary>トレースログ出力メソッド：引数＝結果概要テキスト(例:「HasResults」「Value=xxx」「Commit」), 実行所要時間mSec, 実行したDbCommand(DbCommandではない場合はnull)</summary>
        public Action<string, long, DbCommand?>? TraceLogger { get; set; }

        #endregion

        #region コンストラクタ -------------------------------------------------
        /// <summary>
        /// インスタンスを初期化します。
        /// </summary>
        /// <param name="connection">実際のDB操作に使用するDbConnection</param>
        /// <param name="errorLogger">エラーログ出力メソッド：引数＝例外オブジェクト, 実行したDbCommand(DbCommandではない場合はnull)</param>
        /// <param name="traceLogger">トレースログ出力メソッド：引数＝結果概要テキスト(例:「HasResults」「Value=xxx」「Commit」), 実行所要時間mSec, 実行したDbCommand(DbCommandではない場合はnull)</param>
        public LoggableDbConnection(DbConnection connection, Action<Exception, DbCommand?> errorLogger, Action<string, long, DbCommand?>? traceLogger = null)
        {
            this._innerConnection = connection;
            this.ErrorLogger = errorLogger;
            this.TraceLogger = traceLogger;
        }
        #endregion

        #region ログ出力機能追加のためのオーバーライド -------------------------

        /// <summary>データベース接続を開き、トレースログを出力します。</summary>
        public override void Open()
        {
            try
            {
                var sw = Stopwatch.StartNew();
                _innerConnection.Open();
                this.TraceLogger?.Invoke("Open", sw.ElapsedMilliseconds, null);
            }
            catch (Exception ex)
            {   // エラーログを出力し、スタックトレースを切って再throw
                this.ErrorLogger?.Invoke(ex, null);
                throw ex;
            }
        }

        /// <summary>非同期でデータベース接続を開き、トレースログを出力します。</summary>
        public override async Task OpenAsync(System.Threading.CancellationToken cancellationToken)
        {
            try
            {
                var sw = Stopwatch.StartNew();
                await this._innerConnection.OpenAsync(cancellationToken);
                this.TraceLogger?.Invoke("OpenAsync", sw.ElapsedMilliseconds, null);
            }
            catch (Exception ex)
            {   // エラーログを出力し、スタックトレースを切って再throw
                this.ErrorLogger?.Invoke(ex, null);
                throw ex;
            }
        }

        /// <summary>
        /// ログ出力可能なDbTransactionオブジェクトを生成してトランザクションを開始し、トレースログを出力します。
        /// </summary>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            try
            {
                var sw = Stopwatch.StartNew();
                DbTransaction ret = new Transaction(this, _innerConnection.BeginTransaction());
                this.TraceLogger?.Invoke("BeginTransaction", sw.ElapsedMilliseconds, null);
                return ret;
            }
            catch (Exception ex)
            {   // エラーログを出力し、スタックトレースを切って再throw
                this.ErrorLogger?.Invoke(ex, null);
                throw ex;
            }
        }

        /// <summary>
        /// ログ出力可能なDbCommandオブジェクトを生成します。
        /// </summary>
        protected override DbCommand CreateDbCommand()
        {
            return new Command(this, _innerConnection.CreateCommand());
        }

        /// <summary>オブジェクト破棄</summary>
        protected override void Dispose(bool disposing)
        {
            try
            {
                this._innerConnection.Dispose();
            }
            catch (Exception ex)
            {
                this.ErrorLogger?.Invoke(ex, null);
            }
            base.Dispose(disposing);
        }

#if NET // NET5未満では未対応の非同期メソッド
        /// <summary>
        /// ログ出力可能なDbTransactionオブジェクトを非同期で生成してトランザクションを開始し、トレースログを出力します。
        /// </summary>
        protected override async ValueTask<DbTransaction> BeginDbTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken)
        {
            try
            {
                var sw = Stopwatch.StartNew();
                DbTransaction ret = new Transaction(this, await _innerConnection.BeginTransactionAsync(isolationLevel, cancellationToken));
                this.TraceLogger?.Invoke("BeginTransactionAsync", sw.ElapsedMilliseconds, null);
                return ret;
            }
            catch (Exception ex)
            {   // エラーログを出力し、スタックトレースを切って再throw
                this.ErrorLogger?.Invoke(ex, null);
                throw ex;
            }
        }

        /// <summary>非同期オブジェクト破棄</summary>
        public override async ValueTask DisposeAsync()
        {
            try
            {
                await _innerConnection.DisposeAsync();
            }
            catch (Exception ex)
            {
                this.ErrorLogger?.Invoke(ex, null);
            }
            await base.DisposeAsync();
        }
#endif
        #endregion

        #region ラッピングのためのオーバーライド -------------------------------
#pragma warning disable 1591 // 以下のオーバーライドについてはわざわざドキュメントコメントを書かない。コンパイルの警告(CS1591)も抑止する

        // 以下、実装必須プロパティ/メソッド
        [AllowNull]
        public override string ConnectionString { get { return _innerConnection.ConnectionString; } set { _innerConnection.ConnectionString = value; } }

        public override string Database { get { return _innerConnection.Database; } }
        public override string DataSource { get { return _innerConnection.DataSource; } }
        public override string ServerVersion { get { return _innerConnection.ServerVersion; } }
        public override ConnectionState State { get { return _innerConnection.State; } }

        public override void ChangeDatabase(string databaseName)
        {
            _innerConnection.ChangeDatabase(databaseName);
        }
        public override void Close()
        {
            _innerConnection.Close();
        }

        // 以下、実装必須ではないが挙動のつじつまを合わせるために必要なプロパティ/メソッド

        public override int ConnectionTimeout
        {
            get { return _innerConnection.ConnectionTimeout; }
        }
        public override event StateChangeEventHandler? StateChange
        {
            add { _innerConnection.StateChange += value; }
            remove { _innerConnection.StateChange -= value; }
        }

        public override void EnlistTransaction(System.Transactions.Transaction? transaction)
        {
            _innerConnection.EnlistTransaction(transaction);
        }
        public override DataTable GetSchema()
        {
            return _innerConnection.GetSchema();
        }
        public override DataTable GetSchema(string collectionName)
        {
            return _innerConnection.GetSchema(collectionName);
        }
        public override DataTable GetSchema(string collectionName, string?[] restrictionValues)
        {
            return _innerConnection.GetSchema(collectionName, restrictionValues);
        }
        //protected override void OnStateChange(StateChangeEventArgs stateChange)
        //{
        //    _innerConnection.OnStateChange(stateChange);
        //} // ※StateChangeイベントの発火はinnerConnection側に任せるためオーバーライド不要

#if NET // NET5未満では未対応の非同期メソッド
        public override Task ChangeDatabaseAsync(string databaseName, CancellationToken cancellationToken = default)
        {
            return _innerConnection.ChangeDatabaseAsync(databaseName, cancellationToken);
        }
        public override Task CloseAsync()
        {
            return _innerConnection.CloseAsync();
        }
        // public override Task<DataTable> GetSchemaAsync(string collectionName, string?[] restrictionValues, CancellationToken cancellationToken = default)
        // {
        //     return _innerConnection.GetSchemaAsync(collectionName, restrictionValues, cancellationToken);
        // } // ※NETSTANDARD2.1未対応かつ実装省略しても実害がないためコメントアウトとしておく
#endif

#pragma warning restore 1591
        #endregion

        #region DBトランザクション/DBコマンド ----------------------------------

        /// <summary>
        /// ログ出力機能を組み込んだDbTransactionクラスです。
        /// </summary>
        private class Transaction : DbTransaction
        {
            /// <summary>ログ出力に使用するLoggableDbConnection</summary>
            private readonly LoggableDbConnection _conn;

            /// <summary>ラップしている生のDbTransaction</summary>
            private readonly DbTransaction _innerTransaction;
            internal DbTransaction InnerTransaction { get { return _innerTransaction; } }

            /// <summary>トランザクションが終了しているか否か</summary>
            private bool _isCompleted = false;

            /// <summary>
            /// インスタンスを初期化します。
            /// </summary>
            internal Transaction(LoggableDbConnection conn, DbTransaction innerTransaction)
            {
                this._conn = conn;
                this._innerTransaction = innerTransaction;
            }

            // 以下、オーバーライド実装各種 ------------------------------------
            protected override DbConnection DbConnection
            {
                get { return _conn; }
            }
            public override IsolationLevel IsolationLevel { get { return _innerTransaction.IsolationLevel; } }

            /// <summary>
            /// コミットし、トレースログを出力します。
            /// </summary>
            public override void Commit()
            {
                try
                {
                    var sw = Stopwatch.StartNew();
                    _innerTransaction.Commit();
                    _conn.TraceLogger?.Invoke("Commit", sw.ElapsedMilliseconds, null);
                    _isCompleted = true;
                }
                catch (Exception ex)
                {   // エラーログを出力し、スタックトレースを切って再throw
                    _conn.ErrorLogger?.Invoke(ex, null);
                    throw ex;
                }
            }

            /// <summary>
            /// ロールバックし、トレースログを出力します。
            /// </summary>
            public override void Rollback()
            {
                try
                {
                    var sw = Stopwatch.StartNew();
                    _innerTransaction.Rollback();
                    _conn.TraceLogger?.Invoke("Rollback", sw.ElapsedMilliseconds, null);
                    _isCompleted = true;
                }
                catch (Exception ex)
                {   // エラーログを出力し、スタックトレースを切って再throw
                    _conn.ErrorLogger?.Invoke(ex, null);
                    throw ex;
                }
            }

            /// <summary>
            /// オブジェクトを破棄する際、
            /// トランザクションがCommitもRollbackもされていない場合は暗黙ロールバックについてのトレースログを出力します。
            /// </summary>
            protected override void Dispose(bool disposing)
            {
                try
                {
                    var sw = (_isCompleted ? null : Stopwatch.StartNew());
                    _innerTransaction.Dispose();
                    if (sw != null)
                    {
                        // コミット/ロールバックが明示的に行われていない場合は、Disposeにより暗黙的にRollbackが行われる。
                        // コミットし忘れをログから調査できるようにする意図でトレースログ出力する。
                        _conn.TraceLogger?.Invoke("ImplicitRollback", sw.ElapsedMilliseconds, null);
                        _isCompleted = true;
                    }
                }
                catch (Exception ex)
                {
                    _conn.ErrorLogger?.Invoke(ex, null);
                }
                base.Dispose(disposing);
            }

#if NET // NET5未満では未対応の非同期メソッド
            /// <summary>
            /// 非同期でコミットし、トレースログを出力します。
            /// </summary>
            public override async Task CommitAsync(CancellationToken cancellationToken = default)
            {
                try
                {
                    var sw = Stopwatch.StartNew();
                    await _innerTransaction.CommitAsync(cancellationToken);
                    _conn.TraceLogger?.Invoke("CommitAsync", sw.ElapsedMilliseconds, null);
                    _isCompleted = true;
                }
                catch (Exception ex)
                {   // エラーログを出力し、スタックトレースを切って再throw
                    _conn.ErrorLogger?.Invoke(ex, null);
                    throw ex;
                }
            }

            /// <summary>
            /// 非同期でロールバックし、トレースログを出力します。
            /// </summary>
            public override async Task RollbackAsync(CancellationToken cancellationToken = default)
            {
                try
                {
                    var sw = Stopwatch.StartNew();
                    await _innerTransaction.RollbackAsync(cancellationToken);
                    _conn.TraceLogger?.Invoke("RollbackAsync", sw.ElapsedMilliseconds, null);
                    _isCompleted = true;
                }
                catch (Exception ex)
                {   // エラーログを出力し、スタックトレースを切って再throw
                    _conn.ErrorLogger?.Invoke(ex, null);
                    throw ex;
                }
            }

            /// <summary>
            /// オブジェクトを非同期で破棄する際、
            /// トランザクションがCommitもRollbackもされていない場合は暗黙ロールバックについてのトレースログを出力します。
            /// </summary>
            public override async ValueTask DisposeAsync()
            {
                try
                {
                    var sw = (_isCompleted ? null : Stopwatch.StartNew());
                    await _innerTransaction.DisposeAsync();
                    if (sw != null)
                    {
                        // コミット/ロールバックが明示的に行われていない場合は、Disposeにより暗黙的にRollbackが行われる。
                        // コミットし忘れをログから調査できるようにする意図でトレースログ出力する。
                        _conn.TraceLogger?.Invoke("ImplicitAsyncRollback", sw.ElapsedMilliseconds, null);
                        _isCompleted = true;
                    }
                }
                catch (Exception ex)
                {
                    _conn.ErrorLogger?.Invoke(ex, null);
                }
                await base.DisposeAsync();
            }
#endif
        }

        /// <summary>
        /// innerclass:ログ出力機能を組み込んだDbCommand
        /// </summary>
        private class Command : DbCommand
        {
            /// <summary>ログ出力に使用するLoggableDbConnection</summary>
            private readonly LoggableDbConnection _conn;

            /// <summary>ラップしている生のDbCommand</summary>
            private readonly DbCommand _innerCommand;

            /// <summary>DbCommandに対し指定されたDbTransaction</summary>
            private DbTransaction? _tran;

            /// <summary>
            /// インスタンスを初期化します。
            /// </summary>
            internal Command(LoggableDbConnection conn, DbCommand innerCommand)
            {
                this._conn = conn;
                this._innerCommand = innerCommand;
            }

            // 以下、オーバーライド実装各種 ------------------------------------

            [AllowNull]
            public override string CommandText { get { return _innerCommand.CommandText; } set { _innerCommand.CommandText = value; } }

            public override int CommandTimeout { get { return _innerCommand.CommandTimeout; } set { _innerCommand.CommandTimeout = value; } }
            public override CommandType CommandType { get { return _innerCommand.CommandType; } set { _innerCommand.CommandType = value; } }

            protected override DbConnection? DbConnection
            {   // コネクションは変更不可として取り扱う
                get { return _innerCommand.Connection; }
                set { throw new InvalidOperationException(); }
            }

            protected override DbParameterCollection DbParameterCollection { get { return _innerCommand.Parameters; } }

            protected override DbTransaction? DbTransaction
            {
                get { return _tran; }
                set
                {   // ラップされているDbTransactionが引き渡された場合は、生のDbCommandにはラップされていないDbTransactionを設定する
                    _tran = value;
                    var wrapped = (value as Transaction);
                    _innerCommand.Transaction = (wrapped != null) ? wrapped.InnerTransaction : value;
                }
            }

            public override bool DesignTimeVisible { get { return _innerCommand.DesignTimeVisible; } set { _innerCommand.DesignTimeVisible = value; } }
            public override UpdateRowSource UpdatedRowSource { get { return _innerCommand.UpdatedRowSource; } set { _innerCommand.UpdatedRowSource = value; } }

            public override void Cancel()
            {
                _innerCommand.Cancel();
            }
            protected override DbParameter CreateDbParameter()
            {
                return _innerCommand.CreateParameter();
            }

            /// <summary>
            /// コマンドを実行します。実行結果トレースログも出力します。
            /// </summary>
            protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
            {
                try
                {
                    var sw = Stopwatch.StartNew();
                    var ret = _innerCommand.ExecuteReader(behavior);
                    _conn.TraceLogger?.Invoke((ret.HasRows ? "HasResults" : "NoResult"), sw.ElapsedMilliseconds, _innerCommand);
                    return ret;
                }
                catch (Exception ex)
                {   // エラーログを出力し、スタックトレースを切って再throw
                    _conn.ErrorLogger?.Invoke(ex, this);
                    throw ex;
                }
            }

            /// <summary>
            /// 非同期でコマンドを実行します。実行結果トレースログも出力します。
            /// </summary>
            protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, System.Threading.CancellationToken cancellationToken)
            {
                try
                {
                    var sw = Stopwatch.StartNew();
                    var ret = await _innerCommand.ExecuteReaderAsync(behavior, cancellationToken);
                    _conn.TraceLogger?.Invoke((ret.HasRows ? "*HasResults" : "*NoResult"), sw.ElapsedMilliseconds, _innerCommand);
                    return ret;
                }
                catch (Exception ex)
                {   // エラーログを出力し、スタックトレースを切って再throw
                    _conn.ErrorLogger?.Invoke(ex, this);
                    throw ex;
                }
            }

            /// <summary>
            /// コマンドを実行します。実行結果トレースログも出力します。
            /// </summary>
            public override int ExecuteNonQuery()
            {
                try
                {
                    var sw = Stopwatch.StartNew();
                    var ret = _innerCommand.ExecuteNonQuery();
                    _conn.TraceLogger?.Invoke("Affected=" + ret.ToString(), sw.ElapsedMilliseconds, _innerCommand);
                    return ret;
                }
                catch (Exception ex)
                {   // エラーログを出力し、スタックトレースを切って再throw
                    _conn.ErrorLogger?.Invoke(ex, this);
                    throw ex;
                }
            }

            /// <summary>
            /// 非同期でコマンドを実行します。実行結果トレースログも出力します。
            /// </summary>
            public override async Task<int> ExecuteNonQueryAsync(System.Threading.CancellationToken cancellationToken)
            {
                try
                {
                    var sw = Stopwatch.StartNew();
                    var ret = await _innerCommand.ExecuteNonQueryAsync(cancellationToken);
                    _conn.TraceLogger?.Invoke("*Affected=" + ret.ToString(), sw.ElapsedMilliseconds, _innerCommand);
                    return ret;
                }
                catch (Exception ex)
                {   // エラーログを出力し、スタックトレースを切って再throw
                    _conn.ErrorLogger?.Invoke(ex, this);
                    throw ex;
                }
            }

            /// <summary>
            /// コマンドを実行します。実行結果トレースログも出力します。
            /// </summary>
            public override object? ExecuteScalar()
            {
                try
                {
                    var sw = Stopwatch.StartNew();
                    var ret = _innerCommand.ExecuteScalar();
                    _conn.TraceLogger?.Invoke("Value=" + (ret != null ? ret.ToString() : "null"), sw.ElapsedMilliseconds, _innerCommand);
                    return ret;
                }
                catch (Exception ex)
                {   // エラーログを出力し、スタックトレースを切って再throw
                    _conn.ErrorLogger?.Invoke(ex, this);
                    throw ex;
                }
            }

            /// <summary>
            /// 非同期でコマンドを実行します。実行結果トレースログも出力します。
            /// </summary>
            public override async Task<object?> ExecuteScalarAsync(System.Threading.CancellationToken cancellationToken)
            {
                try
                {
                    var sw = Stopwatch.StartNew();
                    var ret = await _innerCommand.ExecuteScalarAsync(cancellationToken);
                    _conn.TraceLogger?.Invoke("*Value=" + (ret != null ? ret.ToString() : "null"), sw.ElapsedMilliseconds, _innerCommand);
                    return ret;
                }
                catch (Exception ex)
                {   // エラーログを出力し、スタックトレースを切って再throw
                    _conn.ErrorLogger?.Invoke(ex, this);
                    throw ex;
                }
            }

            public override void Prepare()
            {
                _innerCommand.Prepare();
            }

            /// <summary>オブジェクト破棄</summary>
            protected override void Dispose(bool disposing)
            {
                try
                {
                    this._innerCommand.Dispose();
                }
                catch (Exception ex)
                {
                    _conn.ErrorLogger?.Invoke(ex, null);
                }
                base.Dispose(disposing);
            }
        }
        #endregion
    }
}
