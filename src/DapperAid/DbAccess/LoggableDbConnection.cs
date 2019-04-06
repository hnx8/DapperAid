using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;

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
        internal DbConnection _innerConnection { get; private set; }

        /// <summary>エラーログ出力メソッド：引数＝例外オブジェクト, 実行したDbCommand(DbCommandではない場合はnull)</summary>
        public Action<Exception, DbCommand> ErrorLogger { get; set; }

        /// <summary>トレースログ出力メソッド：引数＝結果概要テキスト(例:「HasResults」「Value=xxx」「Commit」), 実行所要時間mSec, 実行したDbCommand(DbCommandではない場合はnull)</summary>
        public Action<string, long, DbCommand> TraceLogger { get; set; }

        #endregion

        #region コンストラクタ -------------------------------------------------
        /// <summary>
        /// インスタンスを初期化します。
        /// </summary>
        /// <param name="connection">実際のDB操作に使用するDbConnection</param>
        /// <param name="errorLogger">エラーログ出力メソッド：引数＝例外オブジェクト, 実行したDbCommand(DbCommandではない場合はnull)</param>
        /// <param name="traceLogger">トレースログ出力メソッド：引数＝結果概要テキスト(例:「HasResults」「Value=xxx」「Commit」), 実行所要時間mSec, 実行したDbCommand(DbCommandではない場合はnull)</param>
        public LoggableDbConnection(DbConnection connection, Action<Exception, DbCommand> errorLogger, Action<string, long, DbCommand> traceLogger = null)
        {
            this._innerConnection = connection;
            this.ErrorLogger = errorLogger;
            this.TraceLogger = traceLogger;
        }
        #endregion

        #region ログ出力 -------------------------------------------------------

        /// <summary>
        /// SQL実行エラーについてのログ出力メソッドを呼び出します。
        /// </summary>
        /// <param name="ex">例外オブジェクト</param>
        /// <param name="command">実行したDbCommand(DbCommandではない場合はnull)</param>
        private void ErrorLog(Exception ex, DbCommand command = null)
        {
            if (this.ErrorLogger != null) { this.ErrorLogger(ex, command); }
        }
        /// <summary>
        /// SQL実行正常終了についてのログ出力メソッドを呼び出します。
        /// </summary>
        /// <param name="resultSummary">結果概要テキスト(例:「更新行数=xx」「Commit成功」)</param>
        /// <param name="mSec">実行所要時間mSec</param>
        /// <param name="command">実行したDbCommand(DbCommandではない場合はnull)</param>
        private void TraceLog(string resultSummary, long mSec, DbCommand command = null)
        {
            if (this.TraceLogger != null) { this.TraceLogger(resultSummary, mSec, command); }
        }

        #endregion

        #region ログ出力機能追加のためのオーバーライド -------------------------

        /// <summary>
        /// ログ出力可能なDbTransactionオブジェクトを生成してトランザクションを開始し、トレースログを出力します。
        /// </summary>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            try
            {
                Stopwatch sw = Stopwatch.StartNew();
                DbTransaction ret = new Transaction(this, _innerConnection.BeginTransaction());
                this.TraceLog("BeginTransaction", sw.ElapsedMilliseconds);
                return ret;
            }
            catch (Exception ex)
            {   // エラーログを出力し、スタックトレースを切って再throw
                this.ErrorLog(ex);
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
                this.ErrorLog(ex);
            }
            base.Dispose(disposing);
        }
        #endregion

        #region ラッピングのためのオーバーライド -------------------------------
        // 以下のオーバーライドについてはわざわざドキュメントコメントを書かない。コンパイルの警告(CS1591)も抑止する
#pragma warning disable 1591

        // 以下、実装必須プロパティ/メソッド
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
        public override void Open()
        {
            _innerConnection.Open();
        }

        // 以下、実装必須ではないが挙動のつじつまを合わせるために必要なプロパティ/メソッド

        public override int ConnectionTimeout
        {
            get { return _innerConnection.ConnectionTimeout; }
        }
        public override event System.Data.StateChangeEventHandler StateChange
        {
            add { _innerConnection.StateChange += value; }
            remove { _innerConnection.StateChange -= value; }
        }

        public override void EnlistTransaction(System.Transactions.Transaction transaction)
        {
            _innerConnection.EnlistTransaction(transaction);
        }
        public override System.Data.DataTable GetSchema()
        {
            return _innerConnection.GetSchema();
        }
        public override System.Data.DataTable GetSchema(string collectionName)
        {
            return _innerConnection.GetSchema(collectionName);
        }
        public override System.Data.DataTable GetSchema(string collectionName, string[] restrictionValues)
        {
            return _innerConnection.GetSchema(collectionName, restrictionValues);
        }
        //protected override void OnStateChange(System.Data.StateChangeEventArgs stateChange)
        //{   
        //    _innerConnection.OnStateChange(stateChange);
        //} // ※StateChangeイベントの発火はinnerConnection側に任せるためオーバーライド不要

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
            internal readonly DbTransaction _innerTransaction;

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
            public override System.Data.IsolationLevel IsolationLevel { get { return _innerTransaction.IsolationLevel; } }

            /// <summary>
            /// コミットし、トレースログを出力します。
            /// </summary>
            public override void Commit()
            {
                try
                {
                    Stopwatch sw = Stopwatch.StartNew();
                    _innerTransaction.Commit();
                    _conn.TraceLog("Commit", sw.ElapsedMilliseconds);
                    _isCompleted = true;
                }
                catch (Exception ex)
                {   // エラーログを出力し、スタックトレースを切って再throw
                    _conn.ErrorLog(ex);
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
                    Stopwatch sw = Stopwatch.StartNew();
                    _innerTransaction.Rollback();
                    _conn.TraceLog("Rollback", sw.ElapsedMilliseconds);
                    _isCompleted = true;
                }
                catch (Exception ex)
                {   // エラーログを出力し、スタックトレースを切って再throw
                    _conn.ErrorLog(ex);
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
                    Stopwatch sw = (_isCompleted ? null : Stopwatch.StartNew());
                    _innerTransaction.Dispose();
                    if (sw != null)
                    {
                        // コミット/ロールバックが明示的に行われていない場合は、Disposeにより暗黙的にRollbackが行われる。
                        // コミットし忘れをログから調査できるようにする意図でトレースログ出力する。
                        _conn.TraceLog("ImplicitRollback", sw.ElapsedMilliseconds);
                        _isCompleted = true;
                    }
                }
                catch (Exception ex)
                {
                    _conn.ErrorLog(ex);
                }
                base.Dispose(disposing);
            }
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
            private DbTransaction _tran;

            /// <summary>
            /// インスタンスを初期化します。
            /// </summary>
            internal Command(LoggableDbConnection conn, DbCommand innerCommand)
            {
                this._conn = conn;
                this._innerCommand = innerCommand;
            }

            // 以下、オーバーライド実装各種 ------------------------------------

            public override string CommandText { get { return _innerCommand.CommandText; } set { _innerCommand.CommandText = value; } }
            public override int CommandTimeout { get { return _innerCommand.CommandTimeout; } set { _innerCommand.CommandTimeout = value; } }
            public override CommandType CommandType { get { return _innerCommand.CommandType; } set { _innerCommand.CommandType = value; } }

            protected override DbConnection DbConnection
            {   // コネクションは変更不可として取り扱う
                get { return _innerCommand.Connection; }
                set { throw new InvalidOperationException(); }
            }

            protected override DbParameterCollection DbParameterCollection { get { return _innerCommand.Parameters; } }

            protected override DbTransaction DbTransaction
            {
                get { return _tran; }
                set
                {   // ラップされているDbTransactionが引き渡された場合は、生のDbCommandにはラップされていないDbTransactionを設定する
                    _tran = value;
                    Transaction wrapped = (value as Transaction);
                    _innerCommand.Transaction = (wrapped != null) ? wrapped._innerTransaction : value;
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
                    Stopwatch sw = Stopwatch.StartNew();
                    DbDataReader ret = _innerCommand.ExecuteReader(behavior);
                    _conn.TraceLog((ret.HasRows ? "HasResults" : "NoResult"), sw.ElapsedMilliseconds, _innerCommand);
                    return ret;
                }
                catch (Exception ex)
                {   // エラーログを出力し、スタックトレースを切って再throw
                    _conn.ErrorLog(ex);
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
                    Stopwatch sw = Stopwatch.StartNew();
                    int ret = _innerCommand.ExecuteNonQuery();
                    _conn.TraceLog("Affected=" + ret.ToString(), sw.ElapsedMilliseconds, _innerCommand);
                    return ret;
                }
                catch (Exception ex)
                {   // エラーログを出力し、スタックトレースを切って再throw
                    _conn.ErrorLog(ex);
                    throw ex;
                }
            }

            /// <summary>
            /// コマンドを実行します。実行結果トレースログも出力します。
            /// </summary>
            public override object ExecuteScalar()
            {
                try
                {
                    Stopwatch sw = Stopwatch.StartNew();
                    object ret = _innerCommand.ExecuteScalar();
                    _conn.TraceLog("Value=" + (ret != null ? ret.ToString() : "null"), sw.ElapsedMilliseconds, _innerCommand);
                    return ret;
                }
                catch (Exception ex)
                {   // エラーログを出力し、スタックトレースを切って再throw
                    _conn.ErrorLog(ex);
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
                    _conn.ErrorLog(ex);
                }
                base.Dispose(disposing);
            }
        }
        #endregion
    }
}
