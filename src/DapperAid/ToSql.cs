using System;
using System.Collections.Generic;

namespace DapperAid
{
    /// <summary>
    /// Obsolete：ToSqlクラスの機能はSqlExprクラスにより置き換えられました。
    /// </summary>
    [Obsolete("'ToSql' is renamed to 'SqlExpr' since v0.8.8.", false)]
    public class ToSql : SqlExpr
    {
        // SqlExprクラスを継承することで、従来のソースコードでも
        // 定義されている同名のメソッドを利用できる状態を維持しておく。

        /// <summary>※外部からのインスタンス化不可</summary>
        private ToSql() : base() { }
    }
}
