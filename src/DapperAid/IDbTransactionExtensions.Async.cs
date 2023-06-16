using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace DapperAid
{
    // Dapperを用いたDB操作をIDbTransactionの拡張メソッドとして提供します。
    // （非同期実行メソッドをこのファイルに記述）
    public static partial class IDbTransactionExtensions
    {
        /// <summary>
        /// 指定されたテーブルのレコード数を非同期で取得します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="where">レコード絞り込み条件（絞り込みを行わず全件対象とする場合はnull）</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>レコード数</returns>
        public static Task<ulong> CountAsync<T>(this IDbTransaction transaction, Expression<Func<T, bool>>? where = null, int? timeout = null)
            where T : notnull
        {
            return new QueryRunner(transaction, timeout).CountAsync(where);
        }

        /// <summary>
        /// 指定されたレコードを非同期で取得します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="keyValues">レコード特定Key値を初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Key1 = 1, Key2 = 99 }</c>」</param>
        /// <param name="targetColumns">値取得対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <param name="otherClauses">SQL文の末尾に付加するforUpdate指定などがあれば、その内容</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>取得したレコード（１件、レコード不存在の場合はnull）</returns>
        public static Task<T?> SelectAsync<T>(this IDbTransaction transaction, Expression<Func<T>> keyValues, Expression<Func<T, dynamic>>? targetColumns = null, string? otherClauses = null, int? timeout = null)
            where T : notnull
        {
            return new QueryRunner(transaction, timeout).SelectAsync(keyValues, targetColumns, otherClauses);
        }

        /// <summary>
        /// 指定されたテーブルからレコードのリストを非同期で取得します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="where">レコード絞り込み条件（絞り込みを行わず全件対象とする場合はnull）</param>
        /// <param name="otherClauses">SQL文の末尾に付加するorderBy条件/limit/offset/forUpdate指定などがあれば、その内容</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>レコードのリスト</returns>
        public static Task<IReadOnlyList<T>> SelectAsync<T>(this IDbTransaction transaction, Expression<Func<T, bool>>? where = null, string? otherClauses = null, int? timeout = null)
            where T : notnull
        {
            return new QueryRunner(transaction, timeout).SelectAsync<T, T>(where, otherClauses);
        }

        /// <summary>
        /// 指定されたテーブルからレコードのリストを非同期で取得します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="where">レコード絞り込み条件（絞り込みを行わず全件対象とする場合はnull）</param>
        /// <param name="targetColumns">値取得対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <param name="otherClauses">SQL文の末尾に付加するorderBy条件/limit/offset/forUpdate指定などがあれば、その内容</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>レコードのリスト</returns>
        public static Task<IReadOnlyList<T>> SelectAsync<T>(this IDbTransaction transaction, Expression<Func<T, bool>>? where, Expression<Func<T, dynamic>>? targetColumns, string? otherClauses = null, int? timeout = null)
            where T : notnull
        {
            return new QueryRunner(transaction, timeout).SelectAsync(where, targetColumns, otherClauses);
        }

        /// <summary>
        /// 指定されたテーブルからレコードを非同期で取得します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="where">レコード絞り込み条件（絞り込みを行わない場合はnull）</param>
        /// <param name="otherClauses">SQL文の末尾に付加するforUpdate指定などがあれば、その内容</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>取得したレコード（１件、レコード不存在の場合はnull）</returns>
        public static Task<T?> SelectFirstOrDefaultAsync<T>(this IDbTransaction transaction, Expression<Func<T, bool>>? where = null, string? otherClauses = null, int? timeout = null)
            where T : notnull
        {
            return new QueryRunner(transaction, timeout).SelectFirstOrDefaultAsync<T, T>(where, otherClauses);
        }

        /// <summary>
        /// 指定されたテーブルからレコードを非同期で取得します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="where">レコード絞り込み条件（絞り込みを行わない場合はnull）</param>
        /// <param name="targetColumns">値取得対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <param name="otherClauses">SQL文の末尾に付加するforUpdate指定などがあれば、その内容</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>取得したレコード（１件、レコード不存在の場合はnull）</returns>
        public static Task<T?> SelectFirstOrDefaultAsync<T>(this IDbTransaction transaction, Expression<Func<T, bool>>? where, Expression<Func<T, dynamic>>? targetColumns, string? otherClauses = null, int? timeout = null)
            where T : notnull
        {
            return new QueryRunner(transaction, timeout).SelectFirstOrDefaultAsync<T>(where, targetColumns, otherClauses);
        }

        /// <summary>
        /// 指定されたテーブルからレコードのリストを非同期で取得します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="where">レコード絞り込み条件（絞り込みを行わず全件対象とする場合はnull）</param>
        /// <param name="otherClauses">SQL文の末尾に付加するorderBy条件/limit/offset/forUpdate指定などがあれば、その内容</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="TFrom">取得対象テーブルにマッピングされた型</typeparam>
        /// <typeparam name="TColumns">取得対象列にマッピングされた型</typeparam>
        /// <returns>レコードのリスト</returns>
        public static Task<IReadOnlyList<TColumns>> SelectAsync<TFrom, TColumns>(this IDbTransaction transaction, Expression<Func<TFrom, bool>>? where = null, string? otherClauses = null, int? timeout = null)
            where TFrom : notnull
            where TColumns : notnull
        {
            return new QueryRunner(transaction, timeout).SelectAsync<TFrom, TColumns>(where, otherClauses);
        }

        /// <summary>
        /// 指定されたテーブルからレコードを非同期で取得します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="where">レコード絞り込み条件（絞り込みを行わない場合はnull）</param>
        /// <param name="otherClauses">SQL文の末尾に付加するforUpdate指定などがあれば、その内容</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="TFrom">取得対象テーブルにマッピングされた型</typeparam>
        /// <typeparam name="TColumns">取得対象列にマッピングされた型</typeparam>
        /// <returns>取得したレコード（１件、レコード不存在の場合はnull）</returns>
        public static Task<TColumns?> SelectFirstOrDefaultAsync<TFrom, TColumns>(this IDbTransaction transaction, Expression<Func<TFrom, bool>>? where = null, string? otherClauses = null, int? timeout = null)
            where TFrom : notnull
            where TColumns : notnull
        {
            return new QueryRunner(transaction, timeout).SelectFirstOrDefaultAsync<TFrom, TColumns>(where, otherClauses);
        }


        /// <summary>
        /// 指定された値でレコードを非同期で挿入します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="values">値設定対象カラム・設定値を初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Key1 = 1, Value = 99 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入された行数</returns>
        public static Task<int> InsertAsync<T>(this IDbTransaction transaction, Expression<Func<T>> values)
            where T : notnull
        {
            return new QueryRunner(transaction, null).InsertAsync(values);
        }

        /// <summary>
        /// 指定された値でレコードを非同期で挿入します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="values">値設定対象カラム・設定値を初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Key1 = 1, Value = 99 }</c>」</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入された行数</returns>
        public static Task<int> InsertAsync<T>(this IDbTransaction transaction, Expression<Func<T>> values, int? timeout = null)
            where T : notnull
        {
            return new QueryRunner(transaction, timeout).InsertAsync(values);
        }

        /// <summary>
        /// 指定されたレコードを非同期で挿入します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="data">挿入するレコード</param>
        /// <param name="targetColumns">値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入された行数</returns>
        public static Task<int> InsertAsync<T>(this IDbTransaction transaction, T data, Expression<Func<T, dynamic>>? targetColumns = null, int? timeout = null)
            where T : notnull
        {
            return new QueryRunner(transaction, timeout).InsertAsync(data, targetColumns);
        }

        /// <summary>
        /// 指定されたレコードを非同期で挿入し、[InsertSQL(RetrieveInsertedId = true)]属性の自動連番カラムで採番されたIDを当該プロパティへセットします。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="data">挿入するレコード</param>
        /// <param name="targetColumns">値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入された行数</returns>
        /// <remarks>
        /// 自動連番に対応していないテーブル/DBMSでは例外がスローされます。
        /// </remarks>
        public static Task<int> InsertAndRetrieveIdAsync<T>(this IDbTransaction transaction, T data, Expression<Func<T, dynamic>>? targetColumns = null, int? timeout = null)
            where T : notnull
        {
            return new QueryRunner(transaction, timeout).InsertAndRetrieveIdAsync(data, targetColumns);
        }

        /// <summary>
        /// 指定されたレコードを非同期で挿入します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="data">挿入するレコード</param>
        /// <param name="targetColumns">値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <param name="retrieveInsertedId">[InsertSQL(RetrieveInsertedId = true)]属性で指定された自動連番カラムについて、挿入時に採番されたIDを当該プロパティにセットする場合は、trueを指定</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入された行数</returns>
        [Obsolete("retrieveInsertedId引数なしのInsert/InsertAndRetrieveIdメソッドを使用してください。")]
        public static Task<int> InsertAsync<T>(this IDbTransaction transaction, T data, Expression<Func<T, dynamic>> targetColumns, bool retrieveInsertedId, int? timeout = null)
            where T : notnull
        {
            return (retrieveInsertedId)
                ? InsertAndRetrieveIdAsync(transaction, data, targetColumns, timeout)
                : InsertAsync(transaction, data, targetColumns, timeout);
        }

        /// <summary>
        /// 指定されたレコードを非同期で一括挿入します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="records">挿入するレコード（複数件）</param>
        /// <param name="targetColumns">値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入された行数</returns>
        public static Task<int> InsertRowsAsync<T>(this IDbTransaction transaction, IEnumerable<T> records, Expression<Func<T, dynamic>>? targetColumns = null, int? timeout = null)
            where T : notnull
        {
            return new QueryRunner(transaction, timeout).InsertRowsAsync(records, targetColumns);
        }

        /// <summary>
        /// 指定されたレコードを非同期で挿入または更新します。(既存レコードはUPDATE／未存在ならINSERTを行います)
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="data">挿入または更新するレコード</param>
        /// <param name="insertTargetColumns">insert実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2, t.Col3 }</c>」</param>
        /// <param name="updateTargetColumns">update実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col2, t.Col3 }</c>」</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入または更新された行数</returns>
        public static Task<int> InsertOrUpdateAsync<T>(this IDbTransaction transaction, T data, Expression<Func<T, dynamic>>? insertTargetColumns = null, Expression<Func<T, dynamic>>? updateTargetColumns = null, int? timeout = null)
            where T : notnull
        {
            return new QueryRunner(transaction, timeout).InsertOrUpdateAsync(data, insertTargetColumns, updateTargetColumns);
        }

        /// <summary>
        /// 指定されたレコードを非同期で一括挿入または更新します。(既存レコードはUPDATE／未存在ならINSERTを行います)
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="records">挿入または更新するレコード（複数件）</param>
        /// <param name="insertTargetColumns">insert実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2, t.Col3 }</c>」</param>
        /// <param name="updateTargetColumns">update実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col2, t.Col3 }</c>」</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入または更新された行数</returns>
        public static Task<int> InsertOrUpdateRowsAsync<T>(this IDbTransaction transaction, IEnumerable<T> records, Expression<Func<T, dynamic>>? insertTargetColumns = null, Expression<Func<T, dynamic>>? updateTargetColumns = null, int? timeout = null)
            where T : notnull
        {
            return new QueryRunner(transaction, timeout).InsertOrUpdateRowsAsync(records, insertTargetColumns, updateTargetColumns);
        }


        /// <summary>
        /// 指定された条件にマッチするレコードについて、指定されたカラムの値を非同期で更新します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="values">更新対象カラム・更新値を初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Value1 = 99, Flg = true }</c>」</param>
        /// <param name="where">更新対象レコードの条件</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>更新された行数</returns>
        public static Task<int> UpdateAsync<T>(this IDbTransaction transaction, Expression<Func<T>> values, Expression<Func<T, bool>> where, int? timeout = null)
            where T : notnull
        {
            return new QueryRunner(transaction, timeout).UpdateAsync(values, where);
        }

        /// <summary>
        /// 指定されたレコードを非同期で更新します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="data">更新するレコード</param>
        /// <param name="targetColumns">値更新対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>更新された行数</returns>
        public static Task<int> UpdateAsync<T>(this IDbTransaction transaction, T data, Expression<Func<T, dynamic>>? targetColumns = null, int? timeout = null)
            where T : notnull
        {
            return new QueryRunner(transaction, timeout).UpdateAsync(data, targetColumns);
        }


        /// <summary>
        /// 指定された条件にマッチするレコードを非同期で削除します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="where">削除対象レコードの条件</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>削除された行数</returns>
        public static Task<int> DeleteAsync<T>(this IDbTransaction transaction, Expression<Func<T, bool>> where)
            where T : notnull
        {
            return new QueryRunner(transaction, null).DeleteAsync(where);
        }

        /// <summary>
        /// 指定された条件にマッチするレコードを非同期で削除します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="where">削除対象レコードの条件</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>削除された行数</returns>
        public static Task<int> DeleteAsync<T>(this IDbTransaction transaction, Expression<Func<T, bool>> where, int? timeout = null)
            where T : notnull
        {
            return new QueryRunner(transaction, timeout).DeleteAsync(where);
        }

        /// <summary>
        /// 指定されたレコードを非同期で削除します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="data">削除するレコード</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <returns>削除された行数</returns>
        public static Task<int> DeleteAsync<T>(this IDbTransaction transaction, T data, int? timeout = null)
            where T : notnull
        {
            return new QueryRunner(transaction, timeout).DeleteAsync(data);
        }

        /// <summary>
        /// 指定されたテーブルの全レコードを非同期で削除します。
        /// </summary>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        public static Task TruncateAsync<T>(this IDbTransaction transaction, int? timeout = null)
            where T : notnull
        {
            return new QueryRunner(transaction, timeout).TruncateAsync<T>();
        }
    }
}
