using System;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.SQLite;
using System.Diagnostics;
using Dapper;
using DapperAid;
using DapperAid.DataAnnotations;
using DapperAid.DbAccess;
using DapperAid.Ddl;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DapperAidTest.Misc
{
    /// <summary>
    /// 楽観的排他の確認用サンプル
    /// </summary>
    [TestClass]
    public partial class ConcurrencyCheckTest
    {
        /// <summary>
        /// LoggableDbConnection Sample
        /// </summary>
        private IDbConnection GetSqliteDbConnection()
        {
            var connectionSb = new SQLiteConnectionStringBuilder { DataSource = ":memory:" };
            var conn = new SQLiteConnection(connectionSb.ToString());
            conn.Open();

            return new LoggableDbConnection(conn,
                errorLogger: (ex, cmd) =>
                {
                    Trace.WriteLine(ex.ToString() + (cmd != null ? ":" + cmd.CommandText : null));
                },
                traceLogger: (text, mSec, cmd) =>
                {
                    Trace.WriteLine(text + "(" + mSec + "ms)" + (cmd != null ? ":" + cmd.CommandText : null));
                    if (cmd.Parameters?.Count > 0)
                    {
                        for (int i = 0; i < cmd.Parameters.Count; i++)
                        {
                            var param = cmd.Parameters[i];
                            Trace.WriteLine($"    {param.ParameterName} = {param.Value}");
                        }
                    }

                    // テスト実行結果検証用
                    if (cmd != null)
                    {
                        ExecutedSQL = cmd.CommandText;
                    }
                });
        }
        /// <summary>直近に実行されたSQL</summary>
        private string ExecutedSQL;

        [TestMethod]
        public void ConcurrencyTest()
        {
            QueryBuilder.DefaultInstance = new QueryBuilder.SQLite();

            using (IDbConnection connection = GetSqliteDbConnection())
            {
                var createTableSql = DDLAttribute.GenerateCreateSQL<ConcurrencyTestTable>();
                connection.Execute(createTableSql);

                var testData = new ConcurrencyTestTable
                {
                    Key = "1",
                    Data = "xxx",
                    UpdatedAt = new DateTime(2019, 5, 1, 0, 0, 0),
                    Version = 2
                };

                connection.Select<ConcurrencyTestTable>(() => testData);
                Assert.AreEqual(
                    "select \"Key\", \"Data\", \"UpdatedAt\", \"Version\" from \"ConcurrencyTestTable\" where \"Key\"=@Key and \"UpdatedAt\"=@UpdatedAt and \"Version\"=@Version",
                    ExecutedSQL);

                connection.Update(testData);
                Assert.AreEqual(
                    "update \"ConcurrencyTestTable\" set \"Data\"=@Data, \"UpdatedAt\"=CURRENT_TIMESTAMP, \"Version\"=@Version where \"Key\"=@Key and \"UpdatedAt\"=@UpdatedAt",
                    ExecutedSQL);

                connection.Delete(testData);
                Assert.AreEqual(
                    "delete from \"ConcurrencyTestTable\" where \"Key\"=@Key and \"UpdatedAt\"=@UpdatedAt and \"Version\"=@Version",
                    ExecutedSQL);

            }
        }

        class ConcurrencyTestTable
        {
            [Key]
            public string Key { get; set; }

            public string Data { get; set; }

            [UpdateSql("CURRENT_TIMESTAMP"), ConcurrencyCheck]
            public DateTime? UpdatedAt { get; set; }

            [ConcurrencyCheck]
            public int Version { get; set; }

            // Versionの保持値は、select/deleteではwhere条件、updateでは更新値として用いられる
            // Key,UpdatedAtの保持値は、select/update/deleteともwhere条件として用いられる
        }
    }
}
