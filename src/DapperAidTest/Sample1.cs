using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.SQLite;
using System.Diagnostics;
using System.Threading;
using Dapper;
using DapperAid;
using DapperAid.DataAnnotations;
using DapperAid.DbAccess;
using DapperAid.Ddl;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DapperAidTest
{
    /// <summary>
    /// DDL生成/実行の動作サンプル
    /// </summary>
    [TestClass]
    public partial class Sample1
    {
        /// <summary>
        /// Sample Table
        /// </summary>
        [Table("Members")]
        [SelectSql(DefaultOtherClauses = "order by Id")]
        class Member
        {
            [Key]
            [InsertSql(false, RetrieveInsertedId = true)]
            [DDL("INTEGER")]
            public int Id { get; set; }

            public string Name { get; set; }

            [Column("Phone_No")]
            public string Tel { get; set; }

            [InsertSql("CURRENT_TIMESTAMP"), UpdateSql(false)]
            public DateTime? CreatedAt { get; set; }

            [InsertSql("CURRENT_TIMESTAMP"), UpdateSql("CURRENT_TIMESTAMP"), ConcurrencyCheck]
            public DateTime? UpdatedAt { get; private set; }

            [NotMapped]
            public string TemporaryPassword { get; set; }
        }

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
                    if (cmd?.Parameters?.Count > 0)
                    {
                        for (int i = 0; i < cmd.Parameters.Count; i++)
                        {
                            var param = cmd.Parameters[i];
                            Trace.WriteLine($"    {param.ParameterName} = {param.Value}");
                        }
                    }
                });
        }

        /// <summary>
        /// Operation Sample
        /// </summary>
        [TestMethod]
        public void Tutorial()
        {
            QueryBuilder.DefaultInstance = new QueryBuilder.SQLite();

            using (IDbConnection connection = GetSqliteDbConnection())
            {
                // optional : create table -----------
                var createTableSql = DDLAttribute.GenerateCreateSQL<Member>();
                connection.Execute(createTableSql);
                // ->  create table Members
                //     (
                //      "Id" INTEGER, -- identity
                //      "Name",
                //      Phone_No,
                //      "CreatedAt",
                //      "UpdatedAt",
                //      primary key( "Id")
                //     )
                var tableInfoTsv = DDLAttribute.GenerateTableDefTSV<Member>();
                Trace.WriteLine(tableInfoTsv);


                // select 1 record -------------------
                Member select1 = connection.Select(
                    () => new Member { Id = 5 });

                Member select2 = connection.Select(
                    () => new Member { Id = 6 },
                    r => new { r.Id, r.Name });

                // (for update etc.)
                Member selectForUpdate = connection.Select(
                    () => new Member { Id = 7 },
                    otherClauses: "--FOR UPDATE"); // SQLite doesn't support "FOR UPDATE", so commented out 

                var targetMember = new Member { Id = 8, Name = "LockTest" };
                var lockedMember = connection.Select(
                    () => targetMember, // where [Key] or [ConcurrencyCheck] is set
                    otherClauses: "--FOR UPDATE"); // SQLite doesn't support "FOR UPDATE", so commented out 

                // (with SqlExpr)
                Member select3 = connection.Select(
                    () => new Member { Id = SqlExpr.Eval<int>("(SELECT MAX(id) FROM Members)") }
                );

                // select records --------------------
                IReadOnlyList<Member> list1 = connection.Select<Member>();

                IReadOnlyList<Member> list2 = connection.Select<Member>(
                    r => r.Name == "TEST");

                IReadOnlyList<Member> list3 = connection.Select<Member>(
                    r => r.Name != "TEST",
                    r => new { r.Id, r.Name });

                IReadOnlyList<Member> list4 = connection.Select<Member>(
                    r => r.Tel != null,
                    $"ORDER BY {nameof(Member.Name)} LIMIT 5 OFFSET 10");

                IReadOnlyList<Member> list5 = connection.Select<Member>(
                    r => r.Tel != null,
                    r => new { r.Id, r.Name },
                    $"ORDER BY {nameof(Member.Name)} LIMIT 5 OFFSET 10");

                // count -----------------------------

                ulong count1 = connection.Count<Member>();

                ulong count2 = connection.Count<Member>(
                    r => (r.Id >= 3 && r.Id <= 9));


                // insert ----------------------------
                var rec1 = new Member { Name = "InsertTest", Tel = "177" };
                int insert1 = connection.Insert(rec1);

                var rec2 = new Member { Name = "ParticularColumnOnly1", CreatedAt = null };
                int insert2 = connection.Insert(rec2,
                    r => new { r.Name, r.CreatedAt });

                var rec3 = new Member { Name = "IdentityTest", Tel = "7777" };
                int insert3 = connection.InsertAndRetrieveId(rec3);
                Trace.WriteLine("insertedID=" + rec3.Id); // -> 3

                int insertX = connection.Insert(
                    () => new Member { Id = 888, Name = "ParticularColumnOnly2" });

                // (with SqlExpr)
                string nameExample = "SqlExpr.Eval Usage Example";
                int insertX2 = connection.Insert(
                    () => new Member { Name = SqlExpr.Eval<string>("Upper(", nameExample, ")") });

                // insert records -------------------
                int insertMulti = connection.InsertRows(new[] {
                    new Member { Name = "MultiInsert1", Tel = null },
                    new Member { Name = "MultiInsert2", Tel = "999-999-9999" },
                    new Member { Name = "MultiInsert3", Tel = "88-8888-8888" },
                });

                // update record ---------------------
                rec1 = connection.Select(() => new Member { Id = 1 });
                rec1.Name = "Updatetest";
                int update1 = connection.Update(rec1);

                rec2.Id = 2;
                rec2.Tel = "6666-66-6666";
                int update2 = connection.Update(rec2, r => new { r.Tel });

                int update3 = connection.Update(
                    () => new Member { Name = "updateName" },
                    r => r.Tel == "55555-5-5555");

                // (with SqlExpr)
                int update4 = connection.Update(
                    () => new Member { Name = SqlExpr.Eval<String>("SUBSTR(name,", 1, ",", 4, ")") },
                    r => r.Tel == "55555-5-5555");

                //　delete record 
                var delRec = new Member { Id = 999, Name = "XXXX" };
                int delete1 = connection.Delete(delRec);

                int delete2 = connection.Delete<Member>(
                    r => r.Name == null);

                // truncate
                connection.Truncate<Member>();

            }
        }


        [SelectSql()]
        class Ver0_9TestColumns
        {
            public string Name { get; private set; }
            [Key]
            public string Tel { get; private set; }
            [Column("CURRENT_TIMESTAMP")]
            public DateTime Now { get; set; }
        }

        [SelectSql(GroupByKey = true, DefaultOtherClauses = "")]
        class Ver0_9TestColumns2
        {
            [Key]
            public string Tel { get; private set; }
            [Column("COUNT(*)")]
            public int Count { get; set; }
        }


        /// <summary>
        /// Ver0.9追加機能（型指定された特定列のみ取得）
        /// </summary>
        [TestMethod]
        public void Ver0_9Test()
        {
            QueryBuilder.DefaultInstance = new QueryBuilder.SQLite();

            using (IDbConnection connection = GetSqliteDbConnection())
            {
                var createTableSql = DDLAttribute.GenerateCreateSQL<Member>();
                connection.Execute(createTableSql);

                // Get only specific columns specified by type
                IReadOnlyList<Ver0_9TestColumns> ver09list1 = connection.Select<Member, Ver0_9TestColumns>();

                // Get only specific columns #2 (group by)
                IReadOnlyList<Ver0_9TestColumns2> ver09list2 = connection.Select<Member, Ver0_9TestColumns2>();

            }
        }


        /// <summary>
        /// SqlExprクラス指定によるSQL条件の追加テスト
        /// </summary>
        [TestMethod]
        public void SqlExprTest()
        {
            QueryBuilder.DefaultInstance = new QueryBuilder.SQLite();

            using (IDbConnection connection = GetSqliteDbConnection())
            {
                var createTableSql = DDLAttribute.GenerateCreateSQL<Member>();
                connection.Execute(createTableSql);

                var list1 = connection.Select<Member>(r =>
                    (r.Name == ToSql.In(new[] { "A", "B" })
                    || r.Name != SqlExpr.Like("%TEST%")
                    || r.Name == SqlExpr.Between("1", "5")
                    || DateTime.Now < r.CreatedAt));

                var list2 = connection.Select<Member>(r =>
                    (r.Id == SqlExpr.In<int>("SELECT MAX(id) FROM Members")
                    || SqlExpr.Eval("EXISTS(SELECT * FROM Members m2 WHERE id=sqlite_version())")
                    || r.Id == SqlExpr.Eval<int>("MAX(", 1, ",", 2, ",", 3, ")")
                    ));
            }
        }

        /// <summary>
        /// Sample Table #2
        /// </summary>
        [SelectSql(DefaultOtherClauses = "order by Code")]
        class Dept
        {
            [Key]
            public int Code { get; set; }

            public string Name { get; set; }

            [InsertSql("CURRENT_TIMESTAMP"), UpdateSql(false)]
            public DateTime? CreatedAt { get; set; }

            [InsertSql(false), UpdateSql("CURRENT_TIMESTAMP")]
            public DateTime? UpdatedAt { get; private set; }

            public override string ToString()
            {
                return Code.ToString() + ":" + Name + "(" + CreatedAt?.ToString("G") + " -> " + UpdatedAt?.ToString("G") + ")";
            }
        }

        /// <summary>
        /// Upsert Sample
        /// </summary>
        [TestMethod]
        public void UpsertTest()
        {
            QueryBuilder.DefaultInstance = new QueryBuilder.SQLite()
            {
                MultiInsertRowsPerQuery = 2
            };

            using (IDbConnection connection = GetSqliteDbConnection())
            {
                var createTableSql = DDLAttribute.GenerateCreateSQL<Dept>();
                connection.Execute(createTableSql);

                // insert records -------------------
                var insertData = new[] {
                    new Dept { Code = 110, Name = "Business"},
                    new Dept { Code = 210, Name = "Accounting"},
                    new Dept { Code = 220, Name = "Finance"},
                    new Dept { Code = 230, Name = "Purchasing"},
                };
                int insertMulti = connection.InsertRows(insertData);

                var inserted = connection.Select<Dept>();
                Assert.AreEqual(4, inserted.Count);
                foreach (var rec in inserted) { Trace.WriteLine(rec.ToString()); }

                // upsert records -------------------
                Thread.Sleep(1000);
                var upsertRow = new Dept { Code = 230, Name = "Buying" };
                int upsertSingle = connection.InsertOrUpdate(upsertRow);
                Thread.Sleep(1000);

                var upsertData = new[] {
                    new Dept { Code = 110, Name = "Sales"},
                    new Dept { Code = 120, Name = "Marketing"},
                    new Dept { Code = 130, Name = "Publicity"},
                };
                int upsertMulti = connection.InsertOrUpdateRows(upsertData);

                var upserted = connection.Select<Dept>(); // returns 6 rows (1 rows updated, 2 rows inserted)
                Assert.AreEqual(6, upserted.Count);
                foreach (var rec in upserted) { Trace.WriteLine(rec.ToString()); }

                // SQL generation according to the dialect of each DBMS -------------------
                Trace.WriteLine("<Oracle>");
                var oracleBuilder = new QueryBuilder.Oracle() { MultiInsertRowsPerQuery = 2 };
                foreach (var sql in oracleBuilder.BuildMultiInsert(insertData)) { Trace.WriteLine(sql); }
                Trace.WriteLine(oracleBuilder.BuildUpsert<Dept>());
                foreach (var sql in oracleBuilder.BuildMultiUpsert(upsertData)) { Trace.WriteLine(sql); }

                Trace.WriteLine("<SqlServer>");
                var mssqlBuilder = new QueryBuilder.SqlServer() { MultiInsertRowsPerQuery = 2 };
                Trace.WriteLine(mssqlBuilder.BuildUpsert<Dept>());
                foreach (var sql in mssqlBuilder.BuildMultiUpsert(upsertData)) { Trace.WriteLine(sql); }

                Trace.WriteLine("<MySql>");
                var mysqlBuilder = new QueryBuilder.MySql() { MultiInsertRowsPerQuery = 2 };
                Trace.WriteLine(mysqlBuilder.BuildUpsert<Dept>());
                foreach (var sql in mysqlBuilder.BuildMultiUpsert(upsertData)) { Trace.WriteLine(sql); }
            }
        }
    }
}
