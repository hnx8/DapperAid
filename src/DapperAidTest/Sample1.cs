using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.SQLite;
using System.Diagnostics;
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
            public DateTime? UpdatedAt { get; private set; } = DateTime.Now;

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
                    if (cmd.Parameters?.Count > 0)
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
                    otherClauses: "--FOR UPDATE"); // SQLite doesnot support "FOR UPDATE", so commented out 

                var targetMember = new Member { Id = 8, Name = "LockTest" };
                var lockedMember = connection.Select(
                    () => targetMember, // where [Key] or [ConcurrencyCheck] is set
                    otherClauses: "--FOR UPDATE"); // SQLite doesnot support "FOR UPDATE", so commented out 


                // select records --------------------
                IReadOnlyList<Member> list1 = connection.Select<Member>();

                IReadOnlyList<Member> list2 = connection.Select<Member>(
                    r => r.Name == "TEST");

                IReadOnlyList<Member> list3 = connection.Select<Member>(
                    r => r.Name != "TEST",
                    r => new { r.Id, r.Name });

                IReadOnlyList<Member> list4 = connection.Select<Member>(
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

                // insert records -------------------
                int insertMulti = connection.InsertRows(new[] {
                    new Member { Name = "MultiInsert1", Tel = null },
                    new Member { Name = "MultiInsert2", Tel = "999-999-9999" },
                    new Member { Name = "MultiInsert3", Tel = "88-8888-8888" },
                });

                // update record ---------------------
                rec1.Id = 1;
                rec1.Name = "Updatetest";
                int update1 = connection.Update(rec1);

                rec2.Id = 2;
                rec2.Tel = "6666-66-6666";
                int update2 = connection.Update(rec2, r => new { r.Tel });

                int update3 = connection.Update(
                    () => new Member { Name = "updateName" },
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

        /// <summary>
        /// ToSQLクラス指定によるSQL条件の追加テスト
        /// </summary>
        [TestMethod]
        public void ToSqlTest()
        {
            QueryBuilder.DefaultInstance = new QueryBuilder.SQLite();

            using (IDbConnection connection = GetSqliteDbConnection())
            {
                var createTableSql = DDLAttribute.GenerateCreateSQL<Member>();
                connection.Execute(createTableSql);

                var list1 = connection.Select<Member>(r =>
                    (r.Name == ToSql.In(new[] { "A", "B" })
                    || r.Name != ToSql.Like("%TEST%")
                    || r.Name == ToSql.Between("1", "5")
                    || DateTime.Now < r.CreatedAt));

            }
        }
    }
}
