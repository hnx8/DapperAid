using System.Reflection;
using DapperAid.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DapperAidTest.Helpers
{
    /// <summary>
    /// MemberAccessorの動作テストクラス
    /// </summary>
    [TestClass]
    public class MemberAccessorTest
    {
        class Table1
        {
            public string InstanceValue { get; set; }
            public static string StaticValue { get; set; }

            public string InstanceField;
            public static string StaticField;
        }

        [TestMethod]
        public void Testインスタンスプロパティ()
        {
            Table1.StaticValue = "STATIC";
            Table1 obj = new Table1()
            {
                InstanceValue = "INSTANCE"
            };

            PropertyInfo prop = obj.GetType().GetProperty("InstanceValue");
            object actual1 = MemberAccessor.GetValue(obj, prop);
            Assert.AreEqual("INSTANCE", actual1);
            MemberAccessor.SetValue(obj, prop, "CHANGEDVALUE");
            object actual2 = MemberAccessor.GetValue(obj, prop);
            Assert.AreEqual("CHANGEDVALUE", actual2);
        }

        [TestMethod]
        public void Test静的プロパティ()
        {
            Table1.StaticValue = "STATIC";
            Table1 obj = new Table1()
            {
                InstanceValue = "INSTANCE"
            };

            PropertyInfo prop = typeof(Table1).GetProperty("StaticValue");
            object actual1 = MemberAccessor.GetStaticValue(prop);
            Assert.AreEqual("STATIC", actual1);
        }

        [TestMethod]
        public void Testインスタンスフィールド()
        {
            Table1.StaticField = "STATIC-F";
            Table1 obj = new Table1()
            {
                InstanceField = "INSTANCE-F"
            };

            FieldInfo field = obj.GetType().GetField("InstanceField");
            object actual1 = MemberAccessor.GetValue(obj, field);
            Assert.AreEqual("INSTANCE-F", actual1);
        }

        [TestMethod]
        public void Test静的フィールド()
        {
            Table1.StaticField = "STATIC-F";
            Table1 obj = new Table1()
            {
                InstanceField = "INSTANCE-F"
            };

            FieldInfo field = typeof(Table1).GetField("StaticField");
            object actual1 = MemberAccessor.GetStaticValue(field);
            Assert.AreEqual("STATIC-F", actual1);
        }

        private class NullableTestTable
        {
            public int? Test1 { get; set; }
            public int? Test2 { get; set; }
            public bool HasValue { get; set; }
            public string FromStatic { get; set; }
        }

        [TestMethod]
        public void TestNull許容値型1()
        {
            var rec = new NullableTestTable { Test1 = 12, Test2 = null };
            Table1.StaticValue = "STATIC";

            var parameters = new Dapper.DynamicParameters();
            var sql1 = DapperAid.QueryBuilder.DefaultInstance.BuildWhere(parameters, () => new NullableTestTable
            {   // null許容値型の非null値のメソッド/プロパティを呼び出し
                Test1 = rec.Test1.GetValueOrDefault(),
                Test2 = rec.Test1.GetValueOrDefault(-555),
                HasValue = rec.Test1.HasValue,
                FromStatic = Table1.StaticValue,
            });
            Assert.AreEqual(12, parameters.Get<int>("Test1"));
            Assert.AreEqual(12, parameters.Get<int>("Test2"));
            Assert.AreEqual(true, parameters.Get<bool>("HasValue"));
            Assert.AreEqual("STATIC", parameters.Get<string>("FromStatic"));
        }

        [TestMethod]
        public void TestNull許容値型2()
        {
            var rec = new NullableTestTable { Test1 = 12, Test2 = null };
            Table1.StaticValue = null;

            var parameters = new Dapper.DynamicParameters();
            var sql1 = DapperAid.QueryBuilder.DefaultInstance.BuildWhere(parameters, () => new NullableTestTable
            {   // null許容値型のnull値のメソッド/プロパティを呼び出し
                Test1 = rec.Test2.GetValueOrDefault(),
                Test2 = rec.Test2.GetValueOrDefault(-555),
                HasValue = rec.Test2.HasValue,
                FromStatic = Table1.StaticValue,
            });
            Assert.AreEqual(0, parameters.Get<int>("Test1"));
            Assert.AreEqual(-555, parameters.Get<int>("Test2"));
            Assert.AreEqual(false, parameters.Get<bool>("HasValue"));
            Assert.AreEqual(sql1, " where \"Test1\"=@Test1 and \"Test2\"=@Test2 and \"HasValue\"=@HasValue and \"FromStatic\" is null");
            //Assert.AreEqual(parameters.Get<string>("FromStatic"), null); // nullなのでバインドされない
        }
    }
}
