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
    }
}
