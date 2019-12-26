using System;
using System.Linq;
using System.Linq.Expressions;
using DapperAid;
using DapperAid.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DapperAidTest.Helpers
{
    [TestClass]
    public class ExpressionHelperTest
    {
        class TestTable
        {
            public string Col1 { get; set; }
            public string Col2 { get; set; }
            public string Col3 { get; set; }
            public string Col4 { get; set; }
            public string Col5 { get; set; }
        }

        [TestMethod]
        public void GetMemberNames_MemberInitExpression()
        {
            var actual = ExpressionHelper.GetMemberNames(((Expression<Func<TestTable>>)(() => new TestTable() { Col1 = "1", Col3 = "3" })).Body).ToArray();
            CollectionAssert.AreEquivalent(new[] { "Col1", "Col3" }, actual);
        }
        [TestMethod]
        public void BindValues_MemberInitExpression()
        {
            var actual = QueryBuilder.DefaultInstance.BindValues<TestTable>(() => new TestTable() { Col1 = "1", Col3 = "3" });
            CollectionAssert.AreEquivalent(new[] { "Col1", "Col3" }, actual.ParameterNames.ToArray());
            Assert.AreEqual("1", actual.Get<object>("Col1"));
            Assert.AreEqual("3", actual.Get<object>("Col3"));
        }

        [TestMethod]
        public void GetMemberNames_NewExpression()
        {
            var actual = ExpressionHelper.GetMemberNames(((Expression<Func<dynamic>>)(() => new { Col1 = "1", Col4 = "4" })).Body).ToArray();
            CollectionAssert.AreEquivalent(new[] { "Col1", "Col4" }, actual);
        }
    }
}
