using System;
using System.Linq;
using System.Linq.Expressions;
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
        public void GetMemberNames_NewExpression()
        {
            var actual = ExpressionHelper.GetMemberNames(((Expression<Func<dynamic>>)(() => new { Col1 = "1", Col4 = "4" })).Body).ToArray();
            CollectionAssert.AreEquivalent(new[] { "Col1", "Col4" }, actual);
        }
    }
}
