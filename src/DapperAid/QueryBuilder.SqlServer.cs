using DapperAid.Helpers;

namespace DapperAid
{
    partial class QueryBuilder
    {
        /// <summary>
        /// SQLServer用のSQL/パラメータを組み立てるクラスです。
        /// </summary>
        public class SqlServer : QueryBuilder
        {
            /// <summary>自動連番値を取得するSQL句として、セミコロンで区切った別のSQL文を付加します。</summary>
            protected override string GetInsertedIdReturningSql<T>(TableInfo.Column column)
            {
                return "; select SCOPE_IDENTITY()";
            }
        }
    }
}
