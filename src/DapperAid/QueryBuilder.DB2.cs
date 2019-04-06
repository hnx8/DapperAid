using DapperAid.Helpers;

namespace DapperAid
{
    partial class QueryBuilder
    {
        /// <summary>
        /// DB2用のSQL/パラメータを組み立てるクラスです。
        /// </summary>
        public class DB2 : QueryBuilder
        {
            /// <summary>自動連番値を取得するSQL句として、セミコロンで区切った別のSQL文を付加します。</summary>
            protected override string GetInsertedIdReturningSql<T>(TableInfo.Column column)
            {
                var tableInfo = GetTableInfo<T>();
                return "; select IDENTITY_VAR_LOCAL() from " + tableInfo.Name;
            }
        }
    }
}
