using System;

namespace DapperAid.DataAnnotations
{
    /// <summary>
    /// SelectSQL生成内容をカスタマイズする属性です。
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class SelectSqlAttribute : Attribute
    {
        /// <summary>SelectSQL文の冒頭に設定する文字列、既定では「select」。必要に応じ「select distinct」「select TOP 10」「with XXX(...) as (....) select」などを指定してください。</summary>
        public string? Beginning { get; set; }

        /// <summary>SelectSQL文のWhere条件として必ず指定するSQL句（「deleted_at is null」など）があれば設定します。</summary>
        public string? BaseWhereClauses { get; set; }

        /// <summary>SelectSQL文でgroup by句を生成しKey項目のカラムでグループ化する場合、trueを指定します。</summary>
        public bool GroupByKey { get; set; }

        /// <summary>SelectSQL文の末尾に、otherClausesが未指定だった場合にデフォルトで付加するSQL句（orderBy条件/limit/offset指定など）があれば設定します。</summary>
        public string? DefaultOtherClauses { get; set; }
    }
}
