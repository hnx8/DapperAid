using System;

namespace DapperAid.DataAnnotations
{
    /// <summary>
    /// 指定カラムのInsertSQL生成内容をカスタマイズする属性です。
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class InsertSqlAttribute : Attribute
    {
        /// <summary>このカラムについてのInsert列指定を生成するか否か</summary>
        public bool SetValue { get; private set; }

        /// <summary>設定値を直接指定する場合、設定値のSQL</summary>
        public string Sql { get; private set; }

        /// <summary>Insert時にこのカラムに採番されたIDを取得する場合、trueを指定します</summary>
        public bool RetrieveInsertedId { get; set; }

        /// <summary>
        /// InsertSQL生成時にこのカラムの列指定を生成するか否かを設定します。
        /// </summary>
        /// <param name="setValue">設定値を指定せずDBのデフォルト値でInsertする場合、falseを指定します。</param>
        public InsertSqlAttribute(bool setValue)
        {
            this.SetValue = setValue;
            this.Sql = null;
        }

        /// <summary>
        /// InsertSQL生成時にこのカラムに設定する値をSQLで直接指定します。
        /// </summary>
        /// <param name="sql">設定値のSQL。必要に応じ「current_timestamp」「nextval(シーケンス名)」といった内容を指定します。</param>
        public InsertSqlAttribute(string sql)
        {
            this.SetValue = true;
            this.Sql = sql;
        }
    }
}
