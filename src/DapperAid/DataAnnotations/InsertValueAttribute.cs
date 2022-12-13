using System;

namespace DapperAid.DataAnnotations
{
    /// <summary>
    /// 指定カラムのInsert値SQL生成内容をカスタマイズする属性です。
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class InsertValueAttribute : Attribute
    {
        /// <summary>このカラムについてのInsert列指定を生成するか否か</summary>
        public bool SetValue { get; private set; }

        /// <summary>設定値を直接指定する場合、設定値のSQL</summary>
        public string? Sql { get; private set; }

        /// <summary>Insert時にこのカラムに採番されたIDを取得する場合、trueを指定します</summary>
        public bool RetrieveInsertedId { get; set; }

        /// <summary>
        /// InsertSQL生成時にこのカラムの列指定を生成するか否かを設定します。
        /// </summary>
        /// <param name="setValue">設定値を指定せずDBのデフォルト値でInsertする場合、falseを指定します。</param>
        public InsertValueAttribute(bool setValue)
        {
            this.SetValue = setValue;
            this.Sql = null;
        }

        /// <summary>
        /// InsertSQL生成時にこのカラムに設定する値をSQLで直接指定します。
        /// </summary>
        /// <param name="sql">設定値のSQL。必要に応じ「current_timestamp」「nextval(シーケンス名)」といった内容を指定します。</param>
        public InsertValueAttribute(string sql)
        {
            this.SetValue = true;
            this.Sql = sql;
        }
    }

#pragma warning disable 1591 // 以下のリネーム前のバーライドについてはわざわざドキュメントコメントを書かない。コンパイルの警告(CS1591)も抑止する
    /// <summary>
    /// Obsolete：属性名がInsertValueAttributeへ変更されました。
    /// </summary>
    [Obsolete("'InsertSqlAttribute' is Renamed to 'InsertValueAttribute' since v1.0.0.")]
    public class InsertSqlAttribute : InsertValueAttribute
    {
        [Obsolete("'InsertSqlAttribute' is Renamed to 'InsertValueAttribute' since v1.0.0.")]
        public InsertSqlAttribute(bool setValue) : base(setValue) { }
        [Obsolete("'InsertSqlAttribute' is Renamed to 'InsertValueAttribute' since v1.0.0.")]
        public InsertSqlAttribute(string sql) : base(sql) { }
    }
#pragma warning restore 1591
}
