using System;

namespace DapperAid.DataAnnotations
{
    /// <summary>
    /// 指定カラムのUpdate値SQL生成内容をカスタマイズする属性です。
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class UpdateValueAttribute : Attribute
    {
        /// <summary>このカラムについてのUpdate Set句を生成するか否か</summary>
        public bool SetValue { get; private set; }

        /// <summary>更新値を直接指定する場合、更新値のSQL</summary>
        public string? Sql { get; private set; }

        /// <summary>
        /// UpdateSQL生成時にこのカラムのUpdate Set句を生成するか否かを設定します。
        /// </summary>
        /// <param name="setValue">Set句を生成せず値を更新しない場合、falseを指定します。</param>
        public UpdateValueAttribute(bool setValue)
        {
            this.SetValue = setValue;
            this.Sql = null;
        }

        /// <summary>
        /// UpdateSQL生成時にこのカラムに設定する更新値をSQLで直接指定します。
        /// </summary>
        /// <param name="sql">更新値のSQL。必要に応じ「current_timestamp」「nextval(シーケンス名)」といった内容を指定します。</param>
        public UpdateValueAttribute(string sql)
        {
            this.SetValue = true;
            this.Sql = sql;
        }
    }

#pragma warning disable 1591 // 以下のリネーム前のバーライドについてはわざわざドキュメントコメントを書かない。コンパイルの警告(CS1591)も抑止する
    /// <summary>
    /// Obsolete：属性名がUpdateValueAttributeへ変更されました。
    /// </summary>
    [Obsolete("'UpdateSqlAttribute' is Renamed to 'UpdateValueAttribute' since v1.0.0.")]
    public class UpdateSqlAttribute : UpdateValueAttribute
    {
        [Obsolete("'UpdateSqlAttribute' is Renamed to 'UpdateValueAttribute' since v1.0.0.")]
        public UpdateSqlAttribute(bool setValue) : base(setValue) { }
        [Obsolete("'UpdateSqlAttribute' is Renamed to 'UpdateValueAttribute' since v1.0.0.")]
        public UpdateSqlAttribute(string sql) : base(sql) { }
    }
#pragma warning restore 1591
}
