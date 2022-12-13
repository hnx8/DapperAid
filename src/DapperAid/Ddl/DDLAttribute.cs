using System;
using System.Linq;
using System.Reflection;
using System.Text;

namespace DapperAid.Ddl
{
    /// <summary>
    /// （開発/テスト向け支援機能用）
    /// DDL生成内容をカスタマイズする属性です。
    /// </summary>
    /// <remarks>
    /// DapperAid本体の機能では参照しない属性です。設定しなくても動作に支障ありません。
    /// </remarks>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Class, AllowMultiple = true)]
    public class DDLAttribute : Attribute
    {
        /// <summary>DBデータ型/制約等の指定内容</summary>
        internal string Specifics { get; private set; }

        /// <summary>
        /// プロパティに対応するカラムのDBデータ型や制約各種を指定します。
        /// </summary>
        /// <param name="specifics">指定内容。プロパティへの指定例：「<c>varchar2(20) default 'x' not null</c>」、クラスへの指定例：「<c>foreign key (c1,c2) references mastertbl(c1,c2)</c>」</param>
        public DDLAttribute(string specifics)
        {
            this.Specifics = specifics;
        }

        #region 開発/テスト向け支援機能 ----------------------------------------

        /// <summary>
        /// テーブルマッピングクラスの定義内容をもとに、CreateTableのSQLを生成します。
        /// </summary>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <param name="queryBuilder">通常はnull、カラム名等のエスケープルールが特殊なDBMS(Access等)の場合はそのDBMSに応じたQueryBuilderオブジェクト</param>
        /// <returns>SQL文</returns>
        /// <remarks>
        /// 以下の設定内容をもとにSQLが生成されます。
        /// <para>(1)テーブル名：クラス名または[Table("テーブル名")]属性の指定内容</para>
        /// <para>(2)カラム名  ：プロパティ名または[Column("カラム名")]属性の指定内容</para>
        /// <para>(3)カラム型等：プロパティに対し指定された[DDL("データ型 制約各種(default/notNull/check等")]属性の内容</para>
        /// <para>(4)PK指定    ：[Key]属性が指定されたすべてのプロパティをPrimaryKeyとして取り扱う</para>
        /// <para>(5)FK指定等  ：クラスに対し指定された[Ddl("制約各種(foreignKey/unique等)")]属性の内容</para>
        /// </remarks>
        public static string GenerateCreateSQL<T>(QueryBuilder? queryBuilder = null)
            where T : notnull
        {
            var builder = (queryBuilder ?? QueryBuilder.DefaultInstance);
            var table = builder.GetTableInfo<T>();

            var sb = new StringBuilder();
            sb.Append("create table " + table.Name + " (");
            // 各列の定義を生成
            var delimiter = "";
            foreach (var column in table.Columns.Where(c => c.IsKey || c.Select || c.Insert || c.Update))
            {
                sb.AppendLine(delimiter);
                delimiter = ",";
                sb.Append(' ').Append(column.Name);
                foreach (var ddlAttr in column.PropertyInfo.GetCustomAttributes<DDLAttribute>(true))
                {
                    sb.Append(' ').Append(ddlAttr.Specifics.Trim());
                }
            }
            // primary keyの定義を生成
            var keyColumns = table.Columns.Where(c => c.IsKey).ToArray();
            if (keyColumns.Any())
            {
                sb.AppendLine(delimiter);
                sb.Append(" primary key(");
                delimiter = "";
                foreach (var column in keyColumns)
                {
                    sb.Append(delimiter);
                    delimiter = ", ";
                    sb.Append(' ').Append(column.Name);
                }
                sb.Append(")");
                delimiter = ",";
            }
            // その他表制約を生成
            foreach (var tableAttr in typeof(T).GetCustomAttributes<DDLAttribute>(true))
            {
                sb.AppendLine(delimiter);
                delimiter = ",";
                sb.Append(tableAttr.Specifics.Trim().Trim(','));
            }
            sb.AppendLine("");
            //
            sb.AppendLine(")");
            return sb.ToString();
        }

        /// <summary>
        /// テーブルマッピングクラスの定義内容をもとに、テーブル定義内容のタブ区切りテキストを生成します。
        /// </summary>
        /// <param name="queryBuilder">通常はnull、カラム名等のエスケープルールが特殊なDBMS(Access等)の場合はそのDBMSに応じたQueryBuilderオブジェクト</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>テーブル定義内容のタブ区切りテキスト</returns>
        /// <remarks>
        /// 以下の内容のTSVが生成されます。
        /// <para>・基本情報（テーブルにマッピングされたクラス名、取得元/更新先テーブル、SelectSQL生成例）</para>
        /// <para>・各列の情報（表示名, プロパティ名, プロパティ型, Keyか, DBカラム名, DB型, Insert設定値, Update設定値)</para>
        /// </remarks>
        public static string GenerateTableDefTSV<T>(QueryBuilder? queryBuilder = null)
            where T : notnull
        {
            var builder = (queryBuilder ?? QueryBuilder.DefaultInstance);
            var table = builder.GetTableInfo<T>();

            Func<string, string> escape = (s) =>
            {
                return (string.IsNullOrWhiteSpace(s)
                    ? string.Empty
                    : s.Replace('\t', ' ').Replace("\r\n", " ").Replace('\r', ' ').Replace('\n', ' ').Trim());
            };

            var sb = new StringBuilder();
            sb.AppendLine(typeof(T).Name);
            sb.AppendLine("\tTable:\t" + escape(table.Name));
            sb.AppendLine("\t-------\t"
                + "DisplayName\t"
                + "Property\t"
                + "DataType\t"
                + "Key\t"
                + "Column\t"
                + "DbType\t"
                + "Insert値\t"
                + "Update値");
            foreach (var column in table.Columns.Where(c => c.IsKey || c.Select || c.Insert || c.Update))
            {
                sb.Append("\tColumn:");
                sb.Append("\t");
                sb.Append(string.Join("/",
                    column.PropertyInfo.GetCustomAttributes<System.ComponentModel.DataAnnotations.DisplayAttribute>(true).Select(a => a.Name).ToArray()));
                sb.Append("\t");
                sb.Append(column.PropertyInfo.Name);
                sb.Append("\t");
                sb.Append(column.PropertyInfo.PropertyType.ToString());
                sb.Append("\t");
                sb.Append(
                    (column.IsKey ? "Key" : (column.ConcurrencyCheck ? "ConcurrencyCheck" : ""))
                    + (table.RetrieveInsertedIdColumn == column ? "(ID)" : ""));
                sb.Append("\t");
                sb.Append(column.Alias == null ? column.PropertyInfo.Name : escape(column.Name));
                sb.Append("\t");
                sb.Append(string.Join("/",
                    column.PropertyInfo.GetCustomAttributes<DDLAttribute>(true).Select(a => a.Specifics).ToArray()));
                sb.Append("\t");
                sb.Append(column.Insert ? escape(column.InsertSQL ?? ("@" + column.PropertyInfo.Name)) : "(default)");
                sb.Append("\t");
                sb.Append(column.Update ? escape(column.UpdateSQL ?? ("@" + column.PropertyInfo.Name)) : "(notupdate)");
                sb.AppendLine();
            }
            sb.AppendLine("\t-------\t");
            sb.AppendLine("\tSQL:\t" + escape(builder.BuildSelect<T>() + builder.BuildSelectOrderByEtc<T>()));
            sb.AppendLine();

            return sb.ToString();
        }

        #endregion
    }
}
