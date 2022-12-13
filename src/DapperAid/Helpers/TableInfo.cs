using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using DapperAid.DataAnnotations;

namespace DapperAid.Helpers
{
    /// <summary>
    /// DapperAidでSQL生成に使用するテーブル情報のクラスです。
    /// </summary>
    public class TableInfo
    {
        /// <summary>SQL文で使用するテーブル名（識別子エスケープ済）、あるいはFrom句に指定する複数テーブル指定</summary>
        public string Name { get; private set; } = null!;

        /// <summary>マッピング対象全カラム</summary>
        public IReadOnlyList<Column> Columns { get; private set; } = null!;

        /// <summary>自動連番のカラム</summary>
        /// <remarks>Insert時に採番されたIDを取得するカラムがある場合のみ設定される</remarks>
        public Column? RetrieveInsertedIdColumn { get; private set; }

        /// <summary>SelectSQL生成内容カスタマイズ情報</summary>
        public SelectSqlAttribute? SelectSqlInfo { get; private set; }

        #region カラム情報クラス -----------------------------------------------
        /// <summary>
        /// DapperAidでSQL生成に使用するカラム情報のクラスです。
        /// </summary>
        public class Column
        {
            /// <summary>カラムのProperty情報</summary>
            public PropertyInfo PropertyInfo { get; private set; }

            /// <summary>キー項目か否か</summary>
            public bool IsKey { get; private set; }

            /// <summary>SQL文で使用するカラム名（識別子エスケープ済）、あるいはSQL式</summary>
            public string Name { get; private set; }

            /// <summary>レコードSelect時に値取得対象カラムとするか否か</summary>
            public bool Select { get; private set; }
            /// <summary>Select時に使用するカラム別名（識別子エスケープ済）、別名設定なしの場合はnull</summary>
            public string? Alias { get; private set; }

            /// <summary>レコード指定Insert時に値設定対象カラムとして指定するか否か</summary>
            public bool Insert { get; private set; }
            /// <summary>レコード指定Insert時に値として設定するSQL。nullの場合はパラメータバインド値による更新</summary>
            public string? InsertSQL { get; private set; }

            /// <summary>レコード指定Update時に楽観的同時実行チェック対象としてWhere条件を組み立てるか否か</summary>
            public bool ConcurrencyCheck { get; private set; }

            /// <summary>レコード指定Update時に値を更新するか否か</summary>
            public bool Update { get; private set; }
            /// <summary>レコード指定Update時に値として設定するSQL。nullの場合はパラメータバインド値による更新</summary>
            public string? UpdateSQL { get; private set; }

            /// <summary>
            /// インスタンスを生成します。
            /// </summary>
            /// <param name="prop">プロパティ情報</param>
            /// <param name="escapeMethod">SQL識別子（テーブル名/カラム名等）をエスケープするメソッド</param>
            internal Column(PropertyInfo prop, Func<string, string> escapeMethod)
            {
                this.PropertyInfo = prop;
                this.IsKey = (prop.GetCustomAttribute<KeyAttribute>() != null);

                var isMapped = (prop.GetCustomAttribute<NotMappedAttribute>() == null);

                var colAttr = prop.GetCustomAttribute<ColumnAttribute>();
                this.Name = (colAttr == null || string.IsNullOrWhiteSpace(colAttr.Name)) ? escapeMethod(prop.Name) : colAttr.Name;
                this.Select = prop.CanWrite && isMapped; // 2022.12 Setterなしのプロパティは自明でSELECT対象外カラムとする
                this.Alias = (colAttr == null || string.IsNullOrWhiteSpace(colAttr.Name)) ? null : escapeMethod(prop.Name);

                var insertAttr = prop.GetCustomAttribute<InsertValueAttribute>();
                this.Insert = (insertAttr?.SetValue) ?? (prop.CanRead && isMapped); // 2022.12 セットする値が取得できないプロパティは自明で更新対象外カラムとする
                this.InsertSQL = (insertAttr == null || string.IsNullOrWhiteSpace(insertAttr.Sql)) ? null : insertAttr.Sql;

                var concurrencyAttr = prop.GetCustomAttribute<ConcurrencyCheckAttribute>();
                this.ConcurrencyCheck = (concurrencyAttr != null);

                var updateAttr = prop.GetCustomAttribute<UpdateValueAttribute>();
                this.Update = (updateAttr?.SetValue) ?? (prop.CanRead && isMapped && !this.IsKey); // 2022.12 セットする値が取得できないプロパティは自明で更新対象外カラムとする
                this.UpdateSQL = (updateAttr == null || string.IsNullOrWhiteSpace(updateAttr.Sql)) ? null : updateAttr.Sql;
            }
        }
        #endregion

        /// <summary>
        /// 指定された型のテーブル情報を生成します。
        /// </summary>
        /// <param name="tableType">テーブルにマッピングされた型</param>
        /// <param name="escapeMethod">SQL識別子（テーブル名/カラム名等）をエスケープするメソッド</param>
        /// <returns>テーブル情報</returns>
        public static TableInfo Create(Type tableType, Func<string, string> escapeMethod)
        {
            var tableAttr = tableType.GetCustomAttribute<TableAttribute>();
            var props = tableType.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            var columns = props.Select(prop => new Column(prop, escapeMethod)).ToArray();

            var tableInfo = new TableInfo
            {
                // テーブル名（スキーマ名指定も考慮）
                Name = (tableAttr == null || string.IsNullOrWhiteSpace(tableAttr.Schema) ? "" : tableAttr.Schema + ".")
                    + (tableAttr == null || string.IsNullOrWhiteSpace(tableAttr.Name) ? escapeMethod(tableType.Name) : tableAttr.Name),
                // 各列
                Columns = columns,
                // 自動連番カラム
                RetrieveInsertedIdColumn = columns.FirstOrDefault(r => r.PropertyInfo.GetCustomAttribute<InsertValueAttribute>()?.RetrieveInsertedId == true),
                // その他付加情報
                SelectSqlInfo = tableType.GetCustomAttribute<SelectSqlAttribute>(),
            };
            return tableInfo;
        }

        /// <summary>
        /// 引数で指定されている名前のカラム情報を返します。
        /// </summary>
        /// <param name="names">カラム名のコレクション</param>
        /// <returns>カラム情報のコレクション</returns>
        public IEnumerable<Column> GetColumns(IEnumerable<string> names)
        {
            foreach (var name in names)
            {
                yield return GetColumn(name);
            }
        }

        /// <summary>
        /// 引数で指定されている名前のカラム情報を返します。
        /// </summary>
        /// <param name="name">カラム名</param>
        /// <returns>カラム情報</returns>
        public Column GetColumn(string name)
        {
            var column = Columns.Where(c => c.PropertyInfo.Name == name).FirstOrDefault();
            if (column == null)
            {
                throw new ArgumentException("column not found", name);
            }
            return column;
        }
    }
}
