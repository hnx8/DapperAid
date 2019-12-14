using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Dapper;
using DapperAid.Helpers;

namespace DapperAid
{
    /// <summary>
    /// Dapperで実行するSQL/パラメータを組み立てるクラスです。
    /// </summary>
    public partial class QueryBuilder
    {
        /// <summary>
        /// DapperAidの拡張メソッドで使用する既定のQueryBuilderオブジェクトです。
        /// </summary>
        /// <remarks>
        /// システム初期化時にDBMSに応じたサブクラスのオブジェクトを設定してください。
        /// </remarks>
        public static QueryBuilder DefaultInstance = new QueryBuilder();


        /// <summary>
        /// 一括Insert用SQLを生成する際の、SQLクエリ１回での挿入行数の上限です。既定値は1000です。
        /// </summary>
        /// <remarks>
        /// 一度に挿入する行数*列数が多すぎてパフォーマンスが悪化する場合など、値を調整してください。
        /// ごく一部のDBMSは一括Insert未対応のため1固定となります。
        /// </remarks>
        public virtual int MultiInsertRowsPerQuery
        {
            get { return _multiInsertRowsPerQuery; }
            set { _multiInsertRowsPerQuery = value; }
        }
        private int _multiInsertRowsPerQuery = 1000; // C#6.0以降でないと自動プロパティの初期値が使えないため旧来の書式とする

        #region テーブル/カラム構造 --------------------------------------------

        /// <summary>
        /// 指定された型のテーブル情報を取得します。
        /// </summary>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>テーブル情報</returns>
        public TableInfo GetTableInfo<T>()
        {
            return GetTableInfo(typeof(T));
        }

        /// <summary>
        /// 指定された型のテーブル情報を取得します。
        /// </summary>
        /// <param name="tableType">テーブルにマッピングされた型</param>
        /// <returns>テーブル情報</returns>
        public TableInfo GetTableInfo(Type tableType)
        {
            return _tables.GetOrAdd(tableType, t => TableInfo.Create(t, EscapeIdentifier));
        }
        private readonly ConcurrentDictionary<Type, TableInfo> _tables = new ConcurrentDictionary<Type, TableInfo>();

        #endregion

        #region DBMS固有設定 ---------------------------------------------------

        /// <summary>バインドパラメータの頭に付加する文字です。</summary>
        /// <remarks>DBMSにより使用する文字が異なります。</remarks>
        public virtual char ParameterMarker { get { return '@'; } }

        /// <summary>
        /// SQL識別子（テーブル名/カラム名等）をDBMSに応じたルールでエスケープします。
        /// </summary>
        /// <param name="identifier">識別子</param>
        /// <returns>エスケープ後の識別子</returns>
        /// <remarks>DBMSによりエスケープに使用する文字が異なります。</remarks>
        public virtual string EscapeIdentifier(string identifier)
        {
            // SQL標準/一般的なDBMS向け：ダブルクォーテーションでエスケープ
            return "\"" + identifier.Replace("\"", "\"\"") + "\"";
        }

        /// <summary>TRUEを表すSQLリテラル表記です。</summary>
        /// <remarks>ごく一部のDBMSは0/1で表記します。</remarks>
        public virtual string TrueLiteral { get { return "true"; } }

        /// <summary>Falseを表すSQLリテラル表記です。</summary>
        /// <remarks>ごく一部のDBMSは0/1で表記します。</remarks>
        public virtual string FalseLiteral { get { return "false"; } }

        /// <summary>
        /// 引数で指定された値をSQLリテラル値表記へと変換します。
        /// </summary>
        /// <param name="value">値</param>
        /// <returns>SQLリテラル値表記</returns>
        public string ToSqlLiteral(object value)
        {
            if (IsNull(value)) { return "null"; }
            if (value is string) { return ToSqlLiteral(value as string); }
            if (value is DateTime) { return ToSqlLiteral((DateTime)value); }
            if (value is bool) { return ((bool)value ? TrueLiteral : FalseLiteral); }
            if (value is Enum) { return ((Enum)value).ToString("d"); }
            if (value.GetType().GetInterfaces().Any(t => t.IsConstructedGenericType && t.GetGenericTypeDefinition() == typeof(ICollection<>)))
            {
                var sb = new StringBuilder();
                var delimiter = "(";
                foreach (var v in value as System.Collections.IEnumerable)
                {
                    sb.Append(delimiter).Append(ToSqlLiteral(v));
                    delimiter = ",";
                }
                sb.Append(")");
                return sb.ToString();
            }
            return value.ToString();
        }
        /// <summary>
        /// 引数で指定された文字列値をSQLリテラル値表記へと変換します。
        /// </summary>
        /// <param name="value">値</param>
        /// <returns>SQLリテラル値表記</returns>
        /// <remarks>DBMSによりリテラル値表記が異なります。</remarks>
        public virtual string ToSqlLiteral(string value)
        {
            return (IsNull(value) ? "null" : "'" + value.Replace("'", "''") + "'");
        }
        /// <summary>
        /// 引数で指定された日付値をSQLリテラル値表記へと変換します。
        /// </summary>
        /// <param name="value">値</param>
        /// <returns>SQLリテラル値表記</returns>
        /// <remarks>DBMSによりリテラル値表記が異なります。</remarks>
        public virtual string ToSqlLiteral(DateTime value)
        {
            // 以下の書式はPostgreSQL,Oracle,DB2向け。SQLite/SqlServerはdatetime、Accessは#日時#、MySqlは文字列表記
            return "timestamp '" + value.ToString("yyyy-MM-dd HH:mm:ss.ffffff") + "'";
        }

        /// <summary>
        /// 引数の値がnullに相当する値であればtrueを返します。
        /// </summary>
        /// <param name="value">値</param>
        /// <returns>nullに相当する値であればtrue</returns>
        /// <remarks>ごく一部のDBMSは長さゼロの文字列もnullとみなします。</remarks>
        public virtual bool IsNull(object value)
        {
            return (value == null || value is System.DBNull);
        }

        #endregion

        #region パラメータバインド ---------------------------------------------

        /// <summary>
        /// パラメータをバインドします。
        /// </summary>
        /// <param name="parameters">パラメーターオブジェクト</param>
        /// <param name="proposedName">パラメータ名の候補（すでに使用されていなければ採用）</param>
        /// <param name="value">パラメータ値</param>
        /// <returns>バインドされたマーカープリフィックス付きのパラメータ名、例："@Key1"。ただしパラメータオブジェクトがnullの場合はSQLリテラル値</returns>
        public string AddParameter(DynamicParameters parameters, string proposedName, object value)
        {
            if (parameters == null)
            {
                return ToSqlLiteral(value);
            }

            var alreadyUsedNames = parameters.ParameterNames.ToArray();
            var i = alreadyUsedNames.Length;
            var parameterName = proposedName;
            while (string.IsNullOrWhiteSpace(parameterName) || alreadyUsedNames.Contains(parameterName))
            {   // すでに同名のパラメータが登録されている場合は、適当な連番によるパラメータバインドを試みる
                parameterName = "P" + i.ToString("D2");
                i++;
            }
            parameters.Add(parameterName, value);
            return this.ParameterMarker + parameterName;
        }

        /// <summary>
        /// ラムダ式の初期化子での各指定値をパラメータにバインドします。
        /// </summary>
        /// <param name="values">バインド対象項目と値を初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Key1 = 1, Key2 = 99 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>パラメータオブジェクト</returns>
        public DynamicParameters BindValues<T>(Expression<Func<T>> values)
        {
            var initExpr = (values.Body as MemberInitExpression);
            if (initExpr == null || initExpr.Bindings.Count == 0)
            {
                throw new ArgumentException("no bind column");
            }

            var parameters = new DynamicParameters();
            foreach (var member in initExpr.Bindings)
            {
                var name = member.Member.Name;
                AddParameter(parameters, name, ExpressionHelper.EvaluateValue((member as MemberAssignment).Expression));
            }
            return parameters;
        }

        #endregion

        #region SQL文生成 ------------------------------------------------------

        /// <summary>
        /// 指定された型のテーブルに対するSELECT Count(*) SQLを生成します。
        /// </summary>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「select count(*) from [テーブル]」のSQL</returns>
        public string BuildCount<T>()
        {
            var tableInfo = GetTableInfo<T>();
            return "select count(*) from " + tableInfo.Name;
        }

        /// <summary>
        /// 指定された型のテーブルに対するSELECT SQLを生成します。
        /// targetColumns省略時は該当テーブルのマッピング対象列すべて、指定時は匿名型で定義された列のみを取得します。
        /// </summary>
        /// <param name="targetColumns">値取得対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「select [各カラム] from [テーブル]」のSQL</returns>
        public string BuildSelect<T>(Expression<Func<T, dynamic>> targetColumns = null)
        {
            var tableInfo = GetTableInfo<T>();
            var columns = (targetColumns == null)
                ? tableInfo.Columns
                : tableInfo.GetColumns(ExpressionHelper.GetMemberNames(targetColumns.Body));
            var sb = new StringBuilder();
            var delimiter = " ";
            foreach (var column in columns)
            {
                sb.Append(delimiter);
                delimiter = ", "; // ２カラム目以降はカンマで連結
                sb.Append(column.Name);
                if (column.Alias != null) { sb.Append(" as ").Append(column.Alias); }
            }

            var selectBeginning = (tableInfo.SelectSqlInfo == null || string.IsNullOrWhiteSpace(tableInfo.SelectSqlInfo.Beginning))
                ? "select"
                : tableInfo.SelectSqlInfo.Beginning;
            return selectBeginning + sb.ToString() + " from " + tableInfo.Name;
        }

        /// <summary>
        /// 指定された型のテーブルに対するSELECT SQLの末尾（group by, order by等)を生成します。
        /// </summary>
        /// <param name="targetColumns">（group byを生成する場合にのみ影響）値取得対象カラムを限定している場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <param name="otherClauses">SQL文の末尾に付加するorderBy条件/limit/offset/forUpdate指定などがあれば、その内容</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>Select SQLの末尾（テーブルの設定などに応じたgroup by/order by等の内容）</returns>
        public string BuildSelectOrderByEtc<T>(Expression<Func<T, dynamic>> targetColumns = null, string otherClauses = null)
        {
            var tableInfo = GetTableInfo<T>();
            var sb = new StringBuilder();
            if (tableInfo.SelectSqlInfo != null && tableInfo.SelectSqlInfo.GroupByKey)
            {
                var columns = (targetColumns == null)
                    ? tableInfo.Columns
                    : tableInfo.GetColumns(ExpressionHelper.GetMemberNames(targetColumns.Body));
                var delimiter = " group by ";
                foreach (var column in columns.Where(c => c.IsKey))
                {
                    sb.Append(delimiter);
                    delimiter = ", "; // ２カラム目以降はカンマで連結
                    sb.Append(column.Name);
                }
            }
            if (otherClauses != null)
            {
                sb.Append(" ").Append(otherClauses);
            }
            else if (tableInfo.SelectSqlInfo != null && !string.IsNullOrWhiteSpace(tableInfo.SelectSqlInfo.DefaultOtherClauses))
            {
                sb.Append(" ").Append(tableInfo.SelectSqlInfo.DefaultOtherClauses);
            }
            return sb.ToString();
        }


        /// <summary>
        /// 指定された型のテーブルに対するINSERT SQLを生成します。
        /// </summary>
        /// <param name="targetColumns">値設定対象カラムを限定する場合は、対象カラムを初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Key1 = 1, Value = 99 }</c>」「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「insert into [テーブル]([各カラム])values([各設定値])」のSQL</returns>
        public string BuildInsert<T>(LambdaExpression targetColumns = null)
        {
            var tableInfo = GetTableInfo<T>();
            var columns = (targetColumns == null)
                ? tableInfo.Columns.Where(c => c.Insert)
                : tableInfo.GetColumns(ExpressionHelper.GetMemberNames(targetColumns.Body));
            var names = new StringBuilder();
            var values = new StringBuilder();
            foreach (var column in columns)
            {
                if (names.Length > 0)
                {
                    names.Append(", ");
                    values.Append(", ");
                }
                names.Append(column.Name);
                values.Append(targetColumns != null || string.IsNullOrWhiteSpace(column.InsertSQL)
                    ? (ParameterMarker + column.PropertyInfo.Name)
                    : column.InsertSQL);
            }
            return "insert into " + tableInfo.Name + "(" + names.ToString() + ") values (" + values.ToString() + ")";
        }

        /// <summary>
        /// 指定された型のテーブルにレコードを挿入し、[InsertSQL(RetrieveInsertedId = true)]属性の自動連番カラムで採番されたIDを当該プロパティへセットするSQLを生成します。
        /// </summary>
        /// <param name="targetColumns">値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「insert into [テーブル]([各カラム])values([各設定値]) returning [自動採番カラム]」といったSQL</returns>
        /// <remarks>
        /// DBMSに応じたSQLが生成されます。
        /// 一部のDBMS(Oracleなど)では採番されたIDをoutパラメータに格納するようなSQLが生成されます。
        /// </remarks>
        public string BuildInsertAndRetrieveId<T>(Expression<Func<T, dynamic>> targetColumns)
        {
            var tableInfo = GetTableInfo<T>();
            if (tableInfo.RetrieveInsertedIdColumn == null)
            {
                throw new ConstraintException("RetrieveInsertedId-Column not specified");
            }
            return BuildInsert<T>(targetColumns) + " " + GetInsertedIdReturningSql<T>(tableInfo.RetrieveInsertedIdColumn);
        }
        /// <summary>
        /// 採番された自動連番値を返すSQL句を返します。
        /// </summary>
        /// <param name="column">自動採番カラムの情報</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>insertSQLの末尾に付加すべき採番ID値取得句(またはセミコロン＋別のSQL)</returns>
        /// <remarks>
        /// サブクラスによりオーバーライドされ、DBMSに応じたSQLが生成されます。
        /// 一部のDBMS(Oracleなど)では採番されたIDをoutパラメータに格納するようなSQLが生成されます。
        /// </remarks>
        protected virtual string GetInsertedIdReturningSql<T>(TableInfo.Column column)
        {
            // 具体的なDBMSがわからないと自動連番値の取り出し方法が定まらないため既定では例外とする。
            throw new NotSupportedException("use DBMS-specific QueryBuilder");
        }

        /// <summary>
        /// 一括Insert用SQLを生成します。
        /// </summary>
        /// <param name="records">挿入するレコード（複数件）</param>
        /// <param name="targetColumns">値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「insert into [テーブル]([各カラム]) values ([各設定値]),([各設定値])...」の静的SQL。一度に挿入する行数がMultiInsertRowsPerQueryを超過しないよう分割して返されます</returns>
        /// <remarks>一部のDBMSでは生成されるinsert文の内容が異なります。</remarks>
        public virtual IEnumerable<string> BuildMultiInsert<T>(IEnumerable<T> records, Expression<Func<T, dynamic>> targetColumns = null)
        {
            var tableInfo = GetTableInfo<T>();
            var columns = (targetColumns == null
                ? tableInfo.Columns.Where(c => c.Insert)
                : tableInfo.GetColumns(ExpressionHelper.GetMemberNames(targetColumns.Body))).ToArray();
            var names = new StringBuilder();
            foreach (var column in columns)
            {
                if (names.Length > 0) { names.Append(", "); }
                names.Append(column.Name);
            }

            var data = new StringBuilder();
            var rowCount = 0;
            foreach (var record in records)
            {
                var values = new StringBuilder();
                foreach (var column in columns)
                {
                    values.Append(values.Length == 0 ? "(" : ", ");
                    values.Append(targetColumns != null || string.IsNullOrWhiteSpace(column.InsertSQL)
                        ? ToSqlLiteral(MemberAccessor.GetValue(record, column.PropertyInfo))
                        : column.InsertSQL);
                }
                values.Append(")");

                data.AppendLine(data.Length == 0
                    ? "insert into " + tableInfo.Name + "(" + names.ToString() + ") values"
                    : ",");
                data.Append(values.ToString());
                if (rowCount >= MultiInsertRowsPerQuery)
                {
                    yield return data.ToString();
                    data.Clear();
                    rowCount = 0;
                }
            }
            if (data.Length > 0)
            {
                yield return data.ToString();
            }
        }

        /// <summary>
        /// 指定された型のテーブルに対するUPDATE SQLを生成します。
        /// targetColumns省略時は該当テーブルのUpdate対象列すべて(ただしKey項目は除く)、指定時は対象列のみを値更新対象とします。
        /// </summary>
        /// <param name="targetColumns">値更新対象カラムを限定する場合は、対象カラムを初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Col1 = 1, Col2 = 0 }</c>」「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「update [テーブル] set [カラム=設定値,...]」のSQL</returns>
        public string BuildUpdate<T>(LambdaExpression targetColumns = null)
        {
            var tableInfo = GetTableInfo<T>();
            var columns = (targetColumns == null)
                ? tableInfo.Columns.Where(c => (c.Update))
                : tableInfo.GetColumns(ExpressionHelper.GetMemberNames(targetColumns.Body));
            var sb = new StringBuilder();
            foreach (var column in columns)
            {
                if (sb.Length > 0) { sb.Append(", "); }
                sb.Append(column.Name)
                    .Append("=")
                    .Append(targetColumns != null || string.IsNullOrWhiteSpace(column.UpdateSQL)
                        ? (ParameterMarker + column.PropertyInfo.Name)
                        : column.UpdateSQL);
            }
            return "update " + tableInfo.Name + " set " + sb.ToString();
        }

        /// <summary>
        /// 指定された型のテーブルに対するDELETE SQLを生成します。
        /// </summary>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「delete from [テーブル]」のSQL文</returns>
        public string BuildDelete<T>()
        {
            var tableInfo = GetTableInfo<T>();
            return "delete from " + tableInfo.Name;
        }

        /// <summary>
        /// 指定された型のテーブルに対するTRUNCATE SQLを生成します。
        /// </summary>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「truncate table [テーブル]」のSQL</returns>
        public virtual string BuildTruncate<T>()
        {
            var tableInfo = GetTableInfo<T>();
            return "truncate table " + tableInfo.Name;
        }


        /// <summary>
        /// 指定された条件のカラムによるWhere句を生成します。
        /// </summary>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <param name="data">Where条件対象カラムに値が設定されているオブジェクト</param>
        /// <param name="columnWhere">Where条件対象となるカラムであればtrueを返すラムダ式。例：PK項目を条件とする場合「c => (c.IsKey)」</param>
        /// <returns>SQL文のWhere句</returns>
        public string BuildWhere<T>(T data, Func<TableInfo.Column, bool> columnWhere)
        {
            var tableInfo = GetTableInfo<T>();
            var columns = tableInfo.Columns.Where(columnWhere);

            var sb = new StringBuilder();
            foreach (var column in columns)
            {
                sb.Append(sb.Length == 0 ? " where " : " and ");
                var value = (data == null ? null : MemberAccessor.GetValue(data, column.PropertyInfo));
                if (data != null && IsNull(value))
                {
                    sb.Append(column.Name).Append(" is null");
                }
                else
                {
                    sb.Append(column.Name)
                        .Append("=")
                        .Append(ParameterMarker + column.PropertyInfo.Name);
                }
            }
            if (sb.Length == 0)
            {
                throw new ArgumentException("no condition column");
            }
            return sb.ToString();
        }

        /// <summary>
        /// 指定された項目を条件としたWhere条件SQLを生成します。パラメータバインドも行います。
        /// </summary>
        /// <param name="parameters">Dapperパラメーターオブジェクト</param>
        /// <param name="keyValues">レコード特定Key値を初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Key1 = 1, Key2 = 99 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>SQL文のWhere句</returns>
        /// <remarks>parametersにnullが指定されている場合は、パラメータバインドは行わず静的なSQLを生成します。</remarks>
        public string BuildWhere<T>(DynamicParameters parameters, Expression<Func<T>> keyValues)
        {
            var tableInfo = GetTableInfo<T>();
            var sb = new StringBuilder();
            Action<TableInfo.Column, object> bindWhere = (column, value) =>
            {
                sb.Append(sb.Length == 0 ? " where " : " and ");
                if (IsNull(value))
                {
                    sb.Append(column.Name).Append(" is null");
                }
                else
                {
                    sb.Append(column.Name)
                        .Append("=")
                        .Append(AddParameter(parameters, column.PropertyInfo.Name, value));
                }
            };

            var memberExpr = (keyValues.Body as MemberExpression);
            var initExpr = (keyValues.Body as MemberInitExpression);
            if (memberExpr != null)
            {   // 特例対応：ラムダの戻り値として指定されたオブジェクトをもとに楽観排他更新のWhere条件式を生成
                var data = (T)ExpressionHelper.EvaluateValue(memberExpr);
                foreach (var column in tableInfo.Columns.Where(c => c.IsKey || c.ConcurrencyCheck))
                {
                    var value = MemberAccessor.GetValue(data, column.PropertyInfo);
                    bindWhere(column, value);
                }
            }
            else if (initExpr != null)
            {   // 初期化子をもとにWhere条件式を生成
                foreach (var member in initExpr.Bindings)
                {
                    var column = tableInfo.GetColumn(member.Member.Name);
                    var value = ExpressionHelper.EvaluateValue((member as MemberAssignment).Expression);
                    bindWhere(column, value);
                }
            }

            if (sb.Length == 0)
            {
                throw new ArgumentException("no condition column");
            }
            return sb.ToString();
        }

        /// <summary>
        /// 引数で指定された条件式ラムダに基づきWhere条件SQLを生成します。
        /// where条件値のパラメータバインドも行います。
        /// </summary>
        /// <param name="parameters">Dapperパラメーターオブジェクト</param>
        /// <param name="where">式木（ラムダ）により記述された条件式</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>生成されたWhere句（条件指定なしの場合は空文字）</returns>
        /// <remarks>parametersにnullが指定されている場合は、パラメータバインドは行わず静的なSQLを生成します。</remarks>
        public string BuildWhere<T>(DynamicParameters parameters, Expression<Func<T, bool>> where)
        {
            if (where == null)
            {
                return string.Empty;
            }
            var tableInfo = GetTableInfo<T>();
            return " where " + BuildWhere(parameters, tableInfo, where.Body);
        }

        /// <summary>
        /// 引数で指定された条件式ラムダに基づきWhere条件を生成します。
        /// </summary>
        /// <param name="parameters">Dapperパラメーターオブジェクト</param>
        /// <param name="tableInfo">テーブル情報</param>
        /// <param name="expression">式木（ラムダ）により記述された条件式</param>
        /// <returns>条件式のSQL</returns>
        private string BuildWhere(DynamicParameters parameters, TableInfo tableInfo, Expression expression)
        {
            // ローカル関数相当：ExpressionがWhere条件となるカラムなら、そのカラム情報を返す
            Func<Expression, TableInfo.Column> getIfOperandIsConditionColumn = (exp) =>
            {
                var me = ExpressionHelper.CastTo<MemberExpression>(exp);
                if (me == null || me.Expression == null || me.Expression.NodeType != ExpressionType.Parameter)
                {   // ラムダ式の引数についてのメンバーでなければ、Where条件となるカラムとはみなさない
                    return null;
                }
                var prop = me.Member as PropertyInfo;
                if (prop == null)
                {   // プロパティ項目でなければ、Where条件となるカラムとはみなさない
                    return null;
                }
                // tableInfoのカラムに対応するなら、そのカラム情報を返す
                return tableInfo.Columns.Where(c => c.PropertyInfo == prop).FirstOrDefault();
            };

            var binaryExpr = ExpressionHelper.CastTo<BinaryExpression>(expression);
            if (binaryExpr == null)
            {   // 二項演算子以外の特殊な表現が指定されている場合の処理

                // 「!」NOT指定かどうか/bool型のプロパティが指定されているかを判定
                var unaryExpr = ExpressionHelper.CastTo<UnaryExpression>(expression);
                var isNot = (unaryExpr != null && unaryExpr.NodeType == ExpressionType.Not);
                var column = getIfOperandIsConditionColumn(isNot ? unaryExpr.Operand : expression);
                if (column != null && column.PropertyInfo.PropertyType == typeof(bool))
                {   // boolカラム値の条件として組み立てる
                    return column.Name + "=" + (isNot ? FalseLiteral : TrueLiteral);
                }
                if (isNot)
                {   // NOT条件として組み立てる
                    return "not(" + BuildWhere(parameters, tableInfo, unaryExpr.Operand) + ")";
                }

                // 特定のメソッド呼び出しが指定されているかを判定
                var methodCallExpr = ExpressionHelper.CastTo<MethodCallExpression>(expression);
                if (methodCallExpr != null && methodCallExpr.Method != null && methodCallExpr.Method.DeclaringType == typeof(ToSql))
                {
                    // ※C#6.0以降でないとnameofが利用できないため定数定義
                    const string NameofEval = "Eval"; //nameof(ToSql.Eval);

                    if (methodCallExpr.Method.Name == NameofEval)
                    {   // ToSql.Eval(string)：指定されたSQL文字列を直接埋め込む
                        var argumentExpression = methodCallExpr.Arguments[0];
                        if (argumentExpression.Type == typeof(string))
                        {
                            return "(" + ExpressionHelper.EvaluateValue(argumentExpression) + ")";
                        }
                    }
                }

                // いずれでもなければ例外
                throw new InvalidExpressionException(expression.ToString());
            }

            // And/Or条件の解釈
            switch (binaryExpr.NodeType)
            {
                case ExpressionType.AndAlso:
                    var andLeft = BuildWhere(parameters, tableInfo, binaryExpr.Left);
                    if (andLeft == FalseLiteral)
                    {   // 「left && right」のうちleftでfalseが確定している場合は、右辺の展開を行わない
                        return andLeft;
                    }
                    var andRight = BuildWhere(parameters, tableInfo, binaryExpr.Right);
                    if (andLeft == TrueLiteral)
                    {   // 「left && right」のうちleftがtrueなら、右辺のみを返す
                        return andRight;
                    }
                    return andLeft + " and " + andRight;
                case ExpressionType.OrElse:
                    var orLeft = BuildWhere(parameters, tableInfo, binaryExpr.Left);
                    if (orLeft == TrueLiteral)
                    {   // 「left || right」のうちleftでtrueが確定している場合は、右辺の展開を行わない
                        return orLeft;
                    }
                    var orRight = BuildWhere(parameters, tableInfo, binaryExpr.Right);
                    if (orLeft == FalseLiteral)
                    {   // 「left || right」のうちleftがfalseなら、右辺のみを返す
                        return orRight;
                    }
                    return "(" + orLeft + ") or (" + orRight + ")";
            }

            var leftMember = getIfOperandIsConditionColumn(binaryExpr.Left);
            var rightMember = getIfOperandIsConditionColumn(binaryExpr.Right);
            if (leftMember == null && rightMember == null)
            {   // 両辺ともカラム指定ではない：具体的なbool値に展開
                var boolValue = ExpressionHelper.EvaluateValue(binaryExpr);
                if (!(boolValue is bool))
                {
                    throw new InvalidExpressionException(binaryExpr.ToString());
                }
                return ((bool)boolValue ? TrueLiteral : FalseLiteral);
            }

            // 比較演算子を解釈
            string op;
            var opEnd = string.Empty;
            var opIsNot = false;
            switch (binaryExpr.NodeType)
            {
                case ExpressionType.Equal:
                    op = "="; break;
                case ExpressionType.NotEqual:
                    op = "<>"; opIsNot = true; break;
                case ExpressionType.GreaterThan:
                    op = ">"; break;
                case ExpressionType.GreaterThanOrEqual:
                    op = ">="; break;
                case ExpressionType.LessThan:
                    op = "<"; break;
                case ExpressionType.LessThanOrEqual:
                    op = "<="; break;
                default:
                    throw new InvalidExpressionException(expression.ToString());
            }

            if (leftMember != null && rightMember != null)
            {   // 両辺ともカラム名指定の場合はパラメータ値取得不要、両辺のカラム名に基づき条件式を組み立て
                return leftMember.Name + op + rightMember.Name;
            }

            var condColumn = leftMember ?? rightMember;
            var valueExpression = ExpressionHelper.CastTo<Expression>(condColumn == leftMember ? binaryExpr.Right : binaryExpr.Left);
            if (valueExpression is MethodCallExpression)
            {   // 値指定時に特定のToSqlメソッドを通している場合は、メソッドに応じたSQLを組み立てる
                var method = (valueExpression as MethodCallExpression).Method;
                if (method.DeclaringType == typeof(ToSql))
                {
                    // ※C#6.0以降でないとnameofが利用できないため定数定義とする
                    const string NameofLike = "Like"; //nameof(ToSql.Like);
                    const string NameofIn = "In"; //nameof(ToSql.In);
                    const string NameofBetween = "Between"; //nameof(ToSql.Between);

                    if (method.Name == NameofLike)
                    {   // ToSql.Like(string)：比較演算子をLike演算子とする
                        op = (opIsNot ? " not" : "") + " like ";
                        valueExpression = (valueExpression as MethodCallExpression).Arguments[0];
                    }
                    else if (method.Name == NameofIn)
                    {
                        valueExpression = (valueExpression as MethodCallExpression).Arguments[0];
                        if (valueExpression.Type == typeof(string))
                        {   // ToSql.In(string)： INサブクエリとして指定された文字列を直接埋め込む
                            return condColumn.Name + (opIsNot ? " not" : "") + " in(" + ExpressionHelper.EvaluateValue(valueExpression) + ")";
                        }
                        else
                        {   // ToSql.In(コレクション)： In演算子を組み立てる
                            return BuildWhereIn(parameters, condColumn, opIsNot, ExpressionHelper.EvaluateValue(valueExpression));
                        }
                    }
                    else if (method.Name == NameofBetween)
                    {    // ToSql.Between(値1, 値2)： Between演算子を組み立て、パラメータを２つバインドする。nullの可能性は考慮しない
                        var value1 = ExpressionHelper.EvaluateValue((valueExpression as MethodCallExpression).Arguments[0]);
                        var value2 = ExpressionHelper.EvaluateValue((valueExpression as MethodCallExpression).Arguments[1]);
                        return condColumn.Name + (opIsNot ? " not" : "") + " between "
                            + AddParameter(parameters, condColumn.PropertyInfo.Name, value1)
                            + " and "
                            + AddParameter(parameters, condColumn.PropertyInfo.Name, value2);
                    }
                    else
                    {
                        throw new InvalidExpressionException(expression.ToString());
                    }
                }
            }
            var value = ExpressionHelper.EvaluateValue(valueExpression);

            if (value == null)
            {
                return condColumn.Name + " is" + (opIsNot ? " not" : "") + " null";
            }
            else
            {
                var bindedValueSql = AddParameter(parameters, condColumn.PropertyInfo.Name, value);
                return (condColumn == leftMember
                    ? condColumn.Name + op + bindedValueSql
                    : bindedValueSql + op + condColumn.Name);
            }
        }

        /// <summary>
        /// Where条件のIn条件式「[カラム] in [バインド値]」を組み立てます。
        /// </summary>
        /// <param name="parameters">パラメーターオブジェクト</param>
        /// <param name="column">対象カラム</param>
        /// <param name="opIsNot">not条件として組み立てるべきならtrue</param>
        /// <param name="values">バインドする値</param>
        /// <returns>In条件式</returns>
        /// <remarks>
        /// サブクラスによりオーバーライドされることがあります（Oracle/Postgresでは生成内容が変わります）
        /// </remarks>
        protected virtual string BuildWhereIn(DynamicParameters parameters, TableInfo.Column column, bool opIsNot, object values)
        {
            return column.Name + (opIsNot ? " not" : "") + " in " + AddParameter(parameters, column.PropertyInfo.Name, values);
            // →DapperのList Support機能により、カッコつきin句「in (@xx1, @xx2, ...)」へ展開されたうえで実行される
        }

        #endregion
    }
}
