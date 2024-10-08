﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
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
    /// DBMSに応じたサブクラスによりオーバーライドされます。
    /// </summary>
    public abstract partial class QueryBuilder
    {
        /// <summary>
        /// DapperAidの拡張メソッドで使用する既定のQueryBuilderオブジェクトです。
        /// </summary>
        public static QueryBuilder DefaultInstance { get; set; } = null!;

        /// <summary>
        /// (protected) インスタンス生成時、既定のQueryBuilderオブジェクトが未設定ならこのインスタンスを既定オブジェクトとみなして設定します
        /// </summary>
        protected QueryBuilder()
        {
            DefaultInstance ??= this;
        }

        /// <summary>
        /// このQueryBuilderオブジェクトを、指定されたDB接続文字列のDbConnectionオブジェクトで使用するよう紐づけます。
        /// </summary>
        /// <param name="connectionString">DB接続文字列</param>
        public void MapDbConnectionString(string connectionString)
        {
            IDbConnectionExtensions.MapDapperAid(this, connectionString);
        }


        /// <summary>
        /// 一括Insert/Upsert用SQLを生成する際の、SQLクエリ１回での挿入行数の上限です。既定値は1000です。
        /// </summary>
        /// <remarks>
        /// 一度に挿入する行数*列数が多すぎてパフォーマンスが悪化する場合など、値を調整してください。
        /// ごく一部のDBMS(MSAccessなど)は一括Insert未対応のため1固定となります。
        /// </remarks>
        public virtual int MultiInsertRowsPerQuery { get; set; } = 1000;

        /// <summary>
        /// DBMSがUpsert(MERGE)に対応していればtrue/未対応ならfalseを設定します。既定値はtrueです。
        /// </summary>
        /// <remarks>
        /// SQLServer2008未満、PostgresSQL9.5未満・SQLite3.24(2018-06-04)未満など未対応の環境ではfalseを設定してください。
        /// ごく一部のDBMS(MSAccessなど)はUpsert未対応のためfalse固定となります。
        /// </remarks>
        public virtual bool SupportsUpsert { get; set; } = true;


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
            if (tableType.IsSubclassOf(typeof(Delegate)))
            {   // テーブル型を指定すべき箇所で誤ってExpressinを指定している場合には例外とする。
                throw new InvalidOperationException("In this case, Specify the table type instead of Lambda.");
            }
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
        /// 引数で指定された型の値について、SQLリテラル値表記への変換処理を設定します。
        /// </summary>
        /// <typeparam name="T">データ型</typeparam>
        /// <param name="convertMethod">SQLリテラル値への変換メソッド</param>
        /// <remarks>既存のQueryBuilderではリテラル値表記へ変換できないDBデータ型（Geometry型など）の変換処理などを追加するためのメソッドです。</remarks>
        public virtual void AddSqlLiteralConverter<T>(Func<T, string> convertMethod)
        {
            _literalConverters[typeof(T)] = (value) => convertMethod((T)value);
        }
        /// <summary>型の値をSQLリテラル値表記へと変換する処理の対応付けDictionary</summary>
        protected Dictionary<Type, Func<object, string>> _literalConverters = new();

        /// <summary>
        /// 引数で指定された値をSQLリテラル値表記へと変換します。
        /// </summary>
        /// <param name="value">値</param>
        /// <returns>SQLリテラル値表記</returns>
        public string ToSqlLiteral(object? value)
        {
            if (IsNull(value)) { return "null"; }
            foreach (var c in _literalConverters)
            {
                if (c.Key.IsAssignableFrom(value.GetType())) { return c.Value(value); }
            }
            if (value is string s) { return ToSqlLiteral(s); }
            if (value is DateTime dt) { return ToSqlLiteral(dt); }
#if NET6_0_OR_GREATER
            if (value is DateOnly date) { return ToSqlLiteral(date); }
            if (value is TimeOnly time) { return ToSqlLiteral(time); }
#endif
            if (value is bool b) { return (b ? TrueLiteral : FalseLiteral); }
            if (value is Enum e) { return e.ToString("d"); }
            if (value is char ch) { return ToSqlLiteral($"{ch}"); }
            if (value is IConvertible) { return value.ToString()!; }
            if (value is byte[] blob) { return ToSqlLiteral(blob); }
            if (value is System.Collections.IEnumerable ienumerable && value.GetType().GetInterfaces().Any(t => t.IsConstructedGenericType && t.GetGenericTypeDefinition() == typeof(ICollection<>)))
            {
                var sb = new StringBuilder();
                sb.Append("ARRAY[");
                var delimiter = "";
                foreach (var v in ienumerable)
                {
                    sb.Append(delimiter).Append(ToSqlLiteral(v));
                    delimiter = ",";
                }
                sb.Append("]");
                return sb.ToString();
            }
            throw new ArgumentException($"The value of type {value.GetType().FullName} cannot be represented as a sql literal.");
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
        /// 引数で指定された日時値をSQLリテラル値表記へと変換します。
        /// </summary>
        /// <param name="value">値</param>
        /// <returns>SQLリテラル値表記</returns>
        /// <remarks>DBMSによりリテラル値表記が異なります。</remarks>
        public virtual string ToSqlLiteral(DateTime value)
        {
            // 以下の書式はPostgreSQL,Oracle,DB2向け。SQLite/SqlServerはdatetime、Accessは#日時#、MySqlは文字列表記
            return "timestamp '" + value.ToString("yyyy-MM-dd HH:mm:ss.ffffff") + "'";
        }
#if NET6_0_OR_GREATER
        /// <summary>
        /// 引数で指定された日付値をSQLリテラル値表記へと変換します。
        /// </summary>
        /// <param name="value">値</param>
        /// <returns>SQLリテラル値表記</returns>
        /// <remarks>DBMSによりリテラル値表記が異なります。</remarks>
        public virtual string ToSqlLiteral(DateOnly value)
        {
            // 以下の書式はPostgreSQL,Oracle,DB2向け。SQLite/SqlServerはdate、Accessは#日付#、MySqlは文字列表記
            return $"date '{value:yyyy-MM-dd}'";
        }
        /// <summary>
        /// 引数で指定された時刻値をSQLリテラル値表記へと変換します。
        /// </summary>
        /// <param name="value">値</param>
        /// <returns>SQLリテラル値表記</returns>
        /// <remarks>DBMSによりリテラル値表記が異なります。</remarks>
        public virtual string ToSqlLiteral(TimeOnly value)
        {
            // 以下の書式はPostgreSQL,Oracle,DB2向け。SQLite/SqlServerはtime、Accessは#時刻#、MySqlは文字列表記
            return $"time '{value:HH:mm:ss.ffffff}'";
        }
#endif
        /// <summary>
        /// 引数で指定されたblob値をSQLリテラル値表記へと変換します。
        /// </summary>
        /// <param name="value">値</param>
        /// <returns>SQLリテラル値表記</returns>
        /// <remarks>DBMSによりリテラル値表記が異なります。</remarks>
        public virtual string ToSqlLiteral(byte[] value)
        {
            return "X'" + string.Concat(value.Select(b => $"{b:X2}")) + "'";
        }

        /// <summary>
        /// 引数の値がnullに相当する値であればtrueを返します。
        /// </summary>
        /// <param name="value">値</param>
        /// <returns>nullに相当する値であればtrue</returns>
        /// <remarks>ごく一部のDBMSは長さゼロの文字列もnullとみなします。</remarks>
        public virtual bool IsNull([NotNullWhen(false)] object? value)
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
        public string AddParameter(DynamicParameters? parameters, string? proposedName, object? value)
        {
            if (parameters == null)
            {
                return ToSqlLiteral(value);
            }
            if (IsNull(value))
            {
                return "null";
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
        /// バインド値がDapperAid固有のSQL条件式記述用メソッドで値指定している場合は、
        /// SQL条件式生成メソッドを呼び出してパラメータバインド・SQL組み立てを行います。
        /// </summary>
        /// <param name="parameters">パラメーターオブジェクト</param>
        /// <param name="column">バインド対象のカラム</param>
        /// <param name="expression">バインド値をあらわす式木</param>
        /// <param name="opIsNot">条件式をnotで生成する場合はtrue</param>
        /// <returns>生成されたSQL表現文字列。ただしバインド値がDapperAid固有のSQL条件式記述用メソッドではなかった場合null</returns>
        protected string? TryBindSqlExpr(DynamicParameters parameters, TableInfo.Column? column, Expression expression, bool opIsNot)
        {
            var methodCallExpression = ExpressionHelper.CastTo<MethodCallExpression>(expression);
            if (methodCallExpression != null && methodCallExpression.Method != null && typeof(ISqlExpr).IsAssignableFrom(methodCallExpression.Method.DeclaringType))
            {
                return InstanceCreator.Create<ISqlExpr>(methodCallExpression.Method.DeclaringType).BuildSql(
                    methodCallExpression.Method.Name,
                    methodCallExpression.Arguments,
                    this,
                    parameters,
                    column,
                    opIsNot);
            }
            return null;
        }

        #endregion

        #region SQL文生成 ------------------------------------------------------

        /// <summary>
        /// 指定された型のテーブルに対するSELECT Count(*) SQLを生成します。
        /// </summary>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「select count(*) from [テーブル]」のSQL</returns>
        public string BuildCount<T>()
            where T : notnull
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
        public string BuildSelect<T>(Expression<Func<T, dynamic>>? targetColumns = null)
            where T : notnull
        {
            var tableInfo = GetTableInfo<T>();
            var columns = (targetColumns == null)
                ? tableInfo.Columns.Where(c => c.Select)
                : tableInfo.GetColumns(ExpressionHelper.GetMemberNames(targetColumns.Body));
            return BuildSelect(tableInfo, columns);
        }

        /// <summary>
        /// TFromで指定された型のテーブルからTColumn型の各列を取得するSELECT SQLを生成します。
        /// </summary>
        /// <typeparam name="TFrom">取得対象テーブルにマッピングされた型</typeparam>
        /// <typeparam name="TColumns">取得対象列にマッピングされた型</typeparam>
        /// <returns>「select [各カラム] from [テーブル]」のSQL</returns>
        public string BuildSelect<TFrom, TColumns>()
            where TFrom : notnull
            where TColumns : notnull
        {
            var tableInfo = GetTableInfo<TFrom>();
            var columns = GetTableInfo<TColumns>().Columns.Where(c => c.Select).Select(c =>
                // 当該取得列に取得SQL式が設定されているならそれを優先。でなければ取得対象テーブルにおける本来の取得列情報を使用
                (c.Alias != null)
                ? c
                : tableInfo.Columns.FirstOrDefault(fc => fc.PropertyInfo.Name == c.PropertyInfo.Name) ?? c
            ).ToArray();

            return BuildSelect(tableInfo, columns);
        }

        /// <summary>
        /// 引数で指定されたテーブルから引数で指定された各列を取得するSELECT SQLを生成します。
        /// </summary>
        /// <param name="tableInfo">テーブル情報</param>
        /// <param name="columns">取得対象列の列挙</param>
        /// <returns>「select [各カラム] from [テーブル]」のSQL</returns>
        private string BuildSelect(TableInfo tableInfo, IEnumerable<TableInfo.Column> columns)
        {
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
        public string BuildSelectOrderByEtc<T>(Expression<Func<T, dynamic>>? targetColumns = null, string? otherClauses = null)
            where T : notnull
        {
            var tableInfo = GetTableInfo<T>();
            var sb = new StringBuilder();
            if (tableInfo.SelectSqlInfo != null && tableInfo.SelectSqlInfo.GroupByKey)
            {
                var columns = (targetColumns == null)
                    ? tableInfo.Columns
                    : tableInfo.GetColumns(ExpressionHelper.GetMemberNames(targetColumns.Body));
                sb.Append(BuildSelectGroupBy(columns));
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
        /// 指定された型のテーブルに対するSELECT SQLの末尾（group by, order by等)を生成します。
        /// </summary>
        /// <param name="otherClauses">SQL文の末尾に付加するorderBy条件/limit/offset/forUpdate指定などがあれば、その内容</param>
        /// <typeparam name="TFrom">取得対象テーブルにマッピングされた型</typeparam>
        /// <typeparam name="TColumns">取得対象列にマッピングされた型</typeparam>
        /// <returns>Select SQLの末尾（テーブルの設定などに応じたgroup by/order by等の内容）</returns>
        public string BuildSelectOrderByEtc<TFrom, TColumns>(string? otherClauses = null)
            where TFrom : notnull
            where TColumns : notnull
        {
            // 以下、取得対象列にてgroupby/DefaultOtherClausesの定義があればそれを優先、なければ取得対象テーブルにおける指定内容を採用
            var columnsInfo = GetTableInfo<TColumns>();
            var fromInfo = GetTableInfo<TFrom>();

            var sb = new StringBuilder();
            if (columnsInfo.SelectSqlInfo != null && columnsInfo.SelectSqlInfo.GroupByKey)
            {
                sb.Append(BuildSelectGroupBy(columnsInfo.Columns));
            }
            else if (fromInfo.SelectSqlInfo != null && fromInfo.SelectSqlInfo.GroupByKey)
            {
                sb.Append(BuildSelectGroupBy(fromInfo.Columns));
            }

            if (otherClauses != null)
            {
                sb.Append(" ").Append(otherClauses);
            }
            else if (columnsInfo.SelectSqlInfo != null && columnsInfo.SelectSqlInfo.DefaultOtherClauses != null)
            {
                sb.Append(" ").Append(columnsInfo.SelectSqlInfo.DefaultOtherClauses);
            }
            else if (fromInfo.SelectSqlInfo != null && !string.IsNullOrWhiteSpace(fromInfo.SelectSqlInfo.DefaultOtherClauses))
            {
                sb.Append(" ").Append(fromInfo.SelectSqlInfo.DefaultOtherClauses);
            }
            return sb.ToString();
        }

        /// <summary>
        /// 指定されたカラムのうちKey属性が指定されているカラムでgroup by句を生成します。
        /// </summary>
        /// <param name="columns">取得対象カラム（Key属性が指定されているか否か不問）<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <returns>group by句</returns>
        private string BuildSelectGroupBy(IEnumerable<TableInfo.Column> columns)
        {
            var sb = new StringBuilder();
            var delimiter = " group by ";
            foreach (var column in columns.Where(c => c.IsKey))
            {
                sb.Append(delimiter);
                delimiter = ", "; // ２カラム目以降はカンマで連結
                sb.Append(column.Name);
            }
            return sb.ToString();
        }


        /// <summary>
        /// 指定された型のテーブルに対するINSERT SQLを生成します。
        /// </summary>
        /// <param name="targetColumns">値設定対象カラムを限定する場合は、対象カラムを初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Key1 = 1, Value = 99 }</c>」「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「insert into [テーブル]([各カラム])values([各設定値])」のSQL</returns>
        public string BuildInsert<T>(LambdaExpression? targetColumns = null)
            where T : notnull
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
        /// 指定された型のテーブルに対するINSERT SQLを生成します。パラメータバインドも行います。
        /// </summary>
        /// <param name="parameters">Dapperパラメーターオブジェクト</param>
        /// <param name="values">値設定対象カラム・設定値を初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Key1 = 1, Value = 99 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「insert into [テーブル]([各カラム])values([各設定値])」のSQL</returns>
        public string BuildInsert<T>(DynamicParameters parameters, Expression<Func<T>> values)
            where T : notnull
        {
            var initExpr = (values.Body as MemberInitExpression);
            if (initExpr == null || initExpr.Bindings.Count == 0)
            {
                throw new ArgumentException("target column unspecified");
            }

            var tableInfo = GetTableInfo<T>();
            var names = new StringBuilder();
            var valueSb = new StringBuilder();
            foreach (var member in initExpr.Bindings)
            {
                var column = tableInfo.GetColumn(member.Member.Name);
                var expression = (member as MemberAssignment)!.Expression;
                // DapperAid固有のSQL条件式記述用メソッドで値指定している場合は、SQL条件式生成メソッドを呼び出してSQLを組み立てる
                var valueSql = TryBindSqlExpr(parameters, null, expression, false);
                if (valueSql == null)
                {   // そうでなければ更新値を取り出してバインド
                    valueSql = AddParameter(parameters, member.Member.Name, ExpressionHelper.EvaluateValue(expression));
                }

                if (names.Length > 0)
                {
                    names.Append(", ");
                    valueSb.Append(", ");
                }
                names.Append(column.Name);
                valueSb.Append(valueSql);
            }
            return "insert into " + tableInfo.Name + "(" + names.ToString() + ") values (" + valueSb.ToString() + ")";
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
        public string BuildInsertAndRetrieveId<T>(Expression<Func<T, dynamic>>? targetColumns)
            where T : notnull
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
            where T : notnull
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
        public virtual IEnumerable<string> BuildMultiInsert<T>(IEnumerable<T> records, Expression<Func<T, dynamic>>? targetColumns = null)
            where T : notnull
        {
            var tableInfo = GetTableInfo<T>();
            var columns = (targetColumns == null
                ? tableInfo.Columns.Where(c => c.Insert)
                : tableInfo.GetColumns(ExpressionHelper.GetMemberNames(targetColumns.Body))).ToArray();
            var columnNames = string.Join(", ", columns.Select(c => c.Name));

            var sb = new StringBuilder();
            var rowCount = 0;
            foreach (var record in records)
            {
                var values = string.Join(", ", columns.Select(column =>
                    (targetColumns != null || string.IsNullOrWhiteSpace(column.InsertSQL))
                        ? ToSqlLiteral(MemberAccessor.GetValue(record, column.PropertyInfo))
                        : column.InsertSQL)
                    );

                sb.AppendLine(sb.Length == 0
                    ? "insert into " + tableInfo.Name + "(" + columnNames + ") values"
                    : ",");
                sb.Append("(" + values + ")");
                rowCount++;
                if (rowCount >= MultiInsertRowsPerQuery)
                {
                    yield return sb.ToString();
                    sb.Clear();
                    rowCount = 0;
                }
            }
            if (sb.Length > 0)
            {
                yield return sb.ToString();
            }
        }

        /// <summary>
        /// 指定された型のテーブルに対するUPSERT SQLを生成します。(既存レコードはUPDATE／未存在ならINSERTを行います)
        /// </summary>
        /// <param name="insertTargetColumns">insert実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2, t.Col3 }</c>」</param>
        /// <param name="updateTargetColumns">update実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col2, t.Col3 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「merge into TABLE using (各カラムの設定値) ... when matched then update ... when not matched then insert ...」などのUpsert用SQL</returns>
        /// <remarks>サブクラスによりオーバーライドされ、DBMSに応じたSQL文が生成されます。基底クラスではSQL2003に準じたmerge文を生成します。</remarks>
        public virtual string BuildUpsert<T>(Expression<Func<T, dynamic>>? insertTargetColumns = null, Expression<Func<T, dynamic>>? updateTargetColumns = null)
            where T : notnull
        {
            var tableInfo = GetTableInfo<T>();
            // values生成対象カラム（insert対象カラムのうちSQLリテラルによる初期化を行わないカラム）を把握
            var valuesColumns = (insertTargetColumns == null
                ? tableInfo.Columns.Where(c => c.Insert && string.IsNullOrEmpty(c.InsertSQL))
                : tableInfo.GetColumns(ExpressionHelper.GetMemberNames(insertTargetColumns.Body))).ToArray();
            // 条件式部分を把握
            var postfix = BuildUpsertCond(insertTargetColumns, updateTargetColumns);
            // DBMSに応じたusing句を取り出し、merge分として完成
            return "merge into " + tableInfo.Name + " as t "
                + BuildUpsertUsingClause<T>(valuesColumns)
                + postfix;
            //
        }
        /// <summary>
        /// Upsert(merge)のusing句を生成します。
        /// </summary>
        /// <param name="columns">recordsから挿入時の値を取り出すべきカラム</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「using(values (@パラメータ名, ...)) as s(カラム名, ...)」などといったusing句</returns>
        /// <remarks>サブクラスによりオーバーライドされ、DBMSに応じたusing句が返されます。（ソーステーブルは別名sとすること）</remarks>
        protected virtual string BuildUpsertUsingClause<T>(IReadOnlyList<TableInfo.Column> columns)
            where T : notnull
        {
            return "using(values"
                + Environment.NewLine
                + "(" + string.Join(",", columns.Select(column => (ParameterMarker + column.PropertyInfo.Name))) + ")"
                + Environment.NewLine
                + ") as s(" + string.Join(",", columns.Select(c => c.Name)) + ")";
        }

        /// <summary>
        /// 一括Upsert用SQLを生成します。(既存レコードはUPDATE／未存在ならINSERTを行います)
        /// </summary>
        /// <param name="records">挿入または更新するレコード（複数件）</param>
        /// <param name="insertTargetColumns">insert実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2, t.Col3 }</c>」</param>
        /// <param name="updateTargetColumns">update実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col2, t.Col3 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「merge into TABLE using ... when matched then update ... when not matched then insert ...」などのUpsert静的SQL。一度に挿入更新する行数がMultiInsertRowsPerQueryを超過しないよう分割して返されます</returns>
        /// <remarks>サブクラスによりオーバーライドされ、DBMSに応じたSQL文が生成されます。基底クラスではSQL2003に準じたmerge文を生成します。</remarks>
        public virtual IEnumerable<string> BuildMultiUpsert<T>(IEnumerable<T> records, Expression<Func<T, dynamic>>? insertTargetColumns = null, Expression<Func<T, dynamic>>? updateTargetColumns = null)
            where T : notnull
        {
            var tableInfo = GetTableInfo<T>();
            // values生成対象カラム（insert対象カラムのうちSQLリテラルによる初期化を行わないカラム）を把握
            var valuesColumns = (insertTargetColumns == null
                ? tableInfo.Columns.Where(c => c.Insert && string.IsNullOrEmpty(c.InsertSQL))
                : tableInfo.GetColumns(ExpressionHelper.GetMemberNames(insertTargetColumns.Body))).ToArray();
            // 条件式部分を把握
            var postfix = BuildUpsertCond(insertTargetColumns, updateTargetColumns);
            // DBMSに応じたusing句を（適切なレコード件数毎に分割して）取り出し、merge分として完成
            foreach (var usingClause in BuildMultiUpsertUsingClause(records, valuesColumns))
            {
                yield return "merge into " + tableInfo.Name + " as t "
                    + usingClause
                    + postfix;
            }
        }
        /// <summary>
        /// 一括Upsert(merge)のusing句を生成します。
        /// </summary>
        /// <param name="records">挿入または更新するレコード（複数件）</param>
        /// <param name="columns">recordsから挿入時の値を取り出すべきカラム</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「using(values (...), (...), ... ) as s(カラム名, ...)」などといった静的using句</returns>
        /// <remarks>サブクラスによりオーバーライドされ、DBMSに応じたusing句が返されます。（ソーステーブルは別名sとすること）</remarks>
        protected virtual IEnumerable<string> BuildMultiUpsertUsingClause<T>(IEnumerable<T> records, IReadOnlyList<TableInfo.Column> columns)
            where T : notnull
        {
            var postfix = Environment.NewLine + ") as s(" + string.Join(",", columns.Select(c => c.Name)) + ")";

            var sb = new StringBuilder();
            var rowCount = 0;

            foreach (var record in records)
            {
                sb.AppendLine(sb.Length == 0
                    ? "using(values"
                    : ",");
                var values = string.Join(",", columns.Select(
                    column => ToSqlLiteral(MemberAccessor.GetValue(record, column.PropertyInfo))
                    ));
                sb.Append("(" + values + ")");
                rowCount++;
                if (rowCount >= MultiInsertRowsPerQuery)
                {
                    yield return sb.ToString() + postfix;
                    sb.Clear();
                    rowCount = 0;
                }
            }
            if (sb.Length > 0)
            {
                yield return sb.ToString() + postfix;
            }
        }
        /// <summary>
        /// Upsert用SQL内の条件部分設定内容を生成します。
        /// </summary>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <param name="insertTargetColumns">insert実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2, t.Col3 }</c>」</param>
        /// <param name="updateTargetColumns">update実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col2, t.Col3 }</c>」</param>
        /// <returns>「on (t.KEY = s.KEY) when matched then update ... when not matched then insert ...」などといったSQL</returns>
        protected string BuildUpsertCond<T>(Expression<Func<T, dynamic>>? insertTargetColumns, Expression<Func<T, dynamic>>? updateTargetColumns)
            where T : notnull
        {
            var tableInfo = GetTableInfo<T>();
            var columns = (insertTargetColumns == null
                ? tableInfo.Columns.Where(c => c.Insert)
                : tableInfo.GetColumns(ExpressionHelper.GetMemberNames(insertTargetColumns.Body))).ToArray();

            // on以降の定型部分を組み立てる
            var sb = new StringBuilder();
            sb.Append(" on (")
                .Append(string.Join(" and ", tableInfo.Columns.Where(c => c.IsKey).Select(c => ("t." + c.Name + "=s." + c.Name))))
                .AppendLine(")");
            sb.Append(" when matched then")
                .Append(BuildUpsertUpdateClause(" update set", "s.?", updateTargetColumns));
            sb.Append(" when not matched then")
                .Append(" insert(")
                .Append(string.Join(",", columns.Select(c => c.Name)))
                .Append(") values(")
                .Append(string.Join(",", columns.Select(column =>
                    (insertTargetColumns != null || string.IsNullOrWhiteSpace(column.InsertSQL))
                        ? "s." + column.Name
                        : column.InsertSQL
                    )))
                .Append(")");
            return sb.ToString();
        }
        /// <summary>
        /// Upsert用SQL内のupdate句設定内容を生成します。
        /// </summary>
        /// <param name="beginning">「do update set」などupdate句の冒頭に設定する内容</param>
        /// <param name="columnValueTemplate">更新値設定構文のテンプレート。カラム名を埋め込む場所に半角ハテナ「?」を設定する</param>
        /// <param name="updateTargetColumns">update実行時の値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col2, t.Col3 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「～update set カラム=設定値, ...」といったSQL</returns>
        /// <remarks>
        /// beginning/columnValueTemplateにはDBMSに応じた適切な内容を設定すること。
        /// 具体的には、Postgres/SQLiteの場合「on conflict(KEY) do update set」「excluded.?」、MySQLの場合「on duplicate key update」「values(?)」、Oracle/SqlServer/DB2の場合「～ update set」「excluded.?」
        /// </remarks>
        protected string BuildUpsertUpdateClause<T>(string beginning, string columnValueTemplate, Expression<Func<T, dynamic>>? updateTargetColumns)
            where T : notnull
        {
            var tableInfo = GetTableInfo<T>();
            var updateColumns = (updateTargetColumns == null)
                ? tableInfo.Columns.Where(c => c.Update)
                : tableInfo.GetColumns(ExpressionHelper.GetMemberNames(updateTargetColumns.Body));
            var sb = new StringBuilder();
            foreach (var column in updateColumns)
            {
                sb.Append(
                    (sb.Length == 0)
                    ? beginning
                    : ",");
                sb.Append(" ")
                    .Append(column.Name)
                    .Append("=")
                    .Append(updateTargetColumns != null || string.IsNullOrWhiteSpace(column.UpdateSQL)
                        ? columnValueTemplate.Replace("?", column.Name)
                        : column.UpdateSQL);
            }
            return sb.ToString();
        }

        /// <summary>
        /// 指定された型のテーブルに対するUPDATE SQLを生成します。
        /// targetColumns省略時は該当テーブルのUpdate対象列すべて(ただしKey項目は除く)、指定時は対象列のみを値更新対象とします。
        /// </summary>
        /// <param name="targetColumns">値更新対象カラムを限定する場合は、対象カラムを初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Col1 = 1, Col2 = 0 }</c>」「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「update [テーブル] set [カラム=設定値,...]」のSQL</returns>
        public string BuildUpdate<T>(LambdaExpression? targetColumns = null)
            where T : notnull
        {
            var tableInfo = GetTableInfo<T>();
            var columns = (targetColumns == null)
                ? tableInfo.Columns.Where(c => c.Update)
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
        /// 指定された型のテーブルに対するUPDATE SQLを生成します。パラメータバインドも行います。
        /// </summary>
        /// <param name="parameters">Dapperパラメーターオブジェクト</param>
        /// <param name="values">更新対象カラム・更新値を初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Value1 = 99, Flg = true }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「update [テーブル] set [カラム=設定値,...]」のSQL</returns>
        public string BuildUpdate<T>(DynamicParameters parameters, Expression<Func<T>> values)
            where T : notnull
        {
            var initExpr = (values.Body as MemberInitExpression);
            if (initExpr == null || initExpr.Bindings.Count == 0)
            {
                throw new ArgumentException("target column unspecified");
            }

            var tableInfo = GetTableInfo<T>();
            var sb = new StringBuilder();
            foreach (var member in initExpr.Bindings)
            {
                var column = tableInfo.GetColumn(member.Member.Name);
                if (sb.Length > 0) { sb.Append(", "); }

                // DapperAid固有のSQL条件式記述用メソッドで値指定している場合は、SQL条件式生成メソッドを呼び出してSQLを組み立てる
                var expression = (member as MemberAssignment)!.Expression;
                var valueSql = TryBindSqlExpr(parameters, null, expression, false);
                if (valueSql == null)
                {   // そうでなければ更新値を取り出してバインド
                    valueSql = AddParameter(parameters, member.Member.Name, ExpressionHelper.EvaluateValue(expression));
                }
                sb.Append(column.Name)
                    .Append("=")
                    .Append(valueSql);
            }
            return "update " + tableInfo.Name + " set " + sb.ToString();
        }

        /// <summary>
        /// 指定された型のテーブルに対するDELETE SQLを生成します。
        /// </summary>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>「delete from [テーブル]」のSQL文</returns>
        public string BuildDelete<T>()
            where T : notnull
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
            where T : notnull
        {
            var tableInfo = GetTableInfo<T>();
            return "truncate table " + tableInfo.Name;
        }


        /// <summary>
        /// (更新SQL向け)指定された条件のカラムによるWhere句を生成します。
        /// </summary>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <param name="data">Where条件対象カラムに値が設定されているオブジェクト</param>
        /// <param name="columnWhere">Where条件対象となるカラムであればtrueを返すラムダ式。例：PK項目を条件とする場合「c => (c.IsKey)」</param>
        /// <returns>SQL文のWhere句</returns>
        public string BuildWhere<T>(T? data, Func<TableInfo.Column, bool> columnWhere)
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
            where T : notnull
        {
            var tableInfo = GetTableInfo<T>();
            var sb = new StringBuilder();
            if (!string.IsNullOrEmpty(tableInfo.SelectSqlInfo?.BaseWhereClauses))
            {
                sb.Append(" where " + tableInfo.SelectSqlInfo?.BaseWhereClauses);
            }

            // ローカルメソッド：where/and条件を組み立てる
            void bindWhere(TableInfo.Column column, object? value)
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
            if (memberExpr != null && ExpressionHelper.EvaluateValue(memberExpr) is T data)
            {   // 特例対応：ラムダの戻り値として指定されたオブジェクトをもとに楽観排他更新のWhere条件式を生成
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
                    var expression = (member as MemberAssignment)!.Expression;
                    var sqlExprBindResult = TryBindSqlExpr(parameters, column, expression, false);
                    if (sqlExprBindResult != null)
                    {   // DapperAid固有のSQL条件式記述用メソッドで値指定している場合は、SQL条件式生成メソッドを呼び出してSQLを組み立てる
                        sb.Append(sb.Length == 0 ? " where " : " and ");
                        sb.Append(sqlExprBindResult);
                    }
                    else
                    {   // そうでなければ条件値をバインド
                        var value = ExpressionHelper.EvaluateValue(expression);
                        bindWhere(column, value);
                    }
                }
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
        public string BuildWhere<T>(DynamicParameters parameters, Expression<Func<T, bool>>? where)
            where T : notnull
        {
            var tableInfo = GetTableInfo<T>();
            var buildedClauses = (where is null) ? null : BuildWhere(parameters, tableInfo, where.Body);
            return (tableInfo.SelectSqlInfo?.BaseWhereClauses is string baseClauses)
                ? (buildedClauses is not null)
                    ? $" where {baseClauses} and {buildedClauses}"
                    : $" where {baseClauses}"
                : (buildedClauses is not null)
                    ? $" where {buildedClauses}"
                    : string.Empty;
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
            Func<Expression, TableInfo.Column?> getIfOperandIsConditionColumn = (exp) =>
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
                return tableInfo.Columns.Where(c => c.PropertyInfo.Name == prop.Name).FirstOrDefault();
            };

            var binaryExpr = ExpressionHelper.CastTo<BinaryExpression>(expression);
            if (binaryExpr == null)
            {   // 二項演算子以外の特殊な表現が指定されている場合の処理

                // 三項演算子が指定されている場合の対処
                if (ExpressionHelper.CastTo<ConditionalExpression>(expression) is ConditionalExpression condExpr)
                {
                    var cond = BuildWhere(parameters, tableInfo, condExpr.Test);
                    if (cond == TrueLiteral) { return BuildWhere(parameters, tableInfo, condExpr.IfTrue); }
                    if (cond == FalseLiteral) { return BuildWhere(parameters, tableInfo, condExpr.IfFalse); }
                    var trueCond = BuildWhere(parameters, tableInfo, condExpr.IfTrue);
                    var falseCond = BuildWhere(parameters, tableInfo, condExpr.IfFalse);

                    if (trueCond == TrueLiteral)
                    {
                        if (falseCond == TrueLiteral) { return TrueLiteral; }
                        if (falseCond == FalseLiteral) { return cond; }
                        return "(" + cond + " or " + falseCond + ")";
                    }
                    else if (trueCond == FalseLiteral)
                    {
                        if (falseCond == TrueLiteral) { return "not(" + cond + ")"; }
                        if (falseCond == FalseLiteral) { return FalseLiteral; }
                        return "not(" + cond + ") and " + falseCond;
                    }
                    else
                    {
                        if (falseCond == TrueLiteral) { return "(not(" + cond + ") or " + trueCond + ")"; }
                        if (falseCond == FalseLiteral) { return cond + " and " + trueCond; }
                        return "((" + cond + " and " + trueCond + ") or (not(" + cond + ") and " + falseCond + "))";
                    }
                }

                // 「!」NOT指定かどうか/bool型のプロパティが指定されているかを判定
                var unaryExpr = ExpressionHelper.CastTo<UnaryExpression>(expression);
                var isNot = (unaryExpr != null && unaryExpr.NodeType == ExpressionType.Not);
                var column = getIfOperandIsConditionColumn(isNot ? unaryExpr!.Operand : expression);
                if (column != null && column.PropertyInfo.PropertyType == typeof(bool))
                {   // boolカラム値の条件として組み立てる
                    return column.Name + "=" + (isNot ? FalseLiteral : TrueLiteral);
                }
                if (isNot)
                {   // NOT条件として組み立てる
                    return "not(" + BuildWhere(parameters, tableInfo, unaryExpr!.Operand) + ")";
                }

                // DapperAid固有のSQL条件式記述用メソッドで式木を記述している場合は、SQL条件式生成メソッドを呼び出してSQLを組み立てる
                var sqlExprBindResult = TryBindSqlExpr(parameters, null, expression, isNot);
                if (sqlExprBindResult != null)
                {
                    return sqlExprBindResult;
                }

                // bool値となる変数や処理を記述していればその値を組み立てる
                var boolValue = ExpressionHelper.EvaluateValue(expression);
                if (boolValue is bool)
                {
                    return ((bool)boolValue == true) ? TrueLiteral : FalseLiteral;
                }

                // いずれでもなければ例外
                throw new InvalidExpressionException(expression.ToString());
            }

            // And/Or条件の解釈
            switch (binaryExpr.NodeType)
            {
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                    var andLeft = BuildWhere(parameters, tableInfo, binaryExpr.Left);
                    if (andLeft == FalseLiteral)
                    {   // 「left && right」のうちleftでfalseが確定している場合は、右辺の展開を行わない
                        return andLeft;
                    }
                    var andRight = BuildWhere(parameters, tableInfo, binaryExpr.Right);
                    // 左辺右辺いずれかでtrueが確定していればもう一方のみを条件とし、そうでなければand条件式を組み立て
                    return (andLeft == TrueLiteral) ? andRight
                        : (andRight == TrueLiteral) ? andLeft
                                                    : andLeft + " and " + andRight;
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                    var orLeft = BuildWhere(parameters, tableInfo, binaryExpr.Left);
                    if (orLeft == TrueLiteral)
                    {   // 「left || right」のうちleftでtrueが確定している場合は、右辺の展開を行わない
                        return orLeft;
                    }
                    var orRight = BuildWhere(parameters, tableInfo, binaryExpr.Right);
                    // 左辺右辺いずれかでfalseが確定していればもう一方のみを条件とし、そうでなければor条件式を組み立て
                    return (orLeft == FalseLiteral) ? orRight
                        : (orRight == FalseLiteral) ? orLeft
                                                    : "(" + orLeft + " or " + orRight + ")";
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

            var condColumn = leftMember ?? rightMember!;
            var valueExpression = ExpressionHelper.CastTo<Expression>(condColumn == leftMember ? binaryExpr.Right : binaryExpr.Left)!;

            // DapperAid固有のSQL条件式記述用メソッドで値指定している場合は、SQL条件式生成メソッドを呼び出してSQLを組み立てる
            {
                var sqlExprBindResult = TryBindSqlExpr(parameters, condColumn, valueExpression, opIsNot);
                if (sqlExprBindResult != null)
                {
                    return sqlExprBindResult;
                }
            }

            // カラム指定でない側の指定値を把握しSQL条件式を組み立てる
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
        public virtual string BuildWhereIn(DynamicParameters parameters, TableInfo.Column column, bool opIsNot, object values)
        {
            return column.Name + (opIsNot ? " not" : "") + " in " + AddParameter(parameters, column.PropertyInfo.Name, values);
            // →DapperのList Support機能により、カッコつきin句「in (@xx1, @xx2, ...)」へ展開されたうえで実行される
        }

        #endregion
    }
}
