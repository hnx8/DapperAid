using System;
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

        #region テーブル/カラム構造 --------------------------------------------

        /// <summary>テーブル情報のキャッシュ</summary>
        protected readonly IDictionary<Type, TableInfo> Tables = new Dictionary<Type, TableInfo>();

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
            TableInfo tableInfo;
            lock (this.Tables)
            {
                if (!Tables.TryGetValue(tableType, out tableInfo))
                {
                    tableInfo = TableInfo.Create(tableType, EscapeIdentifier);
                    Tables.Add(tableType, tableInfo);
                }
            }
            return tableInfo;
        }

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

        #endregion

        #region パラメータバインド ---------------------------------------------

        /// <summary>
        /// パラメータをバインドします。
        /// </summary>
        /// <param name="parameters">パラメーターオブジェクト</param>
        /// <param name="proposedName">パラメータ名の候補（すでに使用されていなければ採用）</param>
        /// <param name="value">パラメータ値</param>
        /// <returns>バインドされたマーカープリフィックス付きのパラメータ名。例："@Key1"</returns>
        protected virtual string AddParameter(DynamicParameters parameters, string proposedName, object value)
        {
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
        /// <param name="targetColumns">値取得対象カラムを限定している場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
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
        /// 指定された型のテーブルのKey項目を条件としたWhere句を生成します。
        /// </summary>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <param name="data">Where条件対象カラムに値が設定されているオブジェクト</param>
        /// <param name="withConcurrencyCheck">楽観排他更新用SQLのWhere条件を生成する場合、true</param>
        /// <returns>SQL文のWhere句</returns>
        public string BuildWhere<T>(T data, bool withConcurrencyCheck = false)
        {
            var tableInfo = GetTableInfo<T>();
            var columns = tableInfo.Columns.Where(c => c.IsKey || (withConcurrencyCheck && c.ConcurrencyCheck));
            return BuildWhere<T>(columns, data);
        }

        /// <summary>
        /// 指定されたカラムの値を条件としたWhere句を生成します。
        /// </summary>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <param name="columns">Where条件対象カラムの列挙</param>
        /// <param name="data">Where条件対象カラムに値が設定されているオブジェクト</param>
        /// <param name="parameters">パラメータバインドも行う場合は、Dapperパラメーターオブジェクト</param>
        /// <returns>SQL文のWhere句</returns>
        protected internal string BuildWhere<T>(IEnumerable<TableInfo.Column> columns, T data, DynamicParameters parameters = null)
        {
            var sb = new StringBuilder();
            foreach (var column in columns)
            {
                if (sb.Length > 0) { sb.Append(" and "); }
                var value = (data == null ? null : MemberAccessor.GetValue(data, column.PropertyInfo));
                if (data != null && value == null)
                {
                    sb.Append(column.Name).Append(" is null");
                }
                else
                {
                    sb.Append(column.Name)
                        .Append("=")
                        .Append(parameters == null
                            ? (ParameterMarker + column.PropertyInfo.Name)
                            : AddParameter(parameters, column.PropertyInfo.Name, value));
                }
            }
            return (sb.Length == 0 ? "" : " where " + sb.ToString());
        }

        /// <summary>
        /// 指定された項目を条件としたWhere条件SQLを生成します。パラメータバインドも行います。
        /// </summary>
        /// <param name="parameters">Dapperパラメーターオブジェクト（ref：未生成の場合はインスタンスを生成する）</param>
        /// <param name="keyValues">レコード特定Key値を初期化子で指定するラムダ式。例：「<c>() => new Tbl1 { Key1 = 1, Key2 = 99 }</c>」</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>SQL文のWhere句</returns>
        public string BuildWhere<T>(ref DynamicParameters parameters, Expression<Func<T>> keyValues)
        {
            var memberExpr = (keyValues.Body as MemberExpression);
            if (memberExpr != null)
            {   // 特例対応：ラムダの戻り値として指定されたオブジェクトをもとに楽観排他更新のWhere条件式を生成して返す
                var data = (T)ExpressionHelper.EvaluateValue(memberExpr);
                if (parameters == null) { parameters = new DynamicParameters(); }
                var whereColumns = GetTableInfo<T>().Columns.Where(c => c.IsKey || c.ConcurrencyCheck);
                return BuildWhere<T>(whereColumns, data, parameters);
            }

            var initExpr = (keyValues.Body as MemberInitExpression);
            if (initExpr == null || initExpr.Bindings.Count == 0)
            {
                throw new ArgumentException("no condition column");
            }

            var tableInfo = GetTableInfo<T>();
            var sb = new StringBuilder();
            foreach (var member in initExpr.Bindings)
            {
                var column = tableInfo.GetColumn(member.Member.Name);
                var value = ExpressionHelper.EvaluateValue((member as MemberAssignment).Expression);
                if (sb.Length > 0) { sb.Append(" and "); }
                if (value != null)
                {
                    if (parameters == null) { parameters = new DynamicParameters(); }
                    sb.Append(column.Name)
                        .Append("=")
                        .Append(AddParameter(parameters, column.PropertyInfo.Name, value));
                }
                else
                {
                    sb.Append(column.Name).Append(" is null");
                }
            }
            return " where " + sb.ToString();
        }

        /// <summary>
        /// 引数で指定された条件式ラムダに基づきWhere条件SQLを生成します。
        /// where条件値のパラメータバインドも行います。
        /// </summary>
        /// <param name="parameters">Dapperパラメーターオブジェクト（ref：未生成の場合はインスタンスを生成する）</param>
        /// <param name="where">式木（ラムダ）により記述された条件式</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>生成されたWhere句（条件指定なしの場合は空文字）</returns>
        public string BuildWhere<T>(ref DynamicParameters parameters, Expression<Func<T, bool>> where)
        {
            if (where == null)
            {
                return string.Empty;
            }

            if (parameters == null) { parameters = new DynamicParameters(); }
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
                    return column.Name + "=" + (isNot ? "false" : "true");
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
                    if (andLeft == "false")
                    {   // 「left && right」のうちleftでfalseが確定している場合は、右辺の展開を行わない
                        return andLeft;
                    }
                    var andRight = BuildWhere(parameters, tableInfo, binaryExpr.Right);
                    if (andLeft == "true")
                    {   // 「left && right」のうちleftがtrueなら、右辺のみを返す
                        return andRight;
                    }
                    return andLeft + " and " + andRight;
                case ExpressionType.OrElse:
                    var orLeft = BuildWhere(parameters, tableInfo, binaryExpr.Left);
                    if (orLeft == "true")
                    {   // 「left || right」のうちleftでtrueが確定している場合は、右辺の展開を行わない
                        return orLeft;
                    }
                    var orRight = BuildWhere(parameters, tableInfo, binaryExpr.Right);
                    if (orLeft == "false")
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
                return ((bool)boolValue ? "true" : "false");
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

        #region SQL実行(DBMS固有) ----------------------------------------------

        /// <summary>
        /// 指定されたレコードを挿入し、[InsertSQL(RetrieveInsertedId = true)]属性で指定された自動連番カラムに採番されたIDを当該プロパティにセットします。
        /// </summary>
        /// <param name="data">挿入するレコード</param>
        /// <param name="targetColumns">値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <param name="connection">DB接続</param>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入された行数(=1件)</returns>
        /// <remarks>
        /// 自動連番に対応していないテーブル/DBMSでは例外がスローされます。
        /// サブクラスによりオーバーライドされることがあります（Oracleなどでは大幅に挙動が変わります）
        /// </remarks>
        public virtual int InsertAndRetrieveId<T>(T data, Expression<Func<T, dynamic>> targetColumns, IDbConnection connection, IDbTransaction transaction, int? timeout = null)
        {
            var tableInfo = GetTableInfo<T>();
            if (tableInfo.RetrieveInsertedIdColumn == null)
            {
                throw new ConstraintException("RetrieveInsertedId-Column not specified");
            }

            var sql = BuildInsert<T>(targetColumns) + " " + GetInsertedIdReturningSql<T>(tableInfo.RetrieveInsertedIdColumn);
            var insertedId = connection.ExecuteScalar(sql, data, transaction, timeout);
            MemberAccessor.SetValue(data, tableInfo.RetrieveInsertedIdColumn.PropertyInfo, Convert.ChangeType(insertedId, tableInfo.RetrieveInsertedIdColumn.PropertyInfo.PropertyType));

            return 1;
        }

        /// <summary>
        /// 採番された自動連番値を返すSQL句を返します。
        /// </summary>
        /// <param name="column">自動採番カラムの情報</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>insertSQLの末尾に付加すべき採番ID値取得句(またはセミコロン＋別のSQL)</returns>
        /// <remarks>
        /// サブクラスによりオーバーライドされ、DBMSに応じたSQLが生成されます。
        /// </remarks>
        protected virtual string GetInsertedIdReturningSql<T>(TableInfo.Column column)
        {
            // 具体的なDBMSがわからないと自動連番値の取り出し方法が定まらないため既定では例外とする。
            throw new NotSupportedException("use DBMS-specific QueryBuilder");
        }

        /// <summary>
        /// 指定されたレコードを一括挿入します。
        /// </summary>
        /// <param name="data">挿入するレコード（複数件）</param>
        /// <param name="targetColumns">値設定対象カラムを限定する場合は、対象カラムについての匿名型を返すラムダ式。例：「<c>t => new { t.Col1, t.Col2 }</c>」</param>
        /// <param name="connection">DB接続</param>
        /// <param name="transaction">DBトランザクション</param>
        /// <param name="timeout">タイムアウト時間</param>
        /// <typeparam name="T">テーブルにマッピングされた型</typeparam>
        /// <returns>挿入された行数</returns>
        /// <remarks>
        /// サブクラスによりオーバーライドされることがあります（DBMSによっては一括インサートにて高速にデータ挿入が行われます）
        /// </remarks>
        public virtual int InsertRows<T>(IEnumerable<T> data, Expression<Func<T, dynamic>> targetColumns, IDbConnection connection, IDbTransaction transaction, int? timeout = null)
        {
            var sql = this.BuildInsert<T>(targetColumns);
            return connection.Execute(sql, data, transaction, timeout);
        }

        #endregion
    }
}
