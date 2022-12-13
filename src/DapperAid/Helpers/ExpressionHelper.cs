using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace DapperAid.Helpers
{
    /// <summary>
    /// Expressionについてのヘルパーメソッドを提供します。
    /// </summary>
    public static class ExpressionHelper
    {
        /// <summary>
        /// ExpressionのBoxingを展開し、指定された型に変換して返します。
        /// </summary>
        public static T? CastTo<T>(this Expression exp)
            where T : Expression
        {
            while (exp is UnaryExpression unary && exp.NodeType == ExpressionType.Convert)
            {
                exp = unary.Operand;
            }
            return (exp as T);
        }

        /// <summary>
        /// 式木が表している具体的な値を返します。
        /// </summary>
        /// <param name="exp">値を指定している式木</param>
        /// <returns>値</returns>
        public static object? EvaluateValue(this Expression exp)
        {
            // Boxingを展開
            var expression = exp.CastTo<Expression>();

            if (expression is ConstantExpression constantExpression)
            {   // 定数：値を返す
                return constantExpression.Value;
            }
            if (expression is NewExpression newExpression)
            {   // インスタンス生成：生成されたインスタンスを返す
                var parameters = newExpression.Arguments.Select(EvaluateValue).ToArray();
                return newExpression.Constructor!.Invoke(parameters);
            }
            if (expression is NewArrayExpression newArrayExpression)
            {   // 配列生成：生成された配列を返す
                return newArrayExpression.Expressions.Select(EvaluateValue).ToArray();
            }
            if (expression is MethodCallExpression methodCallExpression)
            {   // メソッド呼び出し：呼び出し結果を返す
                var parameters = methodCallExpression.Arguments.Select(EvaluateValue).ToArray();
                var obj = (methodCallExpression.Object == null) ? null : EvaluateValue(methodCallExpression.Object);
                if (obj is null && methodCallExpression.Object is not null && Nullable.GetUnderlyingType(methodCallExpression.Object!.Type) is not null)
                {   // 2022.12 null許容値型のnullである場合はメソッド呼び出しができないので、メソッド名に応じた値を自前で返す。
                    return methodCallExpression.Method.Name switch
                    {
                        "GetValueOrDefault" => (parameters.Length > 0) ? parameters[0] : InstanceCreator.Create<object>(methodCallExpression.Method.ReturnType),
                        "GetHashCode" => 0,
                        _ => throw new NullReferenceException(methodCallExpression.Method.DeclaringType?.FullName + "." + methodCallExpression.Method.Name),
                    };
                }
                return methodCallExpression.Method.Invoke(obj, parameters);
            }
            if (expression is InvocationExpression invocation)
            {   // ラムダ等の呼び出し：呼び出し結果を返す
                var parameters = invocation.Arguments.Select(x => Expression.Parameter(x.Type)).ToArray();
                var arguments = invocation.Arguments.Select(EvaluateValue).ToArray();
                var lambda = Expression.Lambda(invocation, parameters);
                return lambda.Compile().DynamicInvoke(arguments);
            }
            if (expression is BinaryExpression binaryExpression)
            {
                if (expression.NodeType == ExpressionType.ArrayIndex)
                {   // 配列等のインデクサ：そのインデックスの値を返す
                    var array = (Array)EvaluateValue(binaryExpression.Left)!;
                    var index = Convert.ToInt64(EvaluateValue(binaryExpression.Right));
                    return array.GetValue(index);
                }
                if (expression.NodeType == ExpressionType.Coalesce)
                {   // null結合：null結合結果を返す
                    return EvaluateValue(binaryExpression.Left) ?? EvaluateValue(binaryExpression.Right);
                }
            }
            if (expression is ConditionalExpression conditional)
            {   // 三項演算子：評価結果に応じた値を返す
                return (EvaluateValue(conditional.Test)) switch
                {
                    true => EvaluateValue(conditional.IfTrue),
                    _ => EvaluateValue(conditional.IfFalse),
                };
            }

            // メンバ（フィールドまたはプロパティ）：プロパティ/フィールド値を取り出す
            // ※インスタンスメンバならインスタンス値を再帰把握
            if (expression is MemberExpression member)
            {
                if (member.Member is PropertyInfo pi)
                {
                    return (member.Expression is not null)
                        ? EvaluateValue(member.Expression) switch
                        {
                            null => pi.Name switch
                            {   // 2022.12 null許容値型のnullである場合はプロパティにアクセスできないので、プロパティ名に応じた値を自前で返す。
                                "HasValue" => false,
                                _ => throw new NullReferenceException(pi.DeclaringType?.FullName + "." + pi.Name),
                            },
                            object obj => MemberAccessor.GetValue(obj, pi),
                        }
                        : MemberAccessor.GetStaticValue(pi);
                }
                if (member.Member is FieldInfo fi)
                {
                    return (member.Expression is not null && EvaluateValue(member.Expression) is object obj)
                        ? MemberAccessor.GetValue(obj, fi)
                        : MemberAccessor.GetStaticValue(fi);
                }
            }

            // ここまでの処理で値を特定できなかった：実行して値を取り出す
            return Expression.Lambda(expression!).Compile().DynamicInvoke();
        }


        /// <summary>
        /// 引数の式木で指定されている項目名を返します。
        /// </summary>
        /// <param name="expression">項目を指定している式木</param>
        /// <returns>項目名のコレクション</returns>
        public static IEnumerable<string> GetMemberNames(Expression expression)
        {
            if (expression is NewExpression newExpression)
            {
                var members = newExpression.Members;
                if (members?.Count > 0)
                {
                    return members.Select(m => m.Name);
                }
            }
            else if (expression is MemberInitExpression memberInitExpression)
            {
                var members = memberInitExpression.Bindings;
                if (members.Count > 0)
                {
                    return members.Select(m => m.Member.Name);
                }
            }
            else if (expression is MemberExpression memberExpression)
            {
                return new[] { memberExpression.Member.Name };
            }
            throw new ArgumentException("argument must be Expression specifiing an item name", expression.ToString());
        }
    }
}
