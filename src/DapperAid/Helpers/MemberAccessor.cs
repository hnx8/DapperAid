using System;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

namespace DapperAid.Helpers
{
    /// <summary>
    /// プロパティ（およびフィールド）についての高速なアクセッサーメソッドを提供します。
    /// </summary>
    public static class MemberAccessor
    {
        /// <summary>Getterのキャッシュ</summary>
        private static readonly ConcurrentDictionary<MemberInfo, Func<object, object>> getters = new ConcurrentDictionary<MemberInfo, Func<object, object>>();
        /// <summary>静的なGetterのキャッシュ</summary>
        private static readonly ConcurrentDictionary<MemberInfo, Func<object>> staticGetters = new ConcurrentDictionary<MemberInfo, Func<object>>();
        /// <summary>Setterのキャッシュ</summary>
        private static readonly ConcurrentDictionary<MemberInfo, Action<object, object>> setters = new ConcurrentDictionary<MemberInfo, Action<object, object>>();

        // ---------------------------------------------------------------------

        /// <summary>
        /// 引数で指定されたインスタンスプロパティの値を返します。
        /// </summary>
        /// <param name="obj">オブジェクトのインスタンス</param>
        /// <param name="prop">取得したいプロパティ</param>
        /// <returns>インスタンスプロパティの値</returns>
        public static object GetValue(object obj, PropertyInfo prop)
        {
            Func<object, object> getter;
            if (!getters.TryGetValue(prop, out getter!))
            {
                var target = Expression.Parameter(typeof(object), "target");
                var fieldExp = Expression.Property(Expression.Convert(target, prop.DeclaringType!), prop);
                getter = Expression.Lambda<Func<object, object>>(
                    Expression.Convert(fieldExp, typeof(object)), target
                ).Compile();
                getters[prop] = getter;
            }
            return (getter as Func<object, object>)(obj);
        }

        /// <summary>
        /// 引数で指定されたインスタンスプロパティの値を設定します。
        /// </summary>
        /// <param name="obj">オブジェクトのインスタンス</param>
        /// <param name="prop">設定対象のプロパティ</param>
        /// <param name="value">設定する値</param>
        public static void SetValue(object obj, PropertyInfo prop, object value)
        {
            Action<object, object> setter;
            if (!setters.TryGetValue(prop, out setter!))
            {
                var instance = Expression.Parameter(typeof(object), "instance");
                var target = Expression.Parameter(typeof(object), "target");
                setter = Expression.Lambda<Action<object, object>>(
                    Expression.Call(
                        Expression.Convert(instance, prop.DeclaringType!),
                        prop.GetSetMethod(true)!,
                        Expression.Convert(target, prop.PropertyType)
                        ),
                    new[] { instance, target }
                    ).Compile();
                setters[prop] = setter;
            }
            (setter as Action<object, object>)(obj, value);
        }

        /// <summary>
        /// 引数で指定されたインスタンスフィールドの値を返します。
        /// </summary>
        /// <param name="obj">オブジェクトのインスタンス</param>
        /// <param name="field">取得したいフィールド</param>
        /// <returns>インスタンスフィールドの値</returns>
        public static object GetValue(object obj, FieldInfo field)
        {
            Func<object, object> getter;
            if (!getters.TryGetValue(field, out getter!))
            {
                var target = Expression.Parameter(typeof(object), "target");
                var fieldExp = Expression.Field(Expression.Convert(target, field.DeclaringType!), field);
                getter = Expression.Lambda<Func<object, object>>(
                    Expression.Convert(fieldExp, typeof(object)), target
                ).Compile();
                getters[field] = getter;
            }
            return (getter as Func<object, object>)(obj);
        }

        /// <summary>
        /// 引数で指定された静的プロパティの値を返します。
        /// </summary>
        /// <param name="prop">取得したいプロパティ</param>
        /// <returns>静的プロパティの値</returns>
        public static object GetStaticValue(PropertyInfo prop)
        {
            Func<object> getter;
            if (!staticGetters.TryGetValue(prop, out getter!))
            {
                var fieldExp = Expression.Property(null, prop);
                getter = Expression.Lambda<Func<object>>(
                    Expression.Convert(fieldExp, typeof(object))
                ).Compile();
                staticGetters[prop] = getter;
            }
            return (getter as Func<object>)();
        }

        /// <summary>
        /// 引数で指定された静的フィールドの値を返します。
        /// </summary>
        /// <param name="field">取得したいフィールド</param>
        /// <returns>静的フィールドの値</returns>
        public static object GetStaticValue(FieldInfo field)
        {
            Func<object> getter;
            if (!staticGetters.TryGetValue(field, out getter!))
            {
                var fieldExp = Expression.Field(null, field);
                getter = Expression.Lambda<Func<object>>(
                    Expression.Convert(fieldExp, typeof(object))
                ).Compile();
                staticGetters[field] = getter;
            }
            return (getter as Func<object>)();
        }
    }
}
