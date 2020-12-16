using System;
using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace DapperAid.Helpers
{
    /// <summary>
    ///オブジェクトについての高速な生成メソッドを提供します。
    /// </summary>
    public static class InstanceCreator
    {
        /// <summary>オブジェクト生成ファクトリのキャッシュ</summary>
        private static readonly ConcurrentDictionary<Type, Delegate> factories = new ConcurrentDictionary<Type, Delegate>();

        /// <summary>
        /// 引数で指定された型のオブジェクトを生成します。
        /// </summary>
        /// <typeparam name="T">戻り値の型</typeparam>
        /// <param name="t">生成するオブジェクトの型</param>
        /// <returns>生成されたオブジェクト</returns>
        public static T Create<T>(Type t) where T : class
        {
            Delegate factory;
            if (!factories.TryGetValue(t, out factory))
            {
                factory = Expression.Lambda<Func<object>>(Expression.New(t)).Compile();
                factories[t] = factory;
            }
            return (factory as Func<object>)() as T;
        }
    }
}
