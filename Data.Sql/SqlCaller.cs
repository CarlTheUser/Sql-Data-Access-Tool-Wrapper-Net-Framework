using Data.Sql.Mapping;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Data.Sql
{

    public class SqlCaller : ISqlCaller
    {
        protected readonly ISqlProvider _provider;

        public ISqlProvider Provider => _provider;

        public SqlCaller(ISqlProvider sqlProvider)
        {
            _provider = sqlProvider;
        }

        public DataTable Query(DbCommand command)
        {
            DbConnection connection = command.Connection = command.Connection ?? _provider.CreateConnection();
            DbDataReader dr = null;
            try
            {
                connection.Open();
                dr = command.ExecuteReader();
                var dt = new DataTable();
                dt.Load(dr);
                return dt;
            }
            finally
            {
                if(dr != null)
                {
                    dr.Close();
                    dr.Dispose();
                }
                command.Connection = null;
                connection.Close();
                connection.Dispose();
            }
        }

        public DataTable Query(string queryString)
        {
            using(var command = _provider.CreateCommand(queryString))
            {
                return Query(command);
            }
        }

        public async Task<DataTable> QueryAsync(DbCommand command, CancellationToken token)
        {
            DbConnection connection = _provider.CreateConnection();
            command.Connection = connection;
            DbDataReader dr = null;
            try
            {
                await connection.OpenAsync(token);
                dr = await command.ExecuteReaderAsync(token);
                var table = new DataTable();
                table.Load(dr);
                return table;
            }
            finally
            {
                command.Connection = null;
                dr?.Close();
                connection.Close();
                dr?.Dispose();
                connection.Dispose();
            }
        }

        public async Task<DataTable> QueryAsync(string queryString, CancellationToken token)
        {
            using(var command = _provider.CreateCommand(queryString))
            {
                return await QueryAsync(command, token);
            }
        }

        public DataTable GetSchema(string queryString)
        {
            DbConnection connection = _provider.CreateConnection();
            DbCommand command = connection.CreateCommand();
            command.CommandText = queryString;
            DbDataReader dr = null;
            try
            {
                connection.Open();
                dr = command.ExecuteReader();
                return dr.GetSchemaTable();
            }
            finally
            {
                command.Dispose();
                dr?.Close();
                connection.Close();
                dr?.Dispose();
                connection.Dispose();
            }
        }

        public async Task<DataTable> GetSchemaAsync(string queryString, CancellationToken token)
        {
            DbConnection connection = _provider.CreateConnection();
            DbCommand command = connection.CreateCommand();
            command.CommandText = queryString;
            DbDataReader dr = null;
            try
            {
                await connection.OpenAsync(token);
                dr = await command.ExecuteReaderAsync(token);
                return dr.GetSchemaTable();
            }
            finally
            {
                command.Dispose();
                dr?.Close();
                connection.Close();
                dr?.Dispose();
                connection.Dispose();
            }
        }

        public int ExecuteNonQuery(DbCommand command)
        {
            using (DbConnection connection = _provider.CreateConnection())
            {
                command.Connection = connection;
                try
                {
                    connection.Open();
                    return command.ExecuteNonQuery();
                }
                finally
                {
                    command.Connection = null;
                    connection.Close();
                }
            }
        }

        public int ExecuteNonQuery(string commandString)
        {
            using(var command = _provider.CreateCommand(commandString))
            {
                return ExecuteNonQuery(commandString);
            }
        }

        public async Task<int> ExecuteNonQueryAsync(DbCommand command, CancellationToken token)
        {
            DbConnection connection = _provider.CreateConnection();
            command.Connection = connection;
            try
            {
                await connection.OpenAsync(token);
                return await command.ExecuteNonQueryAsync(token);
            }
            finally
            {
                command.Connection = null;
                connection.Close();
            }
        }

        public async Task<int> ExecuteNonQueryAsync(string commandString, CancellationToken token)
        {
            using(var command = _provider.CreateCommand(commandString))
            {
                return await ExecuteNonQueryAsync(command, token);
            }
        }

        public object ExecuteScalar(DbCommand command)
        {
            DbConnection connection = _provider.CreateConnection();
            command.Connection = connection;
            try
            {
                connection.Open();
                return command.ExecuteScalar();
            }
            finally
            {
                command.Connection = null;
                connection.Close();
            }
        }

        public object ExecuteScalar(string queryString)
        {
            using(var command = _provider.CreateCommand(queryString))
            {
                return ExecuteScalar(command);
            }
        }

        public async Task<object> ExecuteScalarAsync(DbCommand command, CancellationToken token)
        {
            DbConnection connection = _provider.CreateConnection();
            command.Connection = connection;
            try
            {
                await connection.OpenAsync(token);
                return await command.ExecuteScalarAsync(token);
            }
            finally
            {
                command.Connection = null;
                connection.Close();
            }
        }

        public async Task<object> ExecuteScalarAsync(string queryString, CancellationToken token)
        {
            using(var command = _provider.CreateCommand(queryString))
            {
                return await ExecuteScalarAsync(command, token);
            }
        }

        public void Transact(IsolationLevel isolationLevel, Queue<Action<DbCommand>> commandActions, Action<string> onCommandFailed)
        {
            if (commandActions.FirstOrDefault() == null) return;

            DbConnection connection = _provider.CreateConnection();
            DbCommand command = connection.CreateCommand();
            DbTransaction transaction = null;
            try
            {
                connection.Open();
                transaction = connection.BeginTransaction(isolationLevel);
                command.Transaction = transaction;

                foreach (Action<DbCommand> commandAction in commandActions)
                {
                    commandAction.Invoke(command);
                    command.ExecuteNonQuery();
                    command.Parameters.Clear();
                }
                transaction.Commit();
            }
            catch (Exception)
            {
                transaction?.Rollback();

                if (onCommandFailed != null) onCommandFailed.Invoke(command.CommandText);

                throw;
            }
            finally
            {
                transaction?.Dispose();
                command.Dispose();
                connection.Close();
                connection.Dispose();
            }
        }

        public async Task TransactAsync(IsolationLevel isolationLevel, Queue<Action<DbCommand>> commandActions, Action<string> onCommandFailed, CancellationToken token)
        {
            if (!commandActions.Any()) return;

            DbConnection connection = _provider.CreateConnection();
            DbCommand command = connection.CreateCommand();
            DbTransaction transaction = null;
            try
            {
                await connection.OpenAsync(token);
                transaction = connection.BeginTransaction(isolationLevel);
                command.Transaction = transaction;

                foreach (Action<DbCommand> commandAction in commandActions)
                {
                    commandAction.Invoke(command);
                    await command.ExecuteNonQueryAsync(token);
                    command.Parameters.Clear();
                }

                transaction.Commit();
            }
            catch
            {
                transaction?.Rollback();
                if (onCommandFailed != null) onCommandFailed.Invoke(command.CommandText);
                throw;
            }
            finally
            {
                transaction?.Dispose();
                command.Dispose();
                connection.Close();
                connection.Dispose();
            }
        }

        public void OperateCollection<T>(IEnumerable<T> collection, Action<DbCommand> commandInitializer, Action<DbCommand, T> bindingAction, IsolationLevel isolationLevel, Action<T> onItemFail)
        {
            if (collection.FirstOrDefault() == null) return;

            DbConnection connection = _provider.CreateConnection();
            DbTransaction transaction = null;
            DbCommand command = connection.CreateCommand();
            T[] copy = collection.ToArray();
            int count = copy.Length;
            T current = default;
            try
            {
                connection.Open();
                transaction = connection.BeginTransaction(isolationLevel);
                command.Transaction = transaction;
                commandInitializer.Invoke(command);
                for (int i = 0; i != count; ++i)
                {
                    current = copy[i];
                    bindingAction.Invoke(command, current);
                    command.ExecuteNonQuery();
                    command.Parameters.Clear();
                }
                transaction.Commit();
            }
            catch (Exception)
            {
                transaction?.Rollback();
                onItemFail.Invoke(current);
                throw;
            }
            finally
            {
                transaction?.Dispose();
                connection.Close();
                command.Dispose();
                connection.Dispose();
            }
        }

        public async Task OperateCollectionAsync<T>(IEnumerable<T> collection, Action<DbCommand> commandInitializer, Action<DbCommand, T> bindingAction, IsolationLevel isolationLevel, Action<T> onItemFail, CancellationToken token)
        {
            if (!collection.Any()) return;

            DbConnection connection = _provider.CreateConnection();
            DbTransaction transaction = null;
            DbCommand command = connection.CreateCommand();
            T[] copy = collection.ToArray();
            int count = copy.Length;
            T current = default;
            try
            {
                await connection.OpenAsync(token);
                transaction = connection.BeginTransaction(isolationLevel);
                command.Transaction = transaction;
                commandInitializer.Invoke(command);
                for (int i = 0; i != count; ++i)
                {
                    current = copy[i];
                    bindingAction.Invoke(command, current);
                    await command.ExecuteNonQueryAsync(token);
                    command.Parameters.Clear();
                }
                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();
                onItemFail.Invoke(current);
                throw;
            }
            finally
            {
                transaction?.Dispose();
                connection.Close();
                command.Dispose();
                connection.Dispose();
            }
        }

        public SqlTransaction CreateScopedTransaction(IsolationLevel isolationLevel)
        {
            return new SqlTransaction(_provider.CreateOpenedConnection(), isolationLevel);
        }

        public IEnumerable<T> Get<T>(Func<IDataReader, List<T>> mappingMethod, string query)
        {
            using(var command = _provider.CreateCommand(query))
            {
                return Get(mappingMethod, command);
            }
        }

        public IEnumerable<T> Get<T>(Func<IDataReader, List<T>> mappingMethod, DbCommand command)
        {
            if (mappingMethod == null) throw new ArgumentNullException(nameof(mappingMethod));
            DbConnection connection = _provider.CreateConnection();
            command.Connection = connection;
            try
            {
                command.Connection.Open();
                return mappingMethod.Invoke(command.ExecuteReader());
            }
            finally
            {
                command.Connection = null;
                connection.Close();
                connection.Dispose();
            }
        }

        public IEnumerable<T> Get<T>(IDataMapper<T> dataMapper, DbCommand command) where T : class, new()
        {
            DbConnection connection = command.Connection = command.Connection ?? _provider.CreateConnection();
            try
            {
                connection.Open();
                IDataReader reader = command.ExecuteReader();
                List<T> temp = new List<T>();
                while (reader.Read()) temp.Add(dataMapper.CreateMappedInstance(reader));
                return temp;
            }
            finally
            {
                command.Connection = null;
                connection.Close();
                connection.Dispose();
            }
        }

        public IEnumerable<T> Get<T>(IDataMapper<T> dataMapper, string query) where T : class, new()
        {
            using(var command = _provider.CreateCommand(query))
            {
                return Get(dataMapper, command);
            }
        }

        public IEnumerable<T> Get<T>(DbCommand command) where T : class, new()
        {
            return Get(new ReflectionDataMapper<T>(), command);
        }

        public IEnumerable<T> Get<T>(string query) where T : class, new()
        {
            using (var command = _provider.CreateCommand(query))
            {
                return Get<T>(command);
            }
        }

        public async Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, DbCommand command, CancellationToken token) where T : class, new()
        {
            DbConnection connection = command.Connection = command.Connection ?? _provider.CreateConnection();
            DbDataReader reader = null;
            try
            {
                List<T> temp = new List<T>();
                await connection.OpenAsync(token);
                reader = await command.ExecuteReaderAsync(token);
                while (await reader.ReadAsync(token)) temp.Add(dataMapper.CreateMappedInstance(reader));
                return temp;
            }
            finally
            {
                if(reader != null)
                {
                    reader.Close();
                    reader.Dispose();
                }
                command.Connection = null;
                connection.Close();
                connection.Dispose();
            }
        }

        public async Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, string query, CancellationToken token) where T : class, new()
        {
            using(var command = _provider.CreateCommand(query))
            {
                return await GetAsync(dataMapper, command, token);
            }
        }

        public async Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, DbCommand command) where T : class, new()
        {
            return await GetAsync(dataMapper, command, CancellationToken.None);
        }

        public async Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, string query) where T : class, new()
        {
            using (var command = _provider.CreateCommand(query))
            {
                return await GetAsync(dataMapper, command);
            }
        }

        public async Task<IEnumerable<T>> GetAsync<T>(DbCommand command) where T : class, new()
        {
            return await GetAsync(new ReflectionDataMapper<T>(), command);
        }

        public async Task<IEnumerable<T>> GetAsync<T>(string query) where T : class, new()
        {
            using(var command = _provider.CreateCommand(query))
            {
                return await GetAsync<T>(command);
            }
        }

        public IEnumerable<dynamic> GetDynamic(DbCommand command)
        {
            throw new NotImplementedException();
        }
        public IEnumerable<dynamic> GetDynamic(string commandString)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<dynamic>> GetDynamicAsync(DbCommand command, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public void Iterate<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, DbCommand command) where T : class, new()
        {
            DbConnection connection = command.Connection = command.Connection ?? _provider.CreateConnection();
            DbDataReader reader = null;
            try
            {
                connection.Open();
                reader = command.ExecuteReader();
                while (reader.Read()) iteratorAction.Invoke(dataMapper.CreateMappedInstance(reader));
            }
            finally
            {
                if(reader != null)
                {
                    reader.Close();
                    reader.Dispose();
                }
                command.Connection = null;
                connection.Close();
                connection.Dispose();
            }
        }

        public void Iterate<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, string query) where T : class, new()
        {
            Iterate(dataMapper, iteratorAction, _provider.CreateCommand(query));
        }

        public async Task IterateAsync<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, DbCommand command, CancellationToken token) where T : class, new()
        {
            DbConnection connection = command.Connection = command.Connection ?? _provider.CreateConnection();
            DbDataReader reader = null;
            try
            {
                await connection.OpenAsync(token);
                reader = await command.ExecuteReaderAsync(token);
                while (await reader.ReadAsync(token))
                {
                    iteratorAction.Invoke(dataMapper.CreateMappedInstance(reader));
                }
            }
            finally
            {
                if(reader != null)
                {
                    reader.Close();
                    reader.Dispose();
                }
                command.Connection = null;
                connection.Close();
                connection.Dispose();
            }
        }

        public async Task IterateAsync<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, string query, CancellationToken token) where T : class, new()
        {
            using(var command = _provider.CreateCommand(query))
            {
                await IterateAsync(dataMapper, iteratorAction, command, token);
            }
        }
    }
}
