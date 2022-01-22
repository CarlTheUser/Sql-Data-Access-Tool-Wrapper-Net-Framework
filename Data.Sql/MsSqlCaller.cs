using Data.Sql.Provider;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;
using System.Threading.Tasks;

namespace Data.Sql
{
    public class MsSqlCaller : SqlCaller
    {
        public MsSqlCaller(string connection) : base(new SqlServerProvider(connection)) { }

        public void BulkCopy<T>(IEnumerable<T> items, string destination) where T : class, new()
        {
            using (SqlConnection connection = _provider.CreateConnection() as SqlConnection)
            {
                SqlBulkCopy bulkCopy = new SqlBulkCopy(
                    connection,
                    SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.UseInternalTransaction,
                    null);

                bulkCopy.DestinationTableName = destination;

                DataTable table = new DataTable(destination);

                PropertyInfo[] properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);

                foreach (var property in properties)
                {
                    table.Columns.Add(property.Name, property.PropertyType);
                }

                int propertiesLength = properties.Length;

                foreach (var item in items)
                {
                    object[] values = new object[propertiesLength];

                    for (var i = 0; i < properties.Length; ++i)
                    {
                        values[i] = properties[i].GetValue(item, null);
                    }

                    table.Rows.Add(values);
                }

                connection.Open();

                bulkCopy.WriteToServer(table);

                connection.Close();
            }
        }

        public async void BulkCopyAsync<T>(IEnumerable<T> items, string destination) where T : class, new()
        {
            using (SqlConnection connection = _provider.CreateConnection() as SqlConnection)
            {
                SqlBulkCopy bulkCopy = new SqlBulkCopy(
                    connection,
                    SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.UseInternalTransaction,
                    null);

                bulkCopy.DestinationTableName = destination;

                DataTable table = new DataTable(destination);

                PropertyInfo[] properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);

                foreach (var property in properties)
                {
                    table.Columns.Add(property.Name, property.PropertyType);
                }

                int propertiesLength = properties.Length;

                await Task.Run(() => {

                    foreach (var item in items)
                    {
                        object[] values = new object[propertiesLength];

                        for (var i = 0; i < properties.Length; ++i)
                        {
                            values[i] = properties[i].GetValue(item, null);
                        }

                        table.Rows.Add(values);
                    }
                });

                await connection.OpenAsync();

                await bulkCopy.WriteToServerAsync(table);

                connection.Close();
            }
        }
    }
}
