using CsvHelper;
using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;

namespace CSVCrossJoin
{
    public class DatabaseLayer
    {
        private string databaseFilePath = "./tmp.db";
        private string tmpTableName = "tmptable";


        private void RemoveDbFile()
        {
            try
            {
                File.Delete(databaseFilePath);
            }
            catch (DirectoryNotFoundException) { }
        }

        /*protected string GenerateDropDbTableSQL()
        {
            return $"DROP TABLE IF EXISTS {tmpTableName}";
        }*/

        protected string CreateDbTableCreateSQLFromCsvHeader(IList<string> headerRow)
        {
            StringBuilder sb = new StringBuilder();

            sb.Append($"CREATE TABLE {tmpTableName} (");

            for (int i = 0; i < headerRow.Count; i += 1)
            {
                sb.Append($"\"{headerRow[i]}\" TEXT");

                if (i != headerRow.Count - 1)
                {
                    sb.Append(',');
                }
            }


            sb.Append(");");

            return sb.ToString();
        }

        protected string CreateInsertSQLFromCsvHeader(IList<string> headerRow)
        {
            StringBuilder sb = new StringBuilder();

            sb.Append($"INSERT INTO {tmpTableName} (");

            for (int i = 0; i < headerRow.Count; i += 1)
            {
                sb.Append($"\"{headerRow[i]}\"");

                if (i != headerRow.Count - 1)
                {
                    sb.Append(',');
                }
            }


            sb.Append(") VALUES (");

            string parametersStr = string.Concat(Enumerable.Repeat("?,", headerRow.Count));
            parametersStr = parametersStr.TrimEnd(',');

            sb.Append(parametersStr);

            sb.Append(");");

            return sb.ToString();
        }

        public void LoadCSVData(string csvFilePath)
        {
            using (StreamReader reader = new StreamReader(csvFilePath))
            {
                using (CsvReader csv = new CsvReader(reader))
                {
                    //csv.Read();
                    if (!csv.Read() || !csv.ReadHeader())
                    {
                        throw new SystemException($"Could not read header row from CSV file \"{csvFilePath}\".");
                    }

                    IList<string> csvHeaderRow = csv.Context.HeaderRecord.ToList();

                    var connectionStringBuilder = new SQLiteConnectionStringBuilder();

                    // Delete the temporary database file if exists
                    RemoveDbFile();

                    //Use DB in project directory.  If it does not exist, create it:
                    connectionStringBuilder.DataSource = databaseFilePath;

                    using (var conn = new SqliteConnection(connectionStringBuilder.ConnectionString))
                    {
                        conn.Open();

                        // Create table for CSV data
                        using (var command = conn.CreateCommand())
                        {
                            string createTableSQL = CreateDbTableCreateSQLFromCsvHeader(csvHeaderRow);
                            command.CommandText = createTableSQL;

                            command.ExecuteNonQuery();
                        }

                        // Load CSV data
                        using (var command = conn.CreateCommand())
                        {
                            string insertSQL = CreateInsertSQLFromCsvHeader(csvHeaderRow);
                            command.CommandText = insertSQL;


                            while (csv.Read())
                            {
                                command.Parameters.Clear();

                                foreach (string value in csv.Context.Record)
                                {
                                    SqliteParameter param = new SqliteParameter();
                                    param.Value = value;

                                    command.Parameters.Add(param);
                                }

                                command.ExecuteNonQuery();
                            }
                        }
                    }
                }
            }
        }
    }
}
