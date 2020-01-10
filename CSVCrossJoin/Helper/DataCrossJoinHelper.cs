using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Linq;
using System.IO;
using CsvHelper;
using CsvHelper.Configuration;

namespace DataCrossJoin.Helper
{
    public class DataCrossJoinHelper
    {
        private const char csvColumnDelimiter = ',';
        private static Configuration CreateCsvConfiguration()
        {
            var config = new Configuration();
            config.Delimiter = csvColumnDelimiter.ToString();
            return config;
        }

        #region handling csv input

        /*public static IList<string> GetColumnNamesFromCSV(string csvFilePath)
        {  
            using (var reader = new StreamReader(csvFilePath, Encoding.UTF8))
            {
                using (var csv = new CsvReader(reader, CreateCsvConfiguration()))
                {
                    csv.Read();
                    csv.ReadHeader();

                    List<string> headerRow = csv.Context.HeaderRecord.ToList();
                    return headerRow;
                }
            }
        }*/

        public static DataTable LoadCSVtoDataTable(string csvFilePath)
        {
            var adapter = new GenericParsing.GenericParserAdapter(csvFilePath, Encoding.UTF8);
            adapter.FirstRowHasHeader = true;
            adapter.ColumnDelimiter = csvColumnDelimiter;

            DataTable sourceDataTable = adapter.GetDataTable();
            return sourceDataTable;
        }

        #endregion

        public static void PerformJoin(DataTable sourceDataTable, IList<string> partitioningColumns, IList<string> keyColumns, bool putKeyColumnsInFront = false)
        {
            //!TODO: check if passed columns are in the csv file. If they are not there will be problems.

            // The coulmns that will be output for each partition. If key (join) columns are to be
            // included in the output, they should not appear in this array.
            List<int> keyColumnIndexes = new List<int>(keyColumns.Count);
            List<int> nonKeyColumnIndexes = new List<int>(sourceDataTable.Columns.Count - keyColumns.Count);

            for (int i = 0; i < sourceDataTable.Columns.Count; i += 1)
            {
                // Add indexes of all the non-key columns
                if (keyColumns.Contains(sourceDataTable.Columns[i].ColumnName))
                {
                    keyColumnIndexes.Add(i);
                }
                else
                {
                    nonKeyColumnIndexes.Add(i);
                }
            }

            List<int> columnsPerPartitionIndexes = null;

            if (putKeyColumnsInFront)
            {
                columnsPerPartitionIndexes = new List<int>(nonKeyColumnIndexes);
            }
            else
            {
                columnsPerPartitionIndexes = new List<int>(Enumerable.Range(0, sourceDataTable.Columns.Count));
            }

            //List<string> partitioningColumns = ;//new List<string>() { "Year" };

            // Sort by key columns ascending (algorithm depends on it)
            string sortFilterStr = string.Join(',', keyColumns.Select(c => $"[{c}] ASC"));
            var tmpView = sourceDataTable.DefaultView;
            tmpView.Sort = sortFilterStr;
            sourceDataTable = tmpView.ToTable();

            DataView sourceDataView = new DataView(sourceDataTable);

            DataTable distinctPartitioningValues = sourceDataView.ToTable(true, partitioningColumns.ToArray());

            List<DataTable> dataTables = new List<DataTable>();

            // Partition input data table into separate DataTables by distinct values of partitioning columns
            // This could also write to files and then read them if low memory usage is an issue
            foreach (DataRow distinctRow in distinctPartitioningValues.Rows)
            {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < distinctRow.ItemArray.Length; i += 1)
                {
                    sb.Append($"[{distinctPartitioningValues.Columns[i].ColumnName}]='{distinctRow[i]}'");

                    if (i != distinctRow.ItemArray.Length - 1)
                    {
                        sb.Append(" AND ");
                    }
                }

                DataTable dt = sourceDataTable.Select(sb.ToString()).CopyToDataTable();

                dataTables.Add(dt);
            }

            List<IEnumerator<DataRow>> dataTablesEnumerators = dataTables.Select(dt => dt.AsEnumerable().GetEnumerator()).ToList();

            /*foreach(var en in dataTablesEnumerators)
            {
                while (en.MoveNext())
                {
                    if (en.Current != null && en.Current["Kod stanowiska"].ToString().Contains("MpKrakBujaka-PM10-1g"))
                    {
                        var a = "abc";
                        break;
                    }
                }
            }*/

            foreach (var en in dataTablesEnumerators)
            {
                en.MoveNext();
            }

            int minEnumeratorIndex = -1;

            while (true)
            {
                DataRow row = null;
                List<string> minKeyColumnVals = null;

                for (int i = 0; i < dataTablesEnumerators.Count; i += 1)
                {
                    if(dataTablesEnumerators[i] == null)
                    {
                        continue;
                    }

                    try

                    {
                        row = dataTablesEnumerators[i].Current;
                    }
                    catch (InvalidOperationException)
                    {
                        continue;
                    }

                    if (row["Kod stanowiska"].ToString().Contains("MpKrakBujaka-PM10-1g"))
                    {
                        var here = true;
                    }

                    for (int j = 0; j < keyColumns.Count; j += 1)
                    {
                        
                        //NOTE: This assumes DataTable will sort the rows using the same method
                        if (minKeyColumnVals == null || row[keyColumns[j]].ToString().CompareTo(minKeyColumnVals[j]) == -1)
                        {
                            minKeyColumnVals = keyColumns.Select(keyColumn => row[keyColumn].ToString()).ToList();
                            minEnumeratorIndex = i;
                            break;
                        }
                    }
                }

                var dr = dataTablesEnumerators[minEnumeratorIndex].Current;
                if (dr != null && dr["Kod stanowiska"].ToString().Contains("MpKrakBujaka-PM10-1g"))
                {
                    var a = dr["Rok"];
                }

                for(int k=0; k < dataTablesEnumerators.Count; k += 1)
                {
                    IEnumerator<DataRow> en = dataTablesEnumerators[k];

                    if (en == null)
                    {
                        continue;
                    }

                    int minColIdx = 0;
                    bool equal = true;
                    foreach (int idx in keyColumnIndexes)
                    {
                        if (en.Current[idx].ToString().CompareTo(minKeyColumnVals[minColIdx]) != 0)
                        {
                            equal = false;
                            break;
                        }
                        minColIdx += 1;
                    }

                    if (equal)
                    {
                        if (en.Current["Kod stanowiska"].ToString().Contains("MpKrakBujaka-PM10-1g"))
                        {
                            var a = dr["Rok"];
                        }
                        var moved1 = en.MoveNext();
                        if (!moved1)
                        {
                            dataTablesEnumerators[k] = null;
                        }
                    }
                }

                if (dataTablesEnumerators[minEnumeratorIndex] != null)
                {
                    if (dataTablesEnumerators[minEnumeratorIndex].Current["Kod stanowiska"].ToString().Contains("MpKrakBujaka-PM10-1g"))
                    {
                        var a = dr["Rok"];
                    }
                    var moved = dataTablesEnumerators[minEnumeratorIndex].MoveNext();
                    if (!moved)
                    {
                        dataTablesEnumerators[minEnumeratorIndex] = null;
                    }
                }
            }

            return;
            
            List<string> minKeyColumnVals1 = null;
            minEnumeratorIndex = -1;

            Func<IEnumerator<DataRow>, bool> moveNext = (IEnumerator<DataRow> en) =>
            {
                if (en.Current != null && en.Current["Kod stanowiska"].ToString().Contains("MpKrakBujaka-PM10-1g"))
                {
                    var a = "abc";
                }
                return en.MoveNext();
            };

            // Move enumerators to first entry
            foreach (var enumerator in dataTablesEnumerators)
            {
                moveNext(enumerator);
            }

            using (TextWriter writer = new StreamWriter(@"out.csv", false, Encoding.UTF8))
            {
                CsvWriter csvWriter = new CsvWriter(writer);

                Action<IEnumerable<string>, IEnumerable<int>> outputTableRowFields = (IEnumerable<string> rowArray, IEnumerable<int> columnIndexes) =>
                {
                    var it = rowArray.GetEnumerator();
                    int itPos = 0;

                    var at = columnIndexes.GetEnumerator();

                    if (!at.MoveNext())
                    {
                        return;
                    }

                    while (it.MoveNext())
                    {
                        if (itPos == at.Current)
                        {
                            csvWriter.WriteField(it.Current);
                            at.MoveNext();
                        }

                        itPos += 1;
                    }
                };

                List<string> columnNames = new List<string>(sourceDataTable.Columns.Count);

                foreach (DataColumn col in sourceDataTable.Columns)
                {
                    columnNames.Add(col.ColumnName);
                }

                if (putKeyColumnsInFront)
                {
                    outputTableRowFields(columnNames, keyColumnIndexes);
                }

                // Writes header row for output CSV
                for (int i = 0; i < dataTables.Count; i += 1)
                {
                    outputTableRowFields(columnNames, columnsPerPartitionIndexes);
                }

                //csvWriter.WriteField(col.ColumnName);

                csvWriter.NextRecord();

                

                // The rest of the code depends on source data table containg some data rows (output column header was handled above)
                if (sourceDataTable.Rows.Count != 0)
                {

                    bool movedNext = true;

                    while (movedNext)
                    {
                        movedNext = false;

                        // Look for "minimum" key column combination
                        for (int i = 0; i < dataTablesEnumerators.Count; i += 1)
                        {
                            //FIXME: THIS CAN THROW IF MoveNext moved the Current past the last element.
                            DataRow row = null;
                            try
                            {
                                row = dataTablesEnumerators[i].Current;

                                if (row["Kod stanowiska"].ToString() == "MpKrakBujaka-PM10-1g")
                                {
                                    string a = "abc";

                                    for (int j = 0; j < dataTables.Count; j += 1)
                                    {
                                           var row1 = dataTables[j].Select("[Kod stanowiska] = 'MpKrakBujaka-PM10-1g'");
                                    }
                                }
                            }
                            catch (InvalidOperationException)
                            {
                                continue;
                            }

                            /*if(minKeyColumnVals != null && minKeyColumnVals[7] == )
                            {
                                while(dataTablesEnumerators[i].Current["Kod stanowiska"] != "MpKrakBujaka-PM10-1g"){
                                    if (!dataTablesEnumerators[i].MoveNext())
                                    {
                                        var dupa = "not found";
                                    }
                                }
                                string a = "abc";
                            }*/

                            // Compare the row's key columns with local minimum
                            for (int j = 0; j < keyColumns.Count; j += 1)
                            {
                                //NOTE: This assumes DataTable will sort the rows using the same method
                                if (minKeyColumnVals1 == null || row[keyColumns[j]].ToString().CompareTo(minKeyColumnVals1[j]) == -1)
                                {
                                    minKeyColumnVals1 = keyColumns.Select(keyColumn => row[keyColumn].ToString()).ToList();
                                    minEnumeratorIndex = i;
                                    break;
                                }
                            }
                        }

                        // Write out the "minimum" key column combination first if key columns are to be put in front
                        if (putKeyColumnsInFront)
                        {
                            foreach (string fieldVal in minKeyColumnVals1)
                            {
                                csvWriter.WriteField(fieldVal);
                            }
                        }

                        // Write all the rows with key colums equal to the found min values
                        for (int i = 0; i < dataTablesEnumerators.Count; i += 1)
                        {
                            //Get the row that the current enumerator points to
                            DataRow row1 = null;
                            try
                            {
                                row1 = dataTablesEnumerators[i].Current;
                            }
                            catch (InvalidOperationException)
                            {
                                continue;
                            }

                            // Since key columns have the same values 

                            // If we are at the index of the enumerator set to minimum entry, no need to compare
                            if (i == minEnumeratorIndex)
                            {
                                //append row data to result row
                                outputTableRowFields(row1.ItemArray.Select(val => val.ToString()), columnsPerPartitionIndexes);

                                //advance enumerator
                                movedNext = moveNext(dataTablesEnumerators[i]) || movedNext;
                            }
                            else
                            {
                                bool equal = true;
                                // Compare the row's key columns with minimum values
                                for (int j = 0; j < keyColumns.Count; j += 1)
                                {
                                    //NOTE: This assumes DataTable will sort the rows using the same method
                                    if (row1[keyColumns[j]].ToString().CompareTo(minKeyColumnVals1[j]) != 0)
                                    {
                                        //found value that is not equal;
                                        equal = false;
                                        break;
                                    }
                                }

                                if (equal)
                                {
                                    //append row data to result row
                                    outputTableRowFields(row1.ItemArray.Select(val => val.ToString()), columnsPerPartitionIndexes);


                                    //advance enumerator
                                    movedNext = moveNext(dataTablesEnumerators[i]) || movedNext;
                                }
                                else
                                {
                                    for (int k = 0; k < columnsPerPartitionIndexes.Count; k += 1)
                                    {
                                        csvWriter.WriteField("");
                                    }
                                }
                            }
                        }

                        if (movedNext)
                        {
                            csvWriter.NextRecord();
                        }
                    }
                }

                csvWriter.Flush();
            }
        }
    }
}
