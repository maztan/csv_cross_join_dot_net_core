using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Linq;
using System.IO;
using CsvHelper;

namespace CSVCrossJoin.Helper
{
    public class DataCrossJoinHelper
    {
        public static IList<string> GetColumnNamesFromCSV(string csvFilePath)
        {  
            using (var reader = new StreamReader(csvFilePath))
            {
                using (var csv = new CsvReader(reader))
                {
                    csv.Read();
                    csv.ReadHeader();

                    List<string> headerRow = csv.Context.HeaderRecord.ToList();
                    return headerRow;
                }
            }
        }

        public static void PerformJoin(string csvFilePath, IList<string> partitioningColumns, IList<string> keyColumns, bool putKeyColumnsInFront = false)
        {
            //!TODO: check if passed columns are in the csv file. If they are not there will be problems.

            var adapter = new GenericParsing.GenericParserAdapter(csvFilePath);
            adapter.FirstRowHasHeader = true;

            DataTable sourceDataTable = adapter.GetDataTable();

            // The coulmns that will be output for each partition. If key (join) columns are to be
            // included in the output, they should not appear in this array.
            int[] outputColumnsPerPartition = null;

            if (putKeyColumnsInFront)
            {
                outputColumnsPerPartition = new int[sourceDataTable.Columns.Count - keyColumns.Count];
                int arrPos = 0;

                for (int i = 0; i < sourceDataTable.Columns.Count; i += 1)
                {
                    // Add indexes of all the non-key columns
                    if (!keyColumns.Contains(sourceDataTable.Columns[i].ColumnName))
                    {
                        outputColumnsPerPartition[arrPos] = i;
                        arrPos += 1;
                    }
                }
            }
            else
            {
                outputColumnsPerPartition = Enumerable.Range(0, sourceDataTable.Columns.Count).ToArray();
            }

            //List<string> partitioningColumns = ;//new List<string>() { "Year" };

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

            // Source data table is no longer useful
            sourceDataTable.Clear();


            //List<string> keyColumns = new List<string>() { "col1", "col2", "col3" };

            List<DataView> dataViews = new List<DataView>();
            string sortStr = string.Join(",", keyColumns);

            foreach (DataTable dt in dataTables)
            {
                DataView dv = new DataView(dt);
                dv.Sort = sortStr;
                dataViews.Add(dv);
            }

            List<System.Collections.IEnumerator> dataViewsEnumerators = dataViews.Select(dv => dv.GetEnumerator()).ToList();

            List<string> minKeyColumnVals = null;
            int minEnumeratorIndex = -1;

            // Move enumerators to first entry
            foreach (var enumerator in dataViewsEnumerators)
            {
                enumerator.MoveNext();
            }

            using (TextWriter writer = new StreamWriter(@"out.csv", false, Encoding.UTF8))
            {
                CsvWriter csvWriter = new CsvWriter(writer);

                Action<IEnumerable<string>> outputTableRowFunc = (IEnumerable<string> rowArray) =>
                {
                    int atPos = 0;
                    var rowEnum = rowArray.GetEnumerator();

                    var at2 = (IEnumerator<int>)outputColumnsPerPartition.GetEnumerator();

                    while (rowEnum.MoveNext())
                    {
                        if(atPos == at2.MoveNext())
                        {

                        }
                        atPos += 1;
                    }

                    for (int i = 0; i < outputColumnsPerPartition.Length; i += 1)
                    {
                        if(rowEnum.)
                        if (i == outputColumnsPerPartition[i])
                        {
                            csvWriter.WriteField(fields[i]);
                        }
                    }
                };

                // Writes header row for output CSV
                foreach (var dataView in dataViews)
                {
                    foreach(DataColumn col in dataView.Table.Columns)
                    {
                        csvWriter.WriteField(col.ColumnName);
                    }
                }

                csvWriter.NextRecord();

                bool movedNext = true;

                while (movedNext)
                {
                    movedNext = false;
                    minKeyColumnVals = null;

                    // Look for "minimum" key column combination
                    for (int i = 0; i < dataViewsEnumerators.Count; i += 1)
                    {
                        //FIXME: THIS CAN THROW IF MoveNext moved the Current past the last element.
                        DataRowView row = (DataRowView)dataViewsEnumerators[i].Current;

                        // Compare the row's key columns with local minimum
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

                    // Write all the rows with key colums equal to the found min values
                    for (int i = 0; i < dataViewsEnumerators.Count; i += 1)
                    {
                        DataRowView row = (DataRowView)dataViewsEnumerators[i].Current;

                        // If we are at the index of the enumerator set to minimum entry, no need to compare
                        if (i == minEnumeratorIndex)
                        {
                            //append row data to result row
                            outputTableRowFunc(((DataRowView)dataViewsEnumerators[i].Current).Row);

                            //advance enumerator
                            movedNext = dataViewsEnumerators[i].MoveNext() || movedNext;
                        }
                        else
                        {
                            bool equal = true;
                            // Compare the row's key columns with minimum values
                            for (int j = 0; j < keyColumns.Count; j += 1)
                            {
                                //NOTE: This assumes DataTable will sort the rows using the same method
                                if (row[keyColumns[j]].ToString().CompareTo(minKeyColumnVals[j]) != 0)
                                {
                                    //found value that is not equal;
                                    equal = false;
                                    break;
                                }
                            }

                            if (equal)
                            {
                                //append row data to result row
                                outputTableRowFunc(((DataRowView)dataViewsEnumerators[i].Current).Row);

                                //advance enumerator
                                movedNext = dataViewsEnumerators[i].MoveNext() || movedNext;
                            }
                            else
                            {
                                // Append number of empty fields equal to the number of columns in corresponding input data table
                                for (int k = 0; k < dataTables[i].Columns.Count; k += 1)
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
        }
    }
}
