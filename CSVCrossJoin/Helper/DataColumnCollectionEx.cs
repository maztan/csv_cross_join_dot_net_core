using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace DataCrossJoin.Helper
{
    public static class DataColumnCollectionEx
    {
        public static List<string> GetColumnNames(this DataColumnCollection columnCollection)
        {
            List<string> columnNames = new List<string>(columnCollection.Count);

            foreach(DataColumn col in columnCollection)
            {
                columnNames.Add(col.ColumnName);
            }

            return columnNames;
        }
    }
}
