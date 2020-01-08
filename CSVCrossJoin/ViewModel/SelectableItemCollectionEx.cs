using System;
using System.Collections.Generic;
using System.Text;

namespace DataCrossJoin.ViewModel
{
    public static class SelectableItemCollectionEx
    {
        /// <summary>
        /// Adds string to the collection as SelectableItem
        /// </summary>
        /// <param name="collection"></param>
        /// <param name="rawString"></param>
        public static void AddRawString(this ICollection<SelectableItem<string>> collection, string rawString, bool isSelected = false)
        {
            collection.Add(new SelectableItem<string>(rawString, isSelected));
        }
    }
}
