using System;
using System.Collections.Generic;
using System.Text;

namespace DataCrossJoin.ViewModel
{
    public class SelectableItem<T>
    {
        public SelectableItem(T itemValue, bool isSelected = false)
        {
            ItemValue = itemValue;
            IsSelected = isSelected;
        }

        public bool IsSelected { get; set; }
        public T ItemValue { get; }
    }
}
