using System;
using System.Collections.Generic;
using System.Text;

namespace CSVCrossJoin.ViewModel
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
