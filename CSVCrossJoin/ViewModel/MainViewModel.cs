using CSVCrossJoin.Helper;
using GalaSoft.MvvmLight.Command;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSVCrossJoin.ViewModel
{
    public class MainViewModel : NotifyPropertyChangedImplBase
    {
        public MainViewModel()
        {
            PartitioningColumns = new ObservableCollection<SelectableItem<string>>();
            JoinColumns = new ObservableCollection<SelectableItem<string>>();

            openCsvFileDialog = new Microsoft.Win32.OpenFileDialog();
            openCsvFileDialog.Filter = "CSV Files|*.csv";

            /*AddDUMMY(PartitioningColumns);
            AddDUMMY(JoinColumns);
            */
        }

        /*private void AddDUMMY(ICollection<SelectableItem<string>> collection)
        {
            for(int i=0; i<10; i += 1)
            {
                collection.Add(new SelectableItem<string>(i.ToString()));
            }
        }*/

        private Microsoft.Win32.OpenFileDialog openCsvFileDialog;

        public string InputFilePath { get; private set; }

        public ObservableCollection<SelectableItem<string>> PartitioningColumns { get; }
        public ObservableCollection<SelectableItem<string>> JoinColumns { get; }

        public bool TaskInProgress { get; private set; } = false;

        private bool _isOpenFileCommandRunning = false;
        private RelayCommand openFileCommand;

        public RelayCommand OpenFileCommand
        {
            get
            {
                return openFileCommand
                  ?? (openFileCommand = new RelayCommand(
                    async () =>
                    {
                        if (_isOpenFileCommandRunning)
                        {
                            return;
                        }

                        _isOpenFileCommandRunning = true;
                        openFileCommand.RaiseCanExecuteChanged();

                        if (openCsvFileDialog.ShowDialog(/*main window ref here*/) == true)
                        {
                            try
                            {
                                TaskInProgress = true;

                                string csvFilePath = openCsvFileDialog.FileName;

                                //await Task.Run(() =>
                                //{
                                IList<string> columnNames = await Task.Run(() =>
                                        DataCrossJoinHelper.GetColumnNamesFromCSV(csvFilePath)
                                    );

                                PartitioningColumns.Clear();
                                JoinColumns.Clear();

                                foreach (string columnName in columnNames)
                                {
                                    PartitioningColumns.AddRawString(columnName);
                                    JoinColumns.AddRawString(columnName);
                                }

                                InputFilePath = csvFilePath;
                            }
                            finally
                            {
                                TaskInProgress = false;
                            }
                            //});
                        }

                        /*IList<SongInfo> songsList = await Task.Run(() =>
                        {
                            try
                            {
                                SelectedWebsite.Downloader.SongInfoAvailable += Downloader_SongInfoAvailable;

                                var songs = SelectedWebsite.Downloader.Parse(SelectedBaseUri.DataString, SelectedRegionFilter, SelectedReccurenceFilter, SelectedDateFilter,
                                            new Progress<double>((p) => Progress = p),
                                            (m) => Message = m,
                                            cancellationTokenSource.Token
                                        );

                                return songs;
                            }
                            finally
                            {
                                SelectedWebsite.Downloader.SongInfoAvailable -= Downloader_SongInfoAvailable;
                            }
                        }, cancellationTokenSource.Token
                        );*/

                        /*foreach (var song in songsList)
                        {
                            SongsList.Add(song);
                        }*/

                        _isOpenFileCommandRunning = false;
                        openFileCommand.RaiseCanExecuteChanged();
                    },
                    () => !_isOpenFileCommandRunning));
            }
        }



        private bool _isPerformJoinCommandRunning = false;
        private RelayCommand performJoinCommand;

        public RelayCommand PerformJoinCommand
        {
            get
            {
                return performJoinCommand
                  ?? (performJoinCommand = new RelayCommand(
                    async () =>
                    {
                    if (_isPerformJoinCommandRunning)
                    {
                        return;
                    }

                    _isPerformJoinCommandRunning = true;
                    performJoinCommand.RaiseCanExecuteChanged();

                        try
                        {
                            TaskInProgress = true;
                            var partitioningColumns = PartitioningColumns.Where(col => col.IsSelected)
                                .Select(col => col.ItemValue).ToList();
                            var joinColumns = JoinColumns.Where(col => col.IsSelected)
                                .Select(col => col.ItemValue).ToList();

                            //TODO: Check input file path, check if column selection is acceptable

                            await Task.Run(() =>
                                DataCrossJoinHelper.PerformJoin(InputFilePath, partitioningColumns, joinColumns, true)
                                );
                        }
                        finally
                        {
                            TaskInProgress = false;
                        }

                        _isPerformJoinCommandRunning = false;
                        performJoinCommand.RaiseCanExecuteChanged();
                    },
                    () => !_isPerformJoinCommandRunning));
            }
        }

    }
}
