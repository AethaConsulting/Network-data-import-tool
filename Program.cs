using System;
using System.Collections.Generic;
using System.IO;
using FastMember;
using Microsoft.Data.SqlClient;
using System.Diagnostics;

namespace Sanity_Checks
{
    class Program
    {

        public static List<Data> dataStore_Temp;
        public static List<Data> dataStore_2020;
        public static List<Data> dataStore_2021;

        public static List<string> missingIds;

        public static List<string> wrongDates;
        public static List<string> wrongTimes;
        public static List<string> correctTimes;

        public static List<string> wrongDLTraffic;
        public static List<string> wrongULTraffic;
        public static List<string> wrongRBUtilisation;
        public static List<string> wrongUserThroughput;
        public static List<string> wrongCellThroughput;
        public static List<string> wrongAverageUsers;
        public static List<string> wrongActiveCellTime;
        public static List<string> wrongCQI;

        public static List<string> acceptedValues;


        public static List<string> allIds;

        public static List<string> KPIs;

        public static Functions functions { get; private set; }

        static void Main(string[] args)
        {
            

            // Constructing functions class in main
            functions = new Functions();

            // Directories to pull files from

            //var di = new DirectoryInfo("C:\\Temp\\AETIS08\\2020 4G traffic data\\TESTING");
            //var di = new DirectoryInfo("C:\\Temp\\AETIS08\\2021 4G and 5G traffic data");
            var di = new DirectoryInfo("C:\\Temp");

            // Lists of data (rows in csv) for 2020, 2021 and a temp store that is reset after each file
            dataStore_2020 = new List<Data>();
            dataStore_2021 = new List<Data>();
            dataStore_Temp = new List<Data>();

            // A list of string that will be populated with any Ids not contained in Cell_Mappings table in db
            missingIds = new List<string>();
            wrongDates = new List<string>();
            wrongTimes = new List<string>();
            correctTimes = new List<string>() { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23" };
            wrongDLTraffic = new List<string>();
            wrongULTraffic = new List<string>();
            wrongRBUtilisation = new List<string>();
            wrongUserThroughput = new List<string>();
            wrongCellThroughput = new List<string>();
            wrongAverageUsers = new List<string>();
            wrongActiveCellTime = new List<string>();
            wrongCQI = new List<string>();

            // A list to contain all Ids in database already (in Cell_Mapping table - This can be changed) 
            allIds = new List<string>();

            // A list of all values that may cause issues with data upload but aren't BIG issues, these can be replaced with zeros etc.
            acceptedValues = new List<string>() { @"\N", "" };

            // A list of all names of KPIs for general use in output/logs
            KPIs = new List<string>() { "DL Traffic", "UL Traffic", "RB Utilisation", "User Throughput", "Cell Throughput", "Average User", "Active Cell Time", "CQI" };

            // Assign connection string
            var connectionString = "Server=twuxed5ffr.database.windows.net;Database=AETIS08;User Id=AuctionDB;Password=N6sSdRuN;";

            // Connect to database and add all Cell_Ids in cell_mapping table to list
            var idsInDb = new List<string>();
            using (var Connection = new SqlConnection(connectionString))
            {
                Connection.Open();
                using (SqlCommand GetCellIds = Connection.CreateCommand())
                {
                    GetCellIds.CommandText =
                        @"SELECT Cell_ID FROM Cell_Mapping";
                    var result = GetCellIds.ExecuteReader();
                    while (result.Read())
                    {
                        idsInDb.Add(result[0].ToString().Trim());
                    }
                }
            }

            // Loop through each file in directory given above
            foreach (var f in di.EnumerateFiles())
            {
                // Call tracer to log writelines to txt file as well as console
                functions.InitiateTracer(f);

                // Set counters and sums to 0 as well ass initialising empty lists to store each counter, sum and error in (used for iterating through in checks at end)
                int counter = 0;
                int wrongDL_counter = 0;
                int wrongUL_counter = 0;
                int wrongRB_counter = 0;
                int wrongUserThroughput_counter = 0;
                int wrongCellThroughput_counter = 0;
                int wrongAverageUsers_counter = 0;
                int wrongActiveCellTime_counter = 0;
                int wrongCQI_counter = 0;
                var CountersList = new List<int>();

                double Temp_DL_Sum = 0;
                double Temp_UL_Sum = 0;
                double Temp_RB_Sum = 0;
                double Temp_UserThroughput_Sum = 0;
                double Temp_CellThroughput_Sum = 0;
                double Temp_AverageUsers_Sum = 0;
                double Temp_ActiveCellTime_Sum = 0;
                double Temp_CQI_Sum = 0;
                var SumsList = new List<double>();

                var ErrorsList = new List<List<string>>();

                // Set a start time for output at end for analysis in logs and clear the temp datastore
                var StartTime = DateTime.Now.ToString();
                dataStore_Temp.Clear();
                Trace.WriteLine("Current File: " + f.FullName);
                Trace.WriteLine("Start of analysis: " + DateTime.Now.ToString()+ "\n");

                // Format date and get parts needed for manipulating database table name we want to connect to etc. (Output it into log)
                var DateParts = f.FullName.Split("_");
                var DatePartDay = DateParts[1].Substring(0, 2);
                var DatePartYear = DateParts[1].Substring(5, 4);
                var CorrectDate = (DatePartYear + "-02-" + DatePartDay);
                var TechPart = f.Name.Substring(0, 2);
                Trace.WriteLine("Date (Check for validity of code): " + CorrectDate+ "\n");

                string DestinationTable = "Traffic_v3_"+DatePartYear;

                // Clear table of any data before running
                using (var Connection = new SqlConnection(connectionString))
                {
                    Connection.Open();
                    using (SqlCommand ClearThisFile = Connection.CreateCommand())
                    {
                        ClearThisFile.CommandTimeout = 0;
                        ClearThisFile.CommandText =
                            $"DELETE FROM {DestinationTable} WHERE date = '{CorrectDate}' AND Tech = '{TechPart}'";
                        var result = ClearThisFile.ExecuteReader();
                    }
                }

                // Begin reading csv
                using (var rdr = new StreamReader(f.FullName))
                {
                    // Check if VoLTE traffic is included and adjust rows if needed
                    var headerRow = rdr.ReadLine();
                    var headerCells = headerRow.Split(',');
                    var VolTEHeading = headerCells[3];
                    if (VolTEHeading.Substring(0, 2) == "\"V")
                    {
                        VolTEHeading = VolTEHeading.Remove(0, 1);
                        VolTEHeading = VolTEHeading.Remove(VolTEHeading.Length - 1, 1);
                    }
                    var columnAdjustment = 0;
                    if (VolTEHeading == "VoLTE Traffic(Erlang)")
                    {
                        columnAdjustment = 1;
                    }

                    while (!rdr.EndOfStream) //&& counter < 5000)
                    {
                        // Output rows and time parsed every 100000 rows to show progress
                        counter++;
                        if (counter % 200000 == 0)
                        {
                            Trace.WriteLine(counter + " rows parsed. Current time: " + DateTime.Now.ToString());
                        }

                        // Assign row to be one line, then individual cells of that row (comma separate each cell)
                        var row = rdr.ReadLine();
                        var cells = row.Split(',');

                        // Create new data class which will represent and store a row of data
                        var output = new Data();

                        // Format and test Cell_ID. Add to output
                        var Cell_ID_Temp = functions.FormatCellId(cells, ref idsInDb, ref missingIds, ref allIds);
                        output.Cell_ID = Cell_ID_Temp;

                        // Format and test Date. Add to output.
                        var Date_Temp = functions.FormatDate(cells, CorrectDate, ref wrongDates);
                        output.Date = DateTime.Parse(Date_Temp);

                        //Format and test Time. Add to output.
                        var Time_Temp = functions.FormatTime(cells, correctTimes, ref wrongTimes);
                        output.Time = Int32.Parse(Time_Temp);

                        // Format and test DL Traffic. Add to output.
                        var DLTraffic_Temp = functions.FormatKpi(cells, (3 + columnAdjustment), acceptedValues, ref wrongDLTraffic, ref wrongDL_counter);
                        output.Data_DL_MB = Math.Round(double.Parse(DLTraffic_Temp), 5);
                        Temp_DL_Sum += output.Data_DL_MB;

                        // Format and test UL Traffic. Add to output.
                        var ULTraffic_Temp = functions.FormatKpi(cells, (4 + columnAdjustment), acceptedValues, ref wrongULTraffic, ref wrongUL_counter);
                        output.Data_UL_MB = Math.Round(double.Parse(ULTraffic_Temp), 5);
                        Temp_UL_Sum += output.Data_UL_MB;

                        // Format and test RB Utilisation. Add to output.
                        var RB_Temp = functions.FormatKpi(cells, (5 + columnAdjustment), acceptedValues, ref wrongRBUtilisation, ref wrongRB_counter);
                        output.RB_Utilisation = Math.Round(double.Parse(RB_Temp), 5);
                        Temp_RB_Sum += output.RB_Utilisation;

                        // Format and test User Throughput. Add to output.
                        var UserThroughput_Temp = functions.FormatKpi(cells, (6 + columnAdjustment), acceptedValues, ref wrongUserThroughput, ref wrongUserThroughput_counter);
                        output.UserThroughputMbps = Math.Round(double.Parse(UserThroughput_Temp), 5);
                        Temp_UserThroughput_Sum += output.UserThroughputMbps;

                        // Format and test Cell Throughput. Add to output.
                        var CellThroughput_Temp = functions.FormatKpi(cells, (7 + columnAdjustment), acceptedValues, ref wrongCellThroughput, ref wrongCellThroughput_counter);
                        output.CellThroughputMbps = Math.Round(double.Parse(CellThroughput_Temp), 5);
                        Temp_CellThroughput_Sum += output.CellThroughputMbps;

                        // Format and test Average Users. Add to output.
                        var AverageUsers_Temp = functions.FormatKpi(cells, (8 + columnAdjustment), acceptedValues, ref wrongAverageUsers, ref wrongAverageUsers_counter);
                        output.AverageUsers = Math.Round(double.Parse(AverageUsers_Temp), 5);
                        Temp_AverageUsers_Sum += output.AverageUsers;

                        // Format and test Active Cell Time. Add to output.
                        var ActiveCellTime_Temp = functions.FormatKpi(cells, (9 + columnAdjustment), acceptedValues, ref wrongActiveCellTime, ref wrongActiveCellTime_counter);
                        output.ActiveCellTime = Math.Round(double.Parse(ActiveCellTime_Temp), 5);
                        Temp_ActiveCellTime_Sum += output.ActiveCellTime;

                        // Format and test CQI. Add to output.
                        var CQI_Temp = functions.FormatKpi(cells, (10 + columnAdjustment), acceptedValues, ref wrongCQI, ref wrongCQI_counter);
                        output.CQI = Math.Round(double.Parse(CQI_Temp), 5);
                        Temp_CQI_Sum += output.CQI;

                        //Assign whether this piece of data is 5G or 4G
                        output.Tech = TechPart;

                        // Add this row to the data store
                        if (DatePartYear == "2020")
                        {
                            dataStore_2020.Add(output);
                        }
                        else
                        {
                            dataStore_2021.Add(output);
                        }
                        dataStore_Temp.Add(output);
                    }
                }
                double missrate = Math.Round((double)missingIds.Count / allIds.Count, 3) * 100;
                Trace.WriteLine("\nUnique IDs in data set: " + allIds.Count);
                Trace.WriteLine("Missing Ids: " + missingIds.Count + ". Miss rate: " + missrate + "%");
                Trace.WriteLine("Wrong Dates: " + wrongDates.Count);
                Trace.WriteLine("Wrong Times: " + wrongTimes.Count);

                // Build error list
                ErrorsList = functions.BuildErrorList(wrongDLTraffic, wrongULTraffic, wrongRBUtilisation, wrongUserThroughput, wrongCellThroughput, wrongAverageUsers, wrongActiveCellTime, wrongCQI);

                var x = 0;
                foreach (var c in CountersList)
                {
                    if (c != 0)
                    {
                        Trace.WriteLine($"Wrong {KPIs[x]} data: " + c + " wrong entries, " + ErrorsList[x].Count + " uniquely wrong values.");
                        foreach (var e in ErrorsList[x])
                        {
                            Trace.WriteLine(e);
                        }
                    }
                    else
                    {
                        Trace.WriteLine($"{KPIs[x]} data OK!");
                    }
                    x++;
                }

                // Build Sums list
                SumsList = functions.BuildSumsList(Temp_DL_Sum, Temp_UL_Sum, Temp_RB_Sum, Temp_UserThroughput_Sum, Temp_CellThroughput_Sum, Temp_AverageUsers_Sum, Temp_ActiveCellTime_Sum, Temp_CQI_Sum);

                // Bulk copy data from file to SQL
                using (var bcp = new SqlBulkCopy("Server=twuxed5ffr.database.windows.net;Database=AETIS08;User Id=AuctionDB;Password=N6sSdRuN;"))
                {
                    using (var reader = ObjectReader.Create(dataStore_Temp, new string[] { "Cell_ID", "Date", "Time", "Data_UL_MB", "Data_DL_MB", "RB_Utilisation", "UserThroughputMbps", "CellThroughputMbps", "AverageUsers", "ActiveCellTime", "CQI", "Tech" }))
                    {
                        Trace.WriteLine("\nSQL Upload started: " + DateTime.Now.ToString());
                        bcp.DestinationTableName = DestinationTable;
                        bcp.BatchSize = 40000;
                        bcp.BulkCopyTimeout = 0;
                        bcp.WriteToServer(reader);
                        Trace.WriteLine("SQL Upload finished: " + DateTime.Now.ToString()+ "\n");
                    }

                }

                // Compare data in database with that in compiler to ensure all data was transferred to DB 
                using (var Connection = new SqlConnection(connectionString))
                {
                    Connection.Open();
                    using (SqlCommand CheckUploadData = Connection.CreateCommand())
                    {
                        CheckUploadData.CommandText =
                            $"SELECT sum(Data_DL_MB), sum(Data_UL_MB), sum(RB_Utilisation), sum(UserThroughputMbps), sum(CellThroughputMbps), sum(AverageUsers), sum(ActiveCellTime), sum(CQI)  FROM {DestinationTable} WHERE date = '{CorrectDate}' AND Tech = '{TechPart}' ";
                        var result = CheckUploadData.ExecuteReader();
                        var resultslist = new List<double>();
                        while (result.Read())
                        {
                            resultslist = functions.BuildResultsList(double.Parse(result[0].ToString()), double.Parse(result[1].ToString()), double.Parse(result[2].ToString()), double.Parse(result[3].ToString()), double.Parse(result[4].ToString()), double.Parse(result[5].ToString()), double.Parse(result[6].ToString()), double.Parse(result[7].ToString()));
                        }
                        var i = 0;
                        foreach (var r in resultslist)
                        {
                            if (Math.Round(double.Parse(r.ToString()), 5) - Math.Round(SumsList[i], 5) == 0)
                            {
                                Trace.WriteLine($"All {KPIs[i]} data has been successfully transferred to database. Traffic Volume: " + Math.Round(SumsList[i], 5));
                            }
                            else
                            {
                                Trace.WriteLine($"{KPIs[i]}: Inconsistency between database and compiler. Compiler: " + Math.Round(SumsList[i], 5) + ". Database: " + (Math.Round(double.Parse(r.ToString()), 5)));
                            }
                            i++;
                        }
                    }
                }

                // Output start time and end time so user can determine how long the entire run for this file took
                Trace.WriteLine("\nStart Time: " + StartTime);
                Trace.WriteLine("End Time: " + DateTime.Now.ToString());
            }

            Trace.WriteLine("\nDone");
            Trace.WriteLine("\n\n");
            Console.ReadLine();
        }
    }
}





