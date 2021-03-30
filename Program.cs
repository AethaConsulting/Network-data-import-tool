using System;
using System.Data;
using System.Collections.Generic;
using System.Globalization;
using System.ComponentModel;
using System.IO;
using FastMember;
using Microsoft.Data.SqlClient;

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

        static void Main(string[] args)
        {




            // var di = new DirectoryInfo("C:\\Temp\\AETIS08\\2021 4G and 5G traffic data");
            //var di = new DirectoryInfo("C:\\Temp");
            var di = new DirectoryInfo("C:\\Temp\\AETIS08\\2020 4G traffic data\\TESTING");

            dataStore_Temp = new List<Data>();
            dataStore_2020 = new List<Data>();
            dataStore_2021 = new List<Data>();

            //A list of string that will be populated with any Ids not contained in Cell_Mappings table in db
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


            allIds = new List<string>();

            acceptedValues = new List<string>() { @"\N", };



            //Assign connection string
            var connectionString = "Server=twuxed5ffr.database.windows.net;Database=AETIS08;User Id=AuctionDB;Password=N6sSdRuN;";


            var idsInDb = new List<string>();

            //Connect to database
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
                        //Console.WriteLine(result[0].ToString());
                        idsInDb.Add(result[0].ToString().Trim());
                    }
                }
            }

            foreach (var f in di.EnumerateFiles())
            {
                FileStream ostrm;
                StreamWriter writer;
                TextWriter oldOut = Console.Out;
                try
                {
                    ostrm = new FileStream("C:\\Temp\\AETIS08\\2020 4G traffic data\\TESTING\\Import_" + f.FullName + "_" + DateTime.Now.ToString() + ".txt", FileMode.OpenOrCreate, FileAccess.Write);
                    writer = new StreamWriter(ostrm);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Cannot open Redirect.txt for writing");
                    Console.WriteLine(e.Message);
                    return;
                }
                Console.SetOut(writer);

                int counter = 0;
                int wrongDL_counter = 0;
                int wrongUL_counter = 0;
                int wrongRB_counter = 0;
                int wrongUserThroughput_counter = 0;
                int wrongCellThroughput_counter = 0;
                int wrongAverageUsers_counter = 0;
                int wrongActiveCellTime_counter = 0;
                int wrongCQI_counter = 0;

                double Temp_DL_Sum = 0;
                double Temp_UL_Sum = 0;
                double Temp_RB_Sum = 0;
                double Temp_UserThroughput_Sum = 0;
                double Temp_CellThroughput_Sum = 0;
                double Temp_AverageUsers_Sum = 0;
                double Temp_ActiveCellTime_Sum = 0;
                double Temp_CQI_Sum = 0;

                var StartTime = DateTime.Now.ToString();
                dataStore_Temp.Clear();
                Console.WriteLine("Current File: " + f.FullName);
                Console.WriteLine("Start of analysis: " + DateTime.Now.ToString());
                var DateParts = f.FullName.Split("_");
                var DatePartDay = DateParts[1].Substring(0, 2);
                var DatePartYear = DateParts[1].Substring(5, 4);
                var CorrectDate = (DatePartYear + "-02-" + DatePartDay);
                Console.WriteLine("Date (Check for validity of code): " + CorrectDate);
                using (var Connection = new SqlConnection(connectionString))
                {
                    Connection.Open();
                    using (SqlCommand ClearThisFile = Connection.CreateCommand())
                    {
                        ClearThisFile.CommandTimeout = 0;
                        ClearThisFile.CommandText =
                            $"DELETE FROM Traffic_{DatePartYear} WHERE date = '{CorrectDate}'";
                        var result = ClearThisFile.ExecuteReader();
                    }
                }
                using (var rdr = new StreamReader(f.FullName))
                {
                    // Check if VoLTE traffic is included
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

                    while (!rdr.EndOfStream && counter < 5000)
                    {

                        counter++;
                        if (counter % 1000 == 0)
                        {
                            Console.WriteLine(counter + " rows parsed. Current time: " + DateTime.Now.ToString());
                        }
                        var row = rdr.ReadLine();
                        var cells = row.Split(',');
                        var output = new Data();

                        // Cell_ID
                        var Cell_ID_Temp = cells[0];
                        if (Cell_ID_Temp.Substring(0, 1) == "\"")
                        {
                            Cell_ID_Temp = Cell_ID_Temp.Remove(0, 1);
                            Cell_ID_Temp = Cell_ID_Temp.Remove(Cell_ID_Temp.Length - 1, 1);
                        }

                        string Cell_ID_Temp1;
                        string Cell_ID_Temp2;
                        if (Cell_ID_Temp.Substring(0, 7) == "420-03-")
                        {
                            // Remove this part
                            Cell_ID_Temp1 = Cell_ID_Temp.Remove(0, 7);
                        }
                        else
                        {
                            Cell_ID_Temp1 = Cell_ID_Temp;
                        }

                        if (Cell_ID_Temp1.Substring(Cell_ID_Temp1.Length - 5, 5) == ".0000")
                        {
                            // Remove this part
                            var StartPosition = Cell_ID_Temp1.Length - 5;
                            Cell_ID_Temp2 = Cell_ID_Temp1.Remove(StartPosition, 5);
                        }
                        else
                        {
                            Cell_ID_Temp2 = Cell_ID_Temp1;
                        }
                        output.Cell_ID = Cell_ID_Temp2;

                        //Test 1: Check each cellID is in table Cell_mapping
                        if (!idsInDb.Contains(output.Cell_ID) && !missingIds.Contains(output.Cell_ID))
                        {
                            missingIds.Add(output.Cell_ID);
                        }
                        if (!allIds.Contains(output.Cell_ID))
                        {
                            allIds.Add(output.Cell_ID);
                        }

                        // Date
                        var Date_Temp = cells[1];
                        if (Date_Temp.Substring(0, 1) == "\"")
                        {
                            Date_Temp = Date_Temp.Remove(0, 1);
                            Date_Temp = Date_Temp.Remove(Date_Temp.Length - 1, 1);
                        }

                        //Test 2: 1. Each date should be the same
                        if (Date_Temp != CorrectDate)
                        {
                            wrongDates.Add(Date_Temp);
                        }
                        output.Date = DateTime.Parse(Date_Temp);

                        // Time
                        var Time_Temp = cells[2];
                        if (Time_Temp.Substring(0, 1) == "\"")
                        {
                            Time_Temp = Time_Temp.Remove(0, 1);
                            Time_Temp = Time_Temp.Remove(Time_Temp.Length - 1, 1);
                        }
                        if (!correctTimes.Contains(Time_Temp))
                        {
                            wrongTimes.Add(Time_Temp);
                        }
                        output.Time = Int32.Parse(Time_Temp);

                        // DL traffic

                        var DLTraffic_Temp = cells[3 + columnAdjustment];
                        if (DLTraffic_Temp.Substring(0, 1) == "\"")
                        {
                            DLTraffic_Temp = DLTraffic_Temp.Remove(0, 1);
                            DLTraffic_Temp = DLTraffic_Temp.Remove(DLTraffic_Temp.Length - 1, 1);
                        }
                        if (acceptedValues.Contains(DLTraffic_Temp))
                        {
                            DLTraffic_Temp = "0";
                        }
                        double DLdouble = 0;
                        if (!double.TryParse(DLTraffic_Temp, out DLdouble))
                        {
                            wrongDL_counter++;
                            if (!wrongDLTraffic.Contains(DLTraffic_Temp))
                            {
                                wrongDLTraffic.Add(DLTraffic_Temp);
                            }
                        }
                        if (double.TryParse(DLTraffic_Temp, out DLdouble))
                        {
                            if (DLdouble < 0)
                            {
                                wrongDL_counter++;
                                if (!wrongDLTraffic.Contains(DLTraffic_Temp))
                                {
                                    wrongDLTraffic.Add(DLTraffic_Temp);
                                }
                            }
                        }
                        output.Data_DL_MB = Math.Round(double.Parse(DLTraffic_Temp), 5);
                        Temp_DL_Sum += output.Data_DL_MB;

                        // UL traffic
                        var ULTraffic_Temp = cells[4 + columnAdjustment];
                        if (ULTraffic_Temp.Substring(0, 1) == "\"")
                        {
                            ULTraffic_Temp = ULTraffic_Temp.Remove(0, 1);
                            ULTraffic_Temp = ULTraffic_Temp.Remove(ULTraffic_Temp.Length - 1, 1);
                        }
                        if (acceptedValues.Contains(ULTraffic_Temp))
                        {
                            ULTraffic_Temp = "0";
                        }
                        double ULdouble = 0;
                        if (!double.TryParse(ULTraffic_Temp, out ULdouble))
                        {
                            wrongUL_counter++;
                            if (!wrongULTraffic.Contains(ULTraffic_Temp))
                            {
                                wrongULTraffic.Add(ULTraffic_Temp);
                            }
                        }
                        if (double.TryParse(ULTraffic_Temp, out ULdouble))
                        {
                            if (ULdouble < 0)
                            {
                                wrongUL_counter++;
                                if (!wrongULTraffic.Contains(ULTraffic_Temp))
                                {
                                    wrongULTraffic.Add(ULTraffic_Temp);
                                }
                            }
                        }
                        output.Data_UL_MB = Math.Round(double.Parse(ULTraffic_Temp), 5);
                        Temp_UL_Sum += output.Data_UL_MB;

                        // RB utilisation
                        var RB_Temp = cells[5 + columnAdjustment];
                        if (RB_Temp.Substring(0, 1) == "\"")
                        {
                            RB_Temp = RB_Temp.Remove(0, 1);
                            RB_Temp = RB_Temp.Remove(RB_Temp.Length - 1, 1);
                        }
                        if (acceptedValues.Contains(RB_Temp))
                        {
                            RB_Temp = "0";
                        }
                        double RBUtildouble = 0;
                        if (!double.TryParse(RB_Temp, out RBUtildouble))
                        {
                            wrongRB_counter++;
                            if (!wrongRBUtilisation.Contains(RB_Temp))
                            {
                                wrongRBUtilisation.Add(RB_Temp);
                            }
                        }
                        if (double.TryParse(RB_Temp, out RBUtildouble))
                        {
                            if (RBUtildouble < 0)
                            {
                                wrongRB_counter++;
                                if (!wrongRBUtilisation.Contains(RB_Temp))
                                {
                                    wrongRBUtilisation.Add(RB_Temp);
                                }
                            }
                        }
                        output.RB_Utilisation = Math.Round(double.Parse(RB_Temp), 5);
                        Temp_RB_Sum = output.RB_Utilisation;

                        // User throughput
                        var UserThroughput_Temp = cells[6 + columnAdjustment];
                        if (UserThroughput_Temp.Substring(0, 1) == "\"")
                        {
                            UserThroughput_Temp = UserThroughput_Temp.Remove(0, 1);
                            UserThroughput_Temp = UserThroughput_Temp.Remove(UserThroughput_Temp.Length - 1, 1);
                        }
                        if (acceptedValues.Contains(UserThroughput_Temp))
                        {
                            UserThroughput_Temp = "0";
                        }
                        double UserThroughput = 0;
                        if (!double.TryParse(UserThroughput_Temp, out UserThroughput))
                        {
                            wrongUserThroughput_counter++;
                            if (!wrongUserThroughput.Contains(UserThroughput_Temp))
                            {
                                wrongUserThroughput.Add(UserThroughput_Temp);
                            }
                        }
                        if (double.TryParse(UserThroughput_Temp, out UserThroughput))
                        {
                            if (UserThroughput < 0)
                            {
                                wrongUserThroughput_counter++;
                                if (!wrongUserThroughput.Contains(UserThroughput_Temp))
                                {
                                    wrongUserThroughput.Add(UserThroughput_Temp);
                                }
                            }
                        }
                        output.UserThroughputMbps = Math.Round(double.Parse(UserThroughput_Temp), 5);
                        Temp_UserThroughput_Sum = output.UserThroughputMbps;

                        // Cell throughput
                        var CellThroughput_Temp = cells[7 + columnAdjustment];
                        if (CellThroughput_Temp.Substring(0, 1) == "\"")
                        {
                            CellThroughput_Temp = CellThroughput_Temp.Remove(0, 1);
                            CellThroughput_Temp = CellThroughput_Temp.Remove(CellThroughput_Temp.Length - 1, 1);
                        }
                        if (acceptedValues.Contains(CellThroughput_Temp))
                        {
                            CellThroughput_Temp = "0";
                        }
                        double CellThroughput = 0;
                        if (!double.TryParse(CellThroughput_Temp, out CellThroughput))
                        {
                            wrongCellThroughput_counter++;
                            if (!wrongCellThroughput.Contains(CellThroughput_Temp))
                            {
                                wrongCellThroughput.Add(CellThroughput_Temp);
                            }
                        }
                        if (double.TryParse(CellThroughput_Temp, out CellThroughput))
                        {
                            if (CellThroughput < 0 && !wrongCellThroughput.Contains(CellThroughput_Temp))
                            {
                                wrongCellThroughput_counter++;
                                if (!wrongCellThroughput.Contains(CellThroughput_Temp))
                                {
                                    wrongCellThroughput.Add(CellThroughput_Temp);
                                }
                            }
                        }
                        output.CellThroughputMbps = Math.Round(double.Parse(CellThroughput_Temp), 5);
                        Temp_CellThroughput_Sum = output.CellThroughputMbps;

                        // Average users
                        var AverageUsers_Temp = cells[8 + columnAdjustment];
                        if (AverageUsers_Temp.Substring(0, 1) == "\"")
                        {
                            AverageUsers_Temp = AverageUsers_Temp.Remove(0, 1);
                            AverageUsers_Temp = AverageUsers_Temp.Remove(AverageUsers_Temp.Length - 1, 1);
                        }
                        if (acceptedValues.Contains(AverageUsers_Temp))
                        {
                            AverageUsers_Temp = "0";
                        }
                        double AvgUsersDouble = 0;
                        if (!double.TryParse(AverageUsers_Temp, out AvgUsersDouble))
                        {
                            wrongAverageUsers_counter++;
                            if (!wrongAverageUsers.Contains(AverageUsers_Temp))
                            {
                                wrongAverageUsers.Add(AverageUsers_Temp);
                            }
                        }
                        if (double.TryParse(AverageUsers_Temp, out AvgUsersDouble))
                        {
                            if (AvgUsersDouble < 0 && !wrongAverageUsers.Contains(AverageUsers_Temp))
                            {
                                wrongAverageUsers_counter++;
                                if (!wrongAverageUsers.Contains(AverageUsers_Temp))
                                {
                                    wrongAverageUsers.Add(AverageUsers_Temp);
                                }
                            }
                        }
                        output.AverageUsers = Math.Round(double.Parse(AverageUsers_Temp), 5);
                        Temp_AverageUsers_Sum = output.AverageUsers;

                        // Active cell time
                        var ActiveCellTime_Temp = cells[9 + columnAdjustment];
                        if (ActiveCellTime_Temp.Substring(0, 1) == "\"")
                        {
                            ActiveCellTime_Temp = ActiveCellTime_Temp.Remove(0, 1);
                            ActiveCellTime_Temp = ActiveCellTime_Temp.Remove(ActiveCellTime_Temp.Length - 1, 1);
                        }
                        if (acceptedValues.Contains(ActiveCellTime_Temp))
                        {
                            ActiveCellTime_Temp = "0";
                        }
                        double ActiveCellTimeDouble = 0;
                        if (!double.TryParse(ActiveCellTime_Temp, out ActiveCellTimeDouble))
                        {
                            wrongActiveCellTime_counter++;
                            if (!wrongActiveCellTime.Contains(ActiveCellTime_Temp))
                            {
                                wrongActiveCellTime.Add(ActiveCellTime_Temp);
                            }
                        }
                        if (double.TryParse(ActiveCellTime_Temp, out ActiveCellTimeDouble))
                        {
                            if (ActiveCellTimeDouble < 0 && !wrongActiveCellTime.Contains(ActiveCellTime_Temp))
                            {
                                wrongActiveCellTime_counter++;
                                if (!wrongActiveCellTime.Contains(ActiveCellTime_Temp))
                                {
                                    wrongActiveCellTime.Add(ActiveCellTime_Temp);
                                }
                            }
                        }
                        output.ActiveCellTime = Math.Round(double.Parse(ActiveCellTime_Temp), 5);
                        Temp_ActiveCellTime_Sum = output.ActiveCellTime;

                        // CQI
                        var CQI_Temp = cells[10 + columnAdjustment];
                        if (CQI_Temp.Substring(0, 1) == "\"")
                        {
                            CQI_Temp = CQI_Temp.Remove(0, 1);
                            CQI_Temp = CQI_Temp.Remove(CQI_Temp.Length - 1, 1);
                        }
                        if (acceptedValues.Contains(CQI_Temp))
                        {
                            CQI_Temp = "0";
                        }
                        double CQIDouble = 0;
                        if (!double.TryParse(CQI_Temp, out CQIDouble))
                        {
                            wrongCQI_counter++;
                            if (!wrongCQI.Contains(CQI_Temp))
                            {
                                wrongCQI.Add(CQI_Temp);
                            }
                        }
                        if (double.TryParse(CQI_Temp, out CQIDouble))
                        {
                            if (CQIDouble < 0 && !wrongCQI.Contains(CQI_Temp))
                            {
                                wrongCQI_counter++;
                                if (!wrongCQI.Contains(CQI_Temp))
                                {
                                    wrongCQI.Add(CQI_Temp);
                                }
                            }
                        }
                        output.CQI = Math.Round(double.Parse(CQI_Temp), 5);
                        Temp_CQI_Sum = output.CQI;


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
                Console.WriteLine("Unique IDs in data set: " + allIds.Count);
                Console.WriteLine("Missing Ids: " + missingIds.Count + ". Miss rate: " + missrate + "%");
                Console.WriteLine("Wrong Dates: " + wrongDates.Count);
                Console.WriteLine("Wrong Times: " + wrongTimes.Count);


                if (wrongDL_counter != 0)
                {
                    Console.WriteLine("Wrong DL traffic data: " + wrongDL_counter + " wrong entries, " + wrongDLTraffic.Count + " uniquely wrong values.");
                    foreach (var dl in wrongDLTraffic)
                    {
                        Console.WriteLine(dl);
                    }
                }
                else
                {
                    Console.WriteLine("DL Traffic data OK!");
                }

                if (wrongUL_counter != 0)
                {
                    Console.WriteLine("Wrong UL traffic data: " + wrongUL_counter + " wrong entries, " + wrongULTraffic.Count + " uniquely wrong values.");
                    foreach (var ul in wrongULTraffic)
                    {
                        Console.WriteLine(ul);
                    }
                }
                else
                {
                    Console.WriteLine("UL Traffic data OK!");
                }

                if (wrongRB_counter != 0)
                {
                    Console.WriteLine("Wrong RB Utilisation data: " + wrongRB_counter + " wrong entries, " + wrongRBUtilisation.Count + " uniquely wrong values.");
                    foreach (var rb in wrongRBUtilisation)
                    {
                        Console.WriteLine(rb);
                    }
                }
                else
                {
                    Console.WriteLine("RB Utilisation data OK!");
                }

                if (wrongUserThroughput_counter != 0)
                {
                    Console.WriteLine("Wrong User Throughout data: " + wrongUserThroughput_counter + " wrong entries, " + wrongUserThroughput.Count + " uniquely wrong values.");
                    foreach (var ut in wrongUserThroughput)
                    {
                        Console.WriteLine(ut);
                    }
                }
                else
                {
                    Console.WriteLine("User Throughput data OK!");
                }

                if (wrongCellThroughput_counter != 0)
                {
                    Console.WriteLine("Wrong Cell Throughput data: " + wrongCellThroughput_counter + " wrong entries, " + wrongCellThroughput.Count + " uniquely wrong values.");



                    foreach (var ct in wrongCellThroughput)
                    {
                        Console.WriteLine(ct);
                    }
                }
                else
                {
                    Console.WriteLine("Cell Throughput data OK!");
                }

                if (wrongAverageUsers_counter != 0)
                {
                    Console.WriteLine("Wrong Average Users data: " + wrongAverageUsers_counter + " wrong entries, " + wrongAverageUsers.Count + " uniquely wrong values.");
                    foreach (var au in wrongAverageUsers)
                    {
                        Console.WriteLine(au);
                    }
                }
                else
                {
                    Console.WriteLine("Average Users data OK!");
                }

                if (wrongActiveCellTime_counter != 0)
                {
                    Console.WriteLine("Wrong Active Cell Time data: " + wrongActiveCellTime_counter + " wrong entries, " + wrongActiveCellTime.Count + " uniquely wrong values.");
                    foreach (var act in wrongActiveCellTime)
                    {
                        Console.WriteLine(act);
                    }
                }
                else
                {
                    Console.WriteLine("Active Cell Time data OK!");
                }

                if (wrongCQI_counter != 0)
                {
                    Console.WriteLine("Wrong CQI data: " + wrongCQI_counter + " wrong entries, " + wrongCQI.Count + " uniquely wrong values."); Console.WriteLine("Wrong CQI:");
                    foreach (var cqi in wrongCQI)
                    {
                        Console.WriteLine(cqi);
                    }
                }
                else
                {
                    Console.WriteLine("CQI data OK!");
                }


                //Descriptions



                using (var bcp = new SqlBulkCopy("Server=twuxed5ffr.database.windows.net;Database=AETIS08;User Id=AuctionDB;Password=N6sSdRuN;"))
                {
                    using (var reader = ObjectReader.Create(dataStore_Temp, new string[] { "Cell_ID", "Date", "Time", "Data_UL_MB", "Data_DL_MB", "RB_Utilisation", "UserThroughputMbps", "CellThroughputMbps", "AverageUsers", "ActiveCellTime", "CQI" }))
                    {
                        Console.WriteLine("SQL Upload started: " + DateTime.Now.ToString());
                        bcp.DestinationTableName = "Traffic_" + DatePartYear;
                        bcp.BatchSize = 40000;
                        bcp.BulkCopyTimeout = 0;
                        bcp.WriteToServer(reader);
                        Console.WriteLine("SQL Upload finished: " + DateTime.Now.ToString());
                    }

                }
                using (var Connection = new SqlConnection(connectionString))
                {
                    Connection.Open();
                    using (SqlCommand CheckUploadData = Connection.CreateCommand())
                    {
                        CheckUploadData.CommandText =
                            $"SELECT sum(Data_DL_MB) FROM Traffic_{DatePartYear} WHERE date = '{CorrectDate}'";
                        var result = CheckUploadData.ExecuteReader();
                        while (result.Read())
                        {
                            if (Math.Round(double.Parse(result[0].ToString()), 5) - Math.Round(Temp_DL_Sum, 5) == 0)
                            {
                                Console.WriteLine("All DL Traffic data has been successfully transferred to database");
                            }
                            else
                            {
                                Console.WriteLine("Not all DL Traffic data was transferred to database");
                            }

                        }
                    }
                }



                Console.WriteLine("Start Time: " + StartTime);
                Console.WriteLine("End Time: " + DateTime.Now.ToString());
                Console.SetOut(oldOut);
                writer.Close();
                ostrm.Close();
            }

            Console.WriteLine("Done");
            Console.ReadLine();
        }
    }

    public class Data
    {
        public string Cell_ID
        {
            get; set;
        }
        public DateTime Date
        {
            get; set;
        }
        public int Time
        {
            get; set;
        }
        public double Data_DL_MB
        {
            get; set;
        }
        public double Data_UL_MB
        {
            get; set;
        }
        public double RB_Utilisation
        {
            get; set;
        }
        public double UserThroughputMbps
        {
            get; set;
        }
        public double CellThroughputMbps
        {
            get; set;
        }
        public double AverageUsers
        {
            get; set;
        }
        public double ActiveCellTime
        {
            get; set;
        }
        public double CQI
        {
            get; set;
        }
    }
}




