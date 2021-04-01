using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Sanity_Checks
{
    public class Functions
    {

        // A function to get cell ids out of csv and test their validity
        public string FormatCellId(string[] cells, ref List<string> idsInDb, ref List<string> missingIds, ref List<string> allIds)
        {
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

            if (!idsInDb.Contains(Cell_ID_Temp2) && !missingIds.Contains(Cell_ID_Temp2))
            {
                missingIds.Add(Cell_ID_Temp2);
            }
            if (!allIds.Contains(Cell_ID_Temp2))
            {
                allIds.Add(Cell_ID_Temp2);
            }
            return Cell_ID_Temp2;
        }

        // A function to get dates out of csv and test their validity
        public string FormatDate(string[] cells, string CorrectDate, ref List<string> wrongDates)
        {
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
            return Date_Temp;
        }

        // A function to get times out of csv and test their validity
        public string FormatTime(string[] cells, List<string> correctTimes, ref List<string> wrongTimes)
        {
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
            return Time_Temp;
        }

        // A function to get KPIs out of csv and test their validity
        public string FormatKpi(string[] cells, int placement, List<string> acceptedValues, ref List<string> wrongKpis, ref int counter)
        {
            var Kpi_Temp = cells[placement];
            if (Kpi_Temp.Substring(0, 1) == "\"")
            {
                Kpi_Temp = Kpi_Temp.Remove(0, 1);
                Kpi_Temp = Kpi_Temp.Remove(Kpi_Temp.Length - 1, 1);
            }
            if (acceptedValues.Contains(Kpi_Temp))
            {
                Kpi_Temp = "0";
            }
            double DLdouble = 0;
            if (!double.TryParse(Kpi_Temp, out DLdouble))
            {
                counter++;
                if (!Kpi_Temp.Contains(Kpi_Temp))
                {
                    wrongKpis.Add(Kpi_Temp);
                }
            }
            if (double.TryParse(Kpi_Temp, out DLdouble))
            {
                if (DLdouble < 0)
                {
                    counter++;
                    if (!Kpi_Temp.Contains(Kpi_Temp))
                    {
                        wrongKpis.Add(Kpi_Temp);
                    }
                }
            }
            return Kpi_Temp;
        }

        // Three function to build up lists
        public List<List<string>> BuildErrorList(List<string> DL, List<string> UL, List<string> RB, List<string> UsrThrpt, List<string> ClThrpt, List<string> AvgUsrs, List<string> ActvClTm, List<string> Cqi)
        {
            var errorsList = new List<List<string>>();
            errorsList.Add(DL);
            errorsList.Add(UL);
            errorsList.Add(RB);
            errorsList.Add(UsrThrpt);
            errorsList.Add(ClThrpt);
            errorsList.Add(AvgUsrs);
            errorsList.Add(ActvClTm);
            errorsList.Add(Cqi);
            return errorsList;
        }

        public List<double> BuildSumsList(double DL, double UL, double RB, double UsrThrpt, double ClThrpt, double AvgUsrs, double ActvClTm, double Cqi)
        {
            var sumsList = new List<double>();
            sumsList.Add(DL);
            sumsList.Add(UL);
            sumsList.Add(RB);
            sumsList.Add(UsrThrpt);
            sumsList.Add(ClThrpt);
            sumsList.Add(AvgUsrs);
            sumsList.Add(ActvClTm);
            sumsList.Add(Cqi);
            return sumsList;
        }

        public List<double> BuildResultsList(double DL, double UL, double RB, double UsrThrpt, double ClThrpt, double AvgUsrs, double ActvClTm, double Cqi)
        {
            var resultsList = new List<double>();
            resultsList.Add(DL);
            resultsList.Add(UL);
            resultsList.Add(RB);
            resultsList.Add(UsrThrpt);
            resultsList.Add(ClThrpt);
            resultsList.Add(AvgUsrs);
            resultsList.Add(ActvClTm);
            resultsList.Add(Cqi);
            return resultsList;
        }

        // A function to initiate a tracer logging data to console as well as txt file
        public void InitiateTracer(FileInfo f)
        {
            Trace.Listeners.Clear();
            var twtl = new TextWriterTraceListener("C:\\Temp\\Logs\\Import_" + f.Name.Replace(".csv", "") + "_" + DateTime.Now.ToShortDateString().Replace("/", "") + "_" + DateTime.Now.ToShortTimeString().Replace(":", "-") + ".txt")
            {
                Name = "TextLogger",
                TraceOutputOptions = TraceOptions.ThreadId | TraceOptions.DateTime
            };
            var ctl = new ConsoleTraceListener(false) { TraceOutputOptions = TraceOptions.DateTime };
            Trace.Listeners.Add(twtl);
            Trace.Listeners.Add(ctl);
            Trace.AutoFlush = true;
        }
    }

}
