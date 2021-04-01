using System;

namespace Sanity_Checks
{
    // A class used to store and represent each row of data in CSV
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
