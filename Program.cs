using System;
using System.Data;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Configuration;
using System.Xml;

namespace OfferTable
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var LinkToXMLPage = $"http://partner.market.yandex.ru/pages/help/YML.xml";
            var nodeNameToCollect = "offer";
            var procedureName = "sp_AddRecord";
            var dbTableName = nodeNameToCollect + "s";
            var dbName = "TDB";
            var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["TestDb"].ConnectionString);
            conn.Open();

            Stopwatch Timer = new Stopwatch();
            Timer.Start();

            var xmlPage = GetPageFromSite(LinkToXMLPage);
            XmlDocument doc = new XmlDocument();
            doc.LoadXml(xmlPage);

            var ofrs = doc.SelectNodes(".//offer");
            var attList = new List<string>();
            var attValList = new List<string>();
            foreach (dynamic of in ofrs)
            {
                foreach (var at in of.Attributes)
                    attList.Add(at.Name);
                foreach (var prop in of)
                    attList.Add(prop.Name);
            }
            var attListDist = attList.Distinct().ToList();


            CreateTable(conn, dbName, dbTableName, attListDist);
            CreateProcedure(conn, dbName, procedureName);

            var command = conn.CreateCommand();

            int totRec = 0;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = procedureName;
            Task task=null;

            foreach (dynamic of in ofrs)
            {
                attList.Clear();
                attValList.Clear();
                foreach (var at in of.Attributes)
                {
                    if (attList.Contains(at.Name))
                    {
                        var i = attList.IndexOf(at.Name);
                        attValList[i] += $"; {at.InnerText.Replace("'", "''")}";
                        continue;
                    }
                    attList.Add(at.Name);
                    attValList.Add(at.InnerText.Replace("'", "''"));
                }
                foreach (var prop in of)
                {
                    if (attList.Contains(prop.Name))
                    {
                        var i = attList.IndexOf(prop.Name);
                        attValList[i] += $"; {prop.InnerText.Replace("'", "''")}";
                        continue;
                    }
                    attList.Add(prop.Name);
                    attValList.Add(prop.InnerText.Replace("'", "''"));
                }
                var fieldString = "";
                var valueString = "";
                for (var i = 0; i < attList.Count; i++)
                {
                    fieldString += $"[{attList[i]}],";
                    valueString += $"'{attValList[i]}',";
                }
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@table", dbTableName);
                command.Parameters.AddWithValue("@fields", fieldString.Substring(0, fieldString.Length - 1));
                command.Parameters.AddWithValue("@values", valueString.Substring(0, valueString.Length - 1));

                if (task != null) { await task; }

                task = command.ExecuteNonQueryAsync();
                totRec++;
            }
            

            Timer.Stop();
            Console.WriteLine($"Elapsed time: {Timer.ElapsedMilliseconds:### ### ##0}ms ({Timer.Elapsed:hh\\:mm\\:ss\\.ffff}), Total recurds={totRec}");
        }

        static bool CreateTable(SqlConnection con, string db, string table, List<string> colNames)
        {
            var com = con.CreateCommand();
            com.CommandText = $"IF object_id('[{db}].[dbo].[{table}]') IS NOT NULL DROP TABLE [{db}].[dbo].[{table}]";
            try { com.ExecuteNonQuery(); }
            catch { return false; }

            var TableCol = "";
            foreach (var cn in colNames)
                TableCol += $"[{cn}] [nvarchar](2000) NULL, ";

            com.CommandText = $"CREATE TABLE [{db}].[dbo].[{table}] ({TableCol})";
            com.ExecuteNonQuery();
            return true;
        }

        static bool CreateProcedure(SqlConnection con, string db, string procName)
        {
            var com = con.CreateCommand();
            com.CommandText = $"IF object_id('[dbo].[{procName}]') IS NOT NULL DROP PROCEDURE [dbo].[" + procName + @"]";
            try { com.ExecuteNonQuery(); }
            catch { return false; }
            com.CommandText = 
@"CREATE PROCEDURE [dbo].["+ procName + @"]
@table nvarchar(MAX),
@fields nvarchar(MAX),
@values nvarchar(MAX)
AS
Declare @com nvarchar(MAX)
set @com = 'INSERT INTO ' + @table + ' (' + @fields + ') VALUES (' + @values + ')'
exec(@com)";

            try { com.ExecuteNonQuery(); }
            catch { return false; }

            return true;
        }

        static string GetPageFromSite(string href)
        {
            int attempts = 0;
            string page = "";
            while (true)
            {
                page = "";
                var request = (HttpWebRequest)WebRequest.Create(href);
                request.Timeout = 10000;
                request.ReadWriteTimeout = 10000;
                request.ContinueTimeout = 10000;
                request.Method = "GET";

                request.AutomaticDecompression = DecompressionMethods.GZip;
                try
                {
                    var response = request.GetResponse();
                    var reader = new System.IO.StreamReader(response.GetResponseStream(), Portable.Text.Encoding.GetEncoding(1251));
                    page = reader.ReadToEnd();
                    reader.Close();
                }
                catch (Exception)
                {
                    if (attempts >= 3)
                        throw new TimeoutException();
                    Task.Delay(2000);
                    attempts++;
                }
                break;
            }
            return page;
        }
    }
}