using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace test
{
    class Program
    {
        public static string connString = "";
        static void Main(string[] args)
        {

        }


        public static void extractOdds(DataTable dt)
        {
            foreach (DataRow dr in dt.Rows)
            {
                //Read data from file
                string fileData = File.ReadAllText(dr["filePath"].ToString());
                //Convert String to Json
                JObject jo = JObject.Parse(fileData);
                //Get Matches from Json
                JObject matches = (JObject)jo["data"]["events"];

                foreach(var match in matches)
                {
                    string matchID = "";
                    string query = "";

                    var teams = match.Value["participants"];
                    string team1 = teams[0].ToString();
                    string team2 = teams[1].ToString();
                    string awayTeam = "";

                   
                    //Set teams for query
                    String homeTeam = match.Value["home_team"].ToString();

                    if (homeTeam == team1)
                    {
                        awayTeam = team2;
                    }
                    else
                        awayTeam = team1;

                    //Get date of match
                    long date = Convert.ToInt64(match.Value["commence"].ToString());
                    DateTime matchDate = EpochToDateTime(date);
                    string timeString = matchDate.ToString("yyyy/MM/dd HH:mm");

                    string sourceName = "The Odds Api";

                    //Select Fixture Id Where home team is team 1, away team is team 2 & match date is equal to matchDate

                        List<SqlParameter> SqlParams = new List<SqlParameter>();

                        SqlParams.Add(new SqlParameter("homeTeam", homeTeam));
                        SqlParams.Add(new SqlParameter("awayTeam", awayTeam));
                        SqlParams.Add(new SqlParameter("matchDate", matchDate));

                        DataTable dataTable = new DataTable();

                        dataTable = ExecuteProcedure("SearchForFixture", SqlParams);

                        if (dataTable.Rows[0] != null)
                        {
                            matchID = dataTable.Rows[0]["fixtureId"].ToString();
                        }

                        else
                        {
                            string id = Guid.NewGuid().ToString();
                            SqlParams = new List<SqlParameter>();

                            SqlParams.Add(new SqlParameter("fixtureId", matchID));
                            SqlParams.Add(new SqlParameter("fixtureDate", matchDate));
                            SqlParams.Add(new SqlParameter("homeTeam", homeTeam));
                            SqlParams.Add(new SqlParameter("awayTeam", awayTeam));

                            dataTable = new DataTable();

                            dataTable = ExecuteProcedure("InsertFixture", SqlParams);
                        }

                    JObject bookies = (JObject)jo["data"]["events"][match.Key]["sites"];

                    foreach(var bookie in bookies)
                    {
                        //Get time of last update
                        long update = Convert.ToInt64(bookie.Value["last_update"].ToString());
                        
                        //Change from Epoch to DateTime
                        DateTime lastUpdate = EpochToDateTime(update);
                        
                        //Convert to string
                        string time = lastUpdate.ToString("yyyy/MM/dd HH:mm");

                        //Set bookie name
                        string b = bookie.Key;

                        string direction = "";
                        double val = 0.0;
                        var odds = bookie.Value["odds"]["h2h"];

                        for(int i = 0; i < 2; i++)
                        {
                            foreach(var odd in odds)
                            {
                                switch(i)
                                {
                                    case 0:
                                        direction = "2";
                                        break;
                                    case 1:
                                        direction = "1";
                                        break;
                                    case 2:
                                        direction = "x";
                                        break;
                                }

                                val = Convert.ToDouble(odd);

                                DataTable d = new DataTable();

                                List<SqlParameter> sqlParams = new List<SqlParameter>();

                                sqlParams.Add(new SqlParameter("lastUpdated", lastUpdate));
                                sqlParams.Add(new SqlParameter("oddValue", val));
                                sqlParams.Add(new SqlParameter("direction", direction));
                                sqlParams.Add(new SqlParameter("fixtureId", matchID));
                                sqlParams.Add(new SqlParameter("bookie", b));

                                d = ExecuteProcedure("InsertBet", sqlParams);
                            }
                        }

                    }

                    DataTable d = new DataTable();

                    List<SqlParameter> sqlParameters = new List<SqlParameter>();
                    sqlParameters.Add(new SqlParameter(("filepath"), dr["filePath"]));

                    d = ExecuteProcedure("UpdateFile", sqlParameters);
                }
            }
        }

        public static void extractResults(DataTable dt)
        {

            foreach (DataRow dr in dt.Rows)
            {
                string fileData = File.ReadAllText(dr["filePath"].ToString());
                //Convert String to Json
                JObject jo = JObject.Parse(fileData);

                JArray match = (JArray)jo["matches"];

                foreach (var x in match)
                {
                    var homeTeam = x["homeTeam"]["name"];
                    var awayTeam = x["awayTeam"]["name"];

                    var scoreHome = x["score"]["fullTime"]["homeTeam"];
                    var scoreAway = x["score"]["fullTime"]["awayTeam"];

                    string direction = "";

                    if(Convert.ToInt16(scoreHome) > Convert.ToInt16(scoreAway))
                    {
                        direction = "1";
                    }
                    else if(Convert.ToInt16(scoreHome) < Convert.ToInt16(scoreAway))
                    {
                        direction = "2";
                    }
                    else
                    {
                        direction = "x";
                    }


                    DateTime date = Convert.ToDateTime(x["utcDate"]);

                    //Select Id of match with corresponding date time, home and away team and insert result
                    List<SqlParameter> sqlParameters = new List<SqlParameter>();

                    sqlParameters.Add(new SqlParameter("homeTeam", homeTeam));
                    sqlParameters.Add(new SqlParameter("awayTeam", awayTeam));
                    sqlParameters.Add(new SqlParameter("date", date));

                    DataTable results = new DataTable();

                    results = ExecuteProcedure("SearchForFixture",sqlParameters);

                    if (results.Rows[0] != null)
                    {
                        string matchID = results.Rows[0]["fixtureId"].ToString();

                        results = new DataTable();
                        sqlParameters.Clear();

                        sqlParameters.Add(new SqlParameter("homeScore", scoreHome));
                        sqlParameters.Add(new SqlParameter("awayScore", scoreAway));
                        sqlParameters.Add(new SqlParameter("direction", direction));

                        results = ExecuteProcedure("InsertResult", sqlParameters);
                    }

                }
            }            
        }

        public static void extractStandings(DataTable dt)
        {
            foreach (DataRow dr in dt.Rows)
            {
                string fileData = File.ReadAllText(dr["filePath"].ToString());
                //Convert String to Json
                JObject jo = JObject.Parse(fileData);

                JArray content = (JArray)jo["standings"][0]["table"];

                foreach (var x in content)
                {
                    var position = x["position"];
                    var team = x["team"]["name"];
                    var wins = x["won"];
                    var draw = x["draw"];
                    var lost = x["lost"];
                    var points = x["points"];
                    var gf = x["goalsFor"];
                    var ga = x["goalsAgainst"];
                    var gd = x["goalDifference"];


                    DataTable data = new DataTable();
                    List<SqlParameter> sqlParameters = new List<SqlParameter>();


                    sqlParameters.Add(new SqlParameter("position", position));
                    //Insert into db
                }
            }
        }

        //Convert date from Epoch to DateTime.
        public static DateTime EpochToDateTime(long timestamp)
        {
            // Unix timestamp is seconds past epoch
            System.DateTime time = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            time = time.AddSeconds(timestamp).ToLocalTime();
            return time;
        }

        public static DataTable ExecuteProcedure(string procName, List<SqlParameter> sqlParams)
        {
            DataTable dt = new DataTable();
            SqlConnection conn = new SqlConnection(connString);

            conn.Open();

            SqlCommand command = new SqlCommand(procName, conn);
            command.CommandType = System.Data.CommandType.StoredProcedure;

            if (sqlParams != null)
            {
                foreach (SqlParameter par in sqlParams)
                {
                    command.Parameters.Add(par);
                }
            }
            command.CommandTimeout = 300000;
            dt.Load(command.ExecuteReader());
            conn.Close();
            conn.Dispose();
            return dt;
        }
    }
}

