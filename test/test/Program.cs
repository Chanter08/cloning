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
using System.Timers;

namespace test
{
    class Program
    {
        public static string connString = "Data Source=GEORDEY\\SQLEXPRESS;Initial Catalog=thesis;Integrated Security=True";

        /*Yet to 
         * implement timer
         * Buy api
         * implement method to call all methods accordingly
        */

        static void Main(string[] args)
        {
            CheckDb();
            Timer t = new System.Timers.Timer
            {
                Interval = 1000 * 60 * 60 * 2
            };

            t.Elapsed += new System.Timers.ElapsedEventHandler(Timer_Elapsed);
            t.Start();
        }

        private static void Timer_Elapsed(object sender, ElapsedEventArgs e)
        {
            CheckDb();
        }

        public static void CheckDb()
        {
            DataTable OddFiles = new DataTable();
            DataTable ResultFiles = new DataTable();
            DataTable StandingFiles = new DataTable();
            List<SqlParameter> SqlParams = new List<SqlParameter>();
            DataTable dataTable = new DataTable();

            dataTable = ExecuteProcedure("bet.checkFiles", SqlParams);

            if (dataTable.Rows.Count == 0)
            {
                Console.WriteLine("No New Files Found");
            }
            else
            {
                foreach(DataRow r in dataTable.Rows)
                {

                    if(r["filePath"].ToString().Contains("Odds"))
                    {
                        OddFiles = dataTable.Clone();
                        OddFiles.Rows.Add(r.ItemArray);
                    }
                    else if (r["filePath"].ToString().Contains("Results"))
                    {
                        ResultFiles = dataTable.Clone();
                        ResultFiles.Rows.Add(r.ItemArray);
                    }
                    else if (r["filePath"].ToString().Contains("Standings"))
                    {
                        StandingFiles = dataTable.Clone();
                        StandingFiles.Rows.Add(r.ItemArray);
                    }
                }

                //ExtractOdds(OddFiles);
                //ExtractResults(ResultFiles);
                ExtractStandings(StandingFiles);
            }
        }

        public static void ExtractOdds(DataTable dt)
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

                    List<SqlParameter> SqlParams = new List<SqlParameter>
                    {
                        new SqlParameter("homeTeam", homeTeam),
                        new SqlParameter("awayTeam", awayTeam),
                        new SqlParameter("matchDate", matchDate),
                    };

                    DataTable dataTable = new DataTable();

                        dataTable = ExecuteProcedure("bet.SearchForFixture", SqlParams);

                        if (dataTable.Rows.Count != 0)
                        {
                            matchID = dataTable.Rows[0]["fixtureId"].ToString();
                        }

                        else
                        {
                            string id = Guid.NewGuid().ToString();
                            matchID = id;
                            SqlParams = new List<SqlParameter>
                            {
                                new SqlParameter("fixtureId", id),
                                new SqlParameter("fixtureDate", matchDate),
                                new SqlParameter("homeTeam", homeTeam),
                                new SqlParameter("awayTeam", awayTeam),
                                new SqlParameter("sourceName",sourceName)
                            };

                            dataTable = new DataTable();

                            dataTable = ExecuteProcedure("bet.InsertFixture", SqlParams);
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
                                        direction = "X";
                                        break;
                                }

                                val = Convert.ToDouble(odd);

                                DataTable insertBet = new DataTable();

                                List<SqlParameter> sqlParams = new List<SqlParameter>
                                {
                                    new SqlParameter("lastUpdated", lastUpdate),
                                    new SqlParameter("oddValue", val),
                                    new SqlParameter("direction", direction),
                                    new SqlParameter("fixtureId", matchID),
                                    new SqlParameter("bookie", b)
                                };

                                insertBet = ExecuteProcedure("bet.InsertOdd", sqlParams);
                                i++;
                            }
                                
                        }

                    }

                    DataTable d = new DataTable();

                    List<SqlParameter> sqlParameters = new List<SqlParameter>
                    {
                        new SqlParameter(("filepath"), dr["filePath"])
                    };

                    d = ExecuteProcedure("bet.updateOddDocument", sqlParameters);
                }
            }
            }

        public static void ExtractResults(DataTable dt)
        {

            foreach (DataRow dr in dt.Rows)
            {
                string fileData = File.ReadAllText(dr["filePath"].ToString());
                //Convert String to Json
                JObject jo = JObject.Parse(fileData);

                JArray match = (JArray)jo["matches"];

                foreach (var x in match)
                {
                    string homeTeam = x["homeTeam"]["name"].ToString();
                    string awayTeam = x["awayTeam"]["name"].ToString();

                    var scoreHome = x["score"]["fullTime"]["homeTeam"];
                    var scoreAway = x["score"]["fullTime"]["awayTeam"];

                    char direction;

                    if(Convert.ToInt16(scoreHome) > Convert.ToInt16(scoreAway))
                    {
                        direction = '1';
                    }
                    else if(Convert.ToInt16(scoreHome) < Convert.ToInt16(scoreAway))
                    {
                        direction = '2';
                    }
                    else
                    {
                        direction = 'X';
                    }

                    DateTime date = Convert.ToDateTime(x["utcDate"]);

                    //Select Id of match with corresponding date time, home and away team and insert result
                    List<SqlParameter> sqlParameters = new List<SqlParameter>
                    {
                        new SqlParameter("homeTeam", homeTeam),
                        new SqlParameter("awayTeam", awayTeam),
                        new SqlParameter("date", date)
                    };

                    DataTable results = new DataTable();

                    results = ExecuteProcedure("bet.searchFixtureForResult", sqlParameters);

                    if (results.Rows.Count != 0)
                    {
                        string matchID = results.Rows[0]["fixtureId"].ToString();

                        results = new DataTable();
                        sqlParameters.Clear();
                        sqlParameters.Add(new SqlParameter("matchId", matchID));
                        sqlParameters.Add(new SqlParameter("homeScore", Convert.ToInt16(scoreHome)));
                        sqlParameters.Add(new SqlParameter("awayScore", Convert.ToInt16(scoreAway)));
                        sqlParameters.Add(new SqlParameter("result", direction));

                        results = ExecuteProcedure("bet.InsertResult", sqlParameters);
                    }
                }

                List<SqlParameter> sqlParams = new List<SqlParameter>
                {
                    new SqlParameter("filePath", dr["filePath"])
                };

                ExecuteProcedure("bet.updateResultsDocument", sqlParams);
            }            
        }

        public static void ExtractStandings(DataTable dt)
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
                    var playedGames = x["playedGames"];
                    string team = x["team"]["name"].ToString();
                    var wins = x["won"];
                    var draw = x["draw"];
                    var lost = x["lost"];
                    var points = x["points"];
                    var gf = x["goalsFor"];
                    var ga = x["goalsAgainst"];
                    var gd = x["goalDifference"];

                    if (team.Contains("&"))
                    {
                        team = team.Replace("&", "and");
                    }
                    DataTable data = new DataTable();
                    List<SqlParameter> sqlParameters = new List<SqlParameter>
                    {
                        new SqlParameter("position",Convert.ToInt16(position)),
                        new SqlParameter("week",Convert.ToInt16(playedGames)),
                        new SqlParameter("team",team),
                        new SqlParameter("won",Convert.ToInt16(wins)),
                        new SqlParameter("draw",Convert.ToInt16(draw)),
                        new SqlParameter("lost",Convert.ToInt16(lost)),
                        new SqlParameter("points",Convert.ToInt16(points)),
                        new SqlParameter("gf",Convert.ToInt16(gf)),
                        new SqlParameter("ga",Convert.ToInt16(ga)),
                        new SqlParameter("gd",Convert.ToInt16(gd))
                    };


                    DataTable results = new DataTable();
                    results = ExecuteProcedure("bet.InsertStanding", sqlParameters);
                }

                List<SqlParameter> sqlParams = new List<SqlParameter>
                {
                    new SqlParameter("filePath", dr["filePath"])
                };

                ExecuteProcedure("bet.updateStandingsDocument", sqlParams);
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

            SqlCommand command = new SqlCommand(procName, conn)
            {
                CommandType = System.Data.CommandType.StoredProcedure
            };

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

