CREATE DATABASE thesis

USE thesis

CREATE SCHEMA bet

CREATE TABLE bet.OddFiles(
	fileId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
	filePath NVARCHAR(256) NOT NULL,
	inputTime DATETIME NOT NULL,
	isChecked CHAR(1) NOT NULL
);

CREATE TABLE bet.ResultFiles(
	fileId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
	filePath NVARCHAR(256) NOT NULL,
	inputTime DATETIME NOT NULL,
	isChecked CHAR(1) NOT NULL
);

CREATE TABLE bet.StandingsFiles(
	fileId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
	filePath NVARCHAR(256) NOT NULL,
	inputTime DATETIME NOT NULL,
	isChecked CHAR(1) NOT NULL
);

CREATE TABLE bet.source(
	sourceId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
	sourceName NVARCHAR(256) UNIQUE NOT NULL
);

CREATE TABLE bet.bookie(
	bookieId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
	bookie NVARCHAR(256) UNIQUE NOT NULL,
	sourceId UNIQUEIDENTIFIER NOT NULL REFERENCES bet.source(sourceId)
);

CREATE TABLE bet.bookieSource(
	dataSource UNIQUEIDENTIFIER NOT NULL REFERENCES bet.source(sourceId),
	bookie UNIQUEIDENTIFIER NOT NULL REFERENCES bet.bookie(bookieId),
	isEnabled BIT NOT NULL,
	UNIQUE (dataSource,bookie)
);

CREATE TABLE bet.team(
	teamId  UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
	team NVARCHAR(256) UNIQUE NOT NULL
);

CREATE TABLE bet.league(
	leagueId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
	leagueName NVARCHAR(256) UNIQUE NOT NULL,
	season NVARCHAR NOT NULL
);

CREATE TABLE bet.teamLeague(
	leagueId UNIQUEIDENTIFIER NOT NULL REFERENCES bet.league(leagueId),
	teamId UNIQUEIDENTIFIER NOT NULL REFERENCES bet.team(teamId)
);

CREATE TABLE bet.fixture(
	fixtureId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),	
	fixtureDate DATE NOT NULL,
	homeTeam UNIQUEIDENTIFIER NOT NULL REFERENCES bet.team(teamId),
	awayTeam UNIQUEIDENTIFIER NOT NULL REFERENCES bet.team(teamId),
	dataSource UNIQUEIDENTIFIER NOT NULL REFERENCES bet.source(sourceId)
);

CREATE TABLE bet.betDirection(
	directionId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),	
	direction CHAR(1) UNIQUE NOT NULL 
);

CREATE TABLE bet.betEntry(
	entryId  UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
	lastUpdated DATE NOT NULL,
	oddValue NUMERIC(4,2),
	direction UNIQUEIDENTIFIER NOT NULL REFERENCES bet.betDirection(directionId),
	fixtureId UNIQUEIDENTIFIER NOT NULL REFERENCES bet.fixture(fixtureId),
	bookieId UNIQUEIDENTIFIER NOT NULL REFERENCES bet.bookie(bookieId),
	sourceId UNIQUEIDENTIFIER NOT NULL REFERENCES bet.source(sourceId)
);

CREATE TABLE bet.Standings(
	standingID  UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
	week INT,
	teamID UNIQUEIDENTIFIER NOT NULL REFERENCES bet.team(teamId),
	position INT,
	won INT,
	draw INT,
	lost INT,
	points INT,
	goalsFor INT,
	goalsAt INT,
	goalDifference INT
);

CREATE TABLE bet.Result(
	resultId  UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
	fixtureId UNIQUEIDENTIFIER NOT NULL REFERENCES bet.fixture(fixtureId),
	homeScore INT NOT NULL,
	awayScore INT NOT NULL,
	directionOfWin CHAR(1) NOT NULL );
    
/*
	Add Historical Results
*/

INSERT INTO bet.source (sourceName)
VALUES ('The Odds Api');

INSERT INTO bet.bookie (bookie,sourceId)
VALUES 
	('unibet',(SELECT sourceId FROM bet.source WHERE sourceName LIKE 'The Odds Api')),
	('onexbet',(SELECT sourceId FROM bet.source WHERE sourceName LIKE 'The Odds Api')),
	('williamhill',(SELECT sourceId FROM bet.source WHERE sourceName LIKE 'The Odds Api')),
	('ladbrokes',(SELECT sourceId FROM bet.source WHERE sourceName LIKE 'The Odds Api')),
	('nordicbet',(SELECT sourceId FROM bet.source WHERE sourceName LIKE 'The Odds Api')),
	('betvictor',(SELECT sourceId FROM bet.source WHERE sourceName LIKE 'The Odds Api')),
	('marathonbet',(SELECT sourceId FROM bet.source WHERE sourceName LIKE 'The Odds Api')),
    ('paddypower',(SELECT sourceId FROM bet.source WHERE sourceName LIKE 'The Odds Api')),
    ('betfair',(SELECT sourceId FROM bet.source WHERE sourceName LIKE 'The Odds Api')),
    ('skybet',(SELECT sourceId FROM bet.source WHERE sourceName LIKE 'The Odds Api')),
    ('betfred',(SELECT sourceId FROM bet.source WHERE sourceName LIKE 'The Odds Api')),
    ('matchbook',(SELECT sourceId FROM bet.source WHERE sourceName LIKE 'The Odds Api'));

INSERT INTO bet.bookieSource(bookie,dataSource,isEnabled)
(SELECT bookieId,ds.sourceId,1
FROM bet.bookie bk JOIN bet.source ds ON bk.sourceId = ds.sourceId);
GO

INSERT INTO bet.betDirection(direction)
VALUES 
(1),(2);
GO

INSERT INTO bet.betDirection(direction)
VALUES ('X');
GO


CREATE PROCEDURE bet.insertOddFile @filepath NVARCHAR(MAX)
AS
INSERT INTO bet.OddFiles(filePath, inputTime,isChecked) 
VALUES (@filePath,SYSDATETIME(), 0);
GO

CREATE PROCEDURE bet.insertResultsFile @filepath NVARCHAR(MAX)
AS
INSERT INTO bet.ResultFiles(filePath,inputTime,isChecked) 
VALUES (@filePath,SYSDATETIME(), 0);
GO

CREATE PROCEDURE bet.insertStandingsFile @filepath NVARCHAR(MAX)
AS
INSERT INTO bet.StandingsFiles(filePath,inputTime,isChecked) 
VALUES (@filePath,SYSDATETIME(), 0);
GO

CREATE PROCEDURE bet.checkFiles
AS
SELECT * FROM bet.OddFiles WHERE isChecked  = 0 
UNION ALL
SELECT * FROM bet.StandingsFiles WHERE isChecked = 0
UNION ALL 
SELECT * FROM bet.ResultFiles WHERE isChecked =0 ORDER BY inputTime
GO
	
CREATE PROCEDURE bet.SearchForFixture  @homeTeam NVARCHAR(MAX), @awayTeam NVARCHAR(MAX), @fixtureDate DATE
AS
SELECT * FROM bet.fixture WHERE homeTeam  IN (SELECT teamId FROM bet.team WHERE team = @homeTeam) 
							AND awayTeam  IN  (SELECT teamId FROM bet.team WHERE team = @awayTeam) 
							AND fixtureDate = @fixtureDate;
GO

CREATE PROCEDURE bet.InsertFixture @fixtureId NVARCHAR(MAX), @fixtureDate DATE, @homeTeam NVARCHAR(MAX), @awayTeam NVARCHAR(MAX), @sourceName NVARCHAR(MAX)
AS
INSERT INTO bet.fixture (fixtureId,fixtureDate,homeTeam,awayTeam,dataSource) 
VALUES (@fixtureId,@fixtureDate,(SELECT teamId FROM bet.team WHERE team =@homeTeam),
                                (SELECT teamId FROM bet.team WHERE team = @awayTeam),
                                (SELECT sourceId FROM bet.source WHERE sourceName = @sourceName));
GO

CREATE PROCEDURE bet.updateOddDocument @filePath NVARCHAR(MAX)
AS
UPDATE bet.OddFiles SET isChecked = 1 WHERE filePath = @filepath;
GO

CREATE PROCEDURE bet.updateStandingsDocument @filePath NVARCHAR(MAX)
AS
UPDATE bet.StandingsFiles SET isChecked = 1 WHERE filePath = @filepath;
GO

CREATE PROCEDURE bet.updateResultsDocument @filePath NVARCHAR(MAX)
AS
UPDATE bet.ResultFiles SET isChecked = 1 WHERE filePath = @filepath;
GO

CREATE PROCEDURE bet.InsertOdd @lastUpdated NVARCHAR(MAX), @oddValue DECIMAL(6,2),@direction CHAR(1), @fixtureId NVARCHAR(MAX), @bookie NVARCHAR(MAX)
AS
INSERT INTO bet.betEntry (lastUpdated,oddValue,direction,fixtureId,bookieId,sourceId) 
VALUES (@lastUpdated,@oddValue,(SELECT directionId FROM bet.betDirection WHERE direction = @direction), @fixtureId,
								(SELECT bookieId FROM bet.bookie WHERE bookie = @bookie),
                                (SELECT sourceId FROM bet.bookie WHERE bookie = @bookie));
GO

CREATE PROCEDURE bet.searchFixtureForResult @homeTeam NVARCHAR(MAX), @awayTeam NVARCHAR(MAX), @date DATE
AS
SELECT fixtureId 
FROM bet.fixture 
WHERE homeTeam  = (SELECT teamId FROM bet.team WHERE team LIKE '%'+@homeTeam+'%' 
					OR @homeTeam  LIKE '%' + team + '%') AND 
					awayTeam =  (SELECT teamId FROM bet.team WHERE team LIKE '%'+@awayTeam+'%' 
					OR @awayTeam LIKE '%'+ team+ '%') AND
					 fixtureDate = @date;
GO

CREATE PROCEDURE bet.InsertResult @matchId NVARCHAR(MAX), @homeScore INT, @awayScore INT, @result CHAR(1)
AS
INSERT INTO bet.Result (fixtureId,homeScore,awayScore,directionOfWin)
VALUES ( (SELECT fixtureId FROM bet.fixture WHERE fixtureId = @matchId),
			@homeScore,
			@awayScore,
			@result
		);
GO

CREATE PROCEDURE bet.insertStanding @position INT, @week INT, @team NVARCHAR(MAX), @won INT, @draw INT, @lost INT, @points INT, @gf INT, @ga INT, @gd INT
AS
INSERT INTO bet.Standings(teamID,position,week,won,draw,lost,points,goalsFor,goalsAt,goalDifference)
VALUES ( 
			(SELECT teamId FROM bet.team WHERE team LIKE '%'+@team+'%' OR @team LIKE '%' + team + '%'),
			@position,
			@week,
			@won,
			@draw,
			@lost,
			@points,
			@gf,
			@ga,
			@gd
		)
GO
	

EXEC bet.insertOddFile 'C:\\Users\\User\\Downloads\\Thesis\\Betting Automation\\Prototype\\Data\\Odds\\odds201810251408.txt'
EXEC bet.insertResultsFile 'C:\\Users\\User\\Downloads\\Thesis\\Betting Automation\\Prototype\\Data\\Results\\Results201810251411.txt'
EXEC bet.insertStandingsFile 'C:\\Users\\User\\Downloads\\Thesis\\Betting Automation\\Prototype\\Data\\Standings\\Standings201810251752.txt'

SELECT * FROM bet.OddFiles
UNION ALL 
SELECT * FROM bet.ResultFiles
UNION ALL
SELECT * FROM bet.StandingsFiles

INSERT INTO bet.league (leagueName,season)
VALUES('EPL','2018/2019')

INSERT INTO bet.team (team)
VALUES ('Southampton'),
		('Newcastle United'),
		('Cardiff City'),
		('Liverpool'),
		('Bournemouth'),
		('Fulham'),
		('Brighton and Hove Albion'),
		('Wolverhampton Wanderers'),
		('Huddersfield Town'),
		('Watford'),
		('Leicester City'),
		('West Ham United'),
		('Arsenal'),
		('Crystal Palace'),
		('Burnley'),
		('Chelsea'),
		('Everton'),
		('Manchester United'),
		('Manchester City'),
		('Tottenham Hotspur')

INSERT INTO bet.teamLeague(teamId,leagueId)
VALUES ((SELECT teamId FROM bet.team WHERE team = 'Southampton'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL')),
		((SELECT teamId FROM bet.team WHERE team ='Newcastle United'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL')),
		((SELECT teamId FROM bet.team WHERE team ='Cardiff City'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL')),
		((SELECT teamId FROM bet.team WHERE team ='Liverpool'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL')),
		((SELECT teamId FROM bet.team WHERE team ='Bournemouth'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL')),
		((SELECT teamId FROM bet.team WHERE team ='Fulham'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL')),
		((SELECT teamId FROM bet.team WHERE team ='Brighton and Hove Albion'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL')),
		((SELECT teamId FROM bet.team WHERE team ='Wolverhampton Wanderers'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL')),
		((SELECT teamId FROM bet.team WHERE team ='Huddersfield Town'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL')),
		((SELECT teamId FROM bet.team WHERE team ='Watford'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL')),
		((SELECT teamId FROM bet.team WHERE team ='Leicester City'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL')),
		((SELECT teamId FROM bet.team WHERE team ='West Ham United'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL')),
		((SELECT teamId FROM bet.team WHERE team ='Arsenal'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL')),
		((SELECT teamId FROM bet.team WHERE team ='Crystal Palace'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL')),
		((SELECT teamId FROM bet.team WHERE team ='Burnley'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL')),
		((SELECT teamId FROM bet.team WHERE team ='Chelsea'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL')),
		((SELECT teamId FROM bet.team WHERE team ='Everton'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL')),
		((SELECT teamId FROM bet.team WHERE team ='Manchester United'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL')),
		((SELECT teamId FROM bet.team WHERE team ='Manchester City'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL')),
		((SELECT teamId FROM bet.team WHERE team ='Tottenham Hotspur'),(SELECT leagueId FROM bet.league WHERE leagueName ='EPL'))


INSERT INTO bet.ResultFiles(filePath,inputTime,isChecked)
VALUES ('C:\\Users\\User\\Downloads\\Thesis\\Betting Automation\\Prototype\\Data\\Results\\Results201811181333.txt',SYSDATETIME(),0)

SELECT * FROM bet.StandingsFiles

SELECT *  FROM bet.Standings ORDER BY 4 ASC