-- Table: al_player_review
CREATE TABLE al_player_review (id INTEGER PRIMARY KEY AUTOINCREMENT, "year" INTEGER, "league" TEXT, "player" TEXT, "team" TEXT, "pos" TEXT, "g" INTEGER, "ab" INTEGER, "r" INTEGER, "h" INTEGER, "2b" INTEGER, "3b" INTEGER, "hr" INTEGER, "rbi" INTEGER, "bb" INTEGER, "so" INTEGER, "sb" INTEGER, "cs" INTEGER, "avg" REAL, "obp" REAL, "slg" REAL, "w" INTEGER, "l" INTEGER, "era" REAL, "sv" INTEGER, "sho" INTEGER, "ip" REAL, "er" INTEGER, "k" INTEGER, "player_link" TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

-- Table: sqlite_sequence
CREATE TABLE sqlite_sequence(name,seq);

-- Table: al_pitcher_review
CREATE TABLE al_pitcher_review (id INTEGER PRIMARY KEY AUTOINCREMENT, "year" INTEGER, "league" TEXT, "player" TEXT, "team" TEXT, "pos" TEXT, "g" INTEGER, "ab" INTEGER, "r" INTEGER, "h" INTEGER, "2b" INTEGER, "3b" INTEGER, "hr" INTEGER, "rbi" INTEGER, "bb" INTEGER, "so" INTEGER, "sb" INTEGER, "cs" INTEGER, "avg" REAL, "obp" REAL, "slg" REAL, "w" INTEGER, "l" INTEGER, "era" REAL, "sv" INTEGER, "sho" INTEGER, "ip" REAL, "er" INTEGER, "k" INTEGER, "player_link" TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

-- Table: al_team_standings
CREATE TABLE al_team_standings (id INTEGER PRIMARY KEY AUTOINCREMENT, "year" INTEGER, "league" TEXT, "division" TEXT, "team" TEXT, "w" INTEGER, "l" INTEGER, "wp" REAL, "gb" TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

-- Table: nl_player_review
CREATE TABLE nl_player_review (id INTEGER PRIMARY KEY AUTOINCREMENT, "year" INTEGER, "league" TEXT, "player" TEXT, "team" TEXT, "pos" TEXT, "g" INTEGER, "ab" INTEGER, "r" INTEGER, "h" INTEGER, "2b" INTEGER, "3b" INTEGER, "hr" INTEGER, "rbi" INTEGER, "bb" INTEGER, "so" INTEGER, "sb" INTEGER, "cs" INTEGER, "avg" REAL, "obp" REAL, "slg" REAL, "w" INTEGER, "l" INTEGER, "era" REAL, "sv" INTEGER, "sho" INTEGER, "ip" REAL, "er" INTEGER, "k" INTEGER, "player_link" TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

-- Table: nl_pitcher_review
CREATE TABLE nl_pitcher_review (id INTEGER PRIMARY KEY AUTOINCREMENT, "year" INTEGER, "league" TEXT, "player" TEXT, "team" TEXT, "pos" TEXT, "g" INTEGER, "ab" INTEGER, "r" INTEGER, "h" INTEGER, "2b" INTEGER, "3b" INTEGER, "hr" INTEGER, "rbi" INTEGER, "bb" INTEGER, "so" INTEGER, "sb" INTEGER, "cs" INTEGER, "avg" REAL, "obp" REAL, "slg" REAL, "w" INTEGER, "l" INTEGER, "era" REAL, "sv" INTEGER, "sho" INTEGER, "ip" REAL, "er" INTEGER, "k" INTEGER, "player_link" TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

-- Table: nl_team_standings
CREATE TABLE nl_team_standings (id INTEGER PRIMARY KEY AUTOINCREMENT, "year" INTEGER, "league" TEXT, "division" TEXT, "team" TEXT, "w" INTEGER, "l" INTEGER, "wp" REAL, "gb" TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

