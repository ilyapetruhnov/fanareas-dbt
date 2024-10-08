import random
from dagster_fanareas.constants import league_mapping
from dagster_fanareas.quizzes.tm_queries import standing_query, player_for_the_team_query, most_titles_won_query, cl_titles_query, cup_titles_query, team_coach_query, team_stats_query, most_conceded_goals_query, most_scored_goals_query, el_titles_query, points_query, unbeaten_query, unbeaten_options_query, team_logo_options_query, logo_select_query, won_league_with_fewest_points_query
from dagster_fanareas.quizzes.quizzes import Quizzes


class TeamQuizzes(Quizzes):
    def __init__(self, title: str, description: str, quiz_type: int, is_demo: bool) -> None:
        super().__init__(title, description, quiz_type, is_demo)
        self.players = []
        self.quiz_collection = []
        self.league_mapping = league_mapping

    def team_logo(self, team_name):
        options_df = self.generate_df(team_logo_options_query)
        all_teams = list(options_df['name'].unique())
        teams = [
            'Wolverhampton Wanderers',
            'Fulham FC',
            'Olympique Marseille',
            'Leeds United',
            'Villarreal CF',
            'Sevilla FC',
            'Atlético de Madrid',
            'Real Sociedad',
            'Inter Milan',
            'Bologna FC 1909',
            'UC Sampdoria',
            'ACF Fiorentina',
            'Juventus FC',
            'AC Milan',
            'VfB Stuttgart',
            'Eintracht Frankfurt',
            'Borussia Dortmund',
            'SV Werder Bremen',
            'Borussia Mönchengladbach',
            'SC Freiburg'
        ]
        option_teams = [i for i in all_teams if i not in teams]
        # team_name = random.choice(teams)
        df = self.generate_df(logo_select_query.format(team_name))
        image_url = df['image'].iloc[0]
        correct_response = team_name
        options = random.sample(option_teams, 3)
        options.append(correct_response)
        q1 = "Which team's logo is this?"
        q2 = "Which club's logo is this?"
        q3 = "Which football team's emblem is this?"
        q4 = "Which football club's logo is this?"
        q5 = "Which team's emblem is this?"
        question_statement = random.choice([q1,q2,q3,q4,q5])
        correct_response = team_name
        question = self.question_template(question_statement = question_statement, 
                                          options = options, 
                                          correct_response = correct_response,
                                          image_url = image_url)
        return question
    
    def club_nickname(self, club_name):
        club_mapping = {
            'Manchester United':'Red Devils',
            'Inter Milan':'Nerazzurri',
            'Everton':'Toffees',
            'AC Milan':'Rossoneri',
            'Arsenal':'Gunners',
            'Newcastle United':'Magpies',
            'Real Madrid':'Los Blancos',
            'Atletico Madrid':'Los Colchoneros',
            'Manchester City': 'Citizens',
            'Stoke City': 'Potters',
            'Tottenham': 'Spurs',
            'Southampton': 'Saints',
            'AFC Bournemouth': 'Cherries'
               }
        option_teams = ['Barcelona',
                   'West Ham',
                   'Chelsea',
                   'Fiorentina',
                   'Aston Villa',
                   'Fulham',
                   'Brentford',
                   'Nottingham Forest',
                   'Sevilla FC',
                   'Valencia',
                   'Liverpool FC',
                   'Athletic Bilbao',
                   'Villarreal CF',
                   'Paris Saint-Germain',
                   'Crystal Palace'
                   ]
        club_nickname = club_mapping[club_name]
        correct_response = club_name
        options = random.sample(option_teams, 3)
        options.append(correct_response)
        question_statement = f""" Which club is known as "{club_nickname}" """
        question = self.question_template(question_statement, options, correct_response)
        return question

    def unbeaten(self, league_id):
        league_name = self.league_mapping[league_id]
        df = self.generate_df(unbeaten_query.format(league_id))
        season_id = df['season_id'].iloc[0]
        wins = df['wins'].iloc[0]
        draws = df['draw'].iloc[0]
        season_name = f"{season_id}/{season_id+1}"
        correct_response = df['club_name'].iloc[0]
        options_df = self.generate_df(unbeaten_options_query.format(league_id, correct_response))
        options = list(options_df.sample(3)['club_name'].unique())
        options.append(correct_response)
        q1 = "Which football club completed an entire {} league season unbeaten?".format(league_name)
        q2 = "Which club achieved an undefeated season in {}, winning the league title without losing a single match?".format(league_name)
        question_statement = random.choice([q1,q2])
        description = f"""{correct_response} completed the {season_name} {league_name} season without a single defeat with {wins} wins and {draws} draws"""
        question = self.question_template(question_statement, options, correct_response, description)
        return question

    def cl_final_2005(self):
        options = ['AC Milan',
                   'Real Madrid',
                   'Liverpool',
                   'Manchester United']
        correct_response = 'Liverpool'
        question_statement = "Which club staged a remarkable comeback in the 2005 UEFA Champions League final, coming from 3-0 down at halftime to win the match?"
        description = "Liverpool won the Champions League final against AC Milan on penalties after a dramatic 3-3 draw"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def first_to_reach_100_points_in_league(self, league_id):
        league_name = self.league_mapping[league_id]
        df = self.generate_df(points_query.format(league_id)).sort_values('points',ascending=False)
        options_df = df.groupby(['club_name'])['points'].max().reset_index().sort_values('points',ascending=False).head(4)
        season_id = df['season_id'].iloc[0]
        season_name = self.get_season_name(season_id)
        if league_id == 'ES':
            correct_response = 'Real Madrid'
        else:
            correct_response = df['club_name'].iloc[0]
        options = [i for i in options_df['club_name']]
        question_statement = "Which club was the first to achieve 100 points in a single {} season?".format(league_name)
        description = f"{correct_response} was the first club to achieve 100 points in a single {league_name} season, accomplishing this milestone during the {season_name} season"
        question = self.question_template(question_statement, options, correct_response, description)
        return question

    def most_points(self, league_id):
        league_name = self.league_mapping[league_id]
        df = self.generate_df(points_query.format(league_id)).sort_values('points',ascending=False)
        options_df = df.groupby(['club_name'])['points'].max().reset_index().sort_values('points',ascending=False).head(4)
        season_id = df['season_id'].iloc[0]
        season_name = self.get_season_name(season_id)
        points = int(df['points'].iloc[0])
        correct_response = df['club_name'].iloc[0]
        options = [i for i in options_df['club_name']]
        question_statement = "Which team achieved the most points in a single {} season?".format(league_name)
        description = f"{correct_response} achieved the most points in {league_name}, accumulating {points} points during the {season_name} season"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def fewest_points(self, league_id):
        league_name = self.league_mapping[league_id]
        df = self.generate_df(points_query.format(league_id)).sort_values('points')
        df = df[df['club_name'] != 'Juventus']
        options_df = df.groupby(['club_name'])['points'].max().reset_index().sort_values('points',ascending=True).head(3)
        season_id = df['season_id'].iloc[0]
        season_name = self.get_season_name(season_id)
        points = int(df['points'].iloc[0])
        correct_response = options_df['club_name'].iloc[0]
        options = [i for i in options_df['club_name']]
        options.append(correct_response)
        question_statement = "Which team earned the fewest points in a single {} season?".format(league_name)
        description = f"{correct_response} achieved the fewest points in {league_name}, accumulating {points} points during the {season_name} season"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def scored_most_goals(self, league_id):
        league_name = self.league_mapping[league_id]
        df = self.generate_df(most_scored_goals_query.format(league_id))
        options_df = df.groupby(['club_name'])['goals'].max().reset_index().sort_values('goals',ascending=False).head(4)
        season_id = df['season_id'].iloc[0]
        season_name = self.get_season_name(season_id)
        goals = int(df['goals'].iloc[0])
        correct_response = df['club_name'].iloc[0]
        options = [i for i in options_df['club_name']]
        question_statement = "Which team scored the most goals in a single {} season?".format(league_name)
        description = f"{correct_response} scored the most goals in a single season, netting {goals} goals during the {season_name} {league_name} season"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    
    def conceded_most_goals(self, league_id):
        league_name = self.league_mapping[league_id]
        df = self.generate_df(most_conceded_goals_query.format(league_id))
        options_df = df.groupby(['club_name'])['goals_conceded'].max().reset_index().sort_values('goals_conceded',ascending=False).head(4)
        season_id = df['season_id'].iloc[0]
        season_name = self.get_season_name(season_id)
        goals_conceded = int(df['goals_conceded'].iloc[0])
        correct_response = df['club_name'].iloc[0]
        options = [i for i in options_df['club_name']]
        question_statement = "Which team conceded the most goals in a single {} season?".format(league_name)
        description = f"{correct_response} conceded {goals_conceded} goals in {season_name} season which is a {league_name} record for conceding the most goals in a season"
        question = self.question_template(question_statement, options, correct_response, description)
        return question

    def most_fouls(self, league_name):
        df = self.generate_df(team_stats_query.format(league_name))
        df = df.sort_values('fouls',ascending=False).head(4)
        correct_response = df['team_name'].iloc[0]
        option_teams = list(df['team_name'].unique())
        option_teams.remove(correct_response)
        options = random.sample(option_teams, 3)
        options.append(correct_response)
        question_statement = "Which team committed the highest number fouls in the {} during the 2023/2024 season?".format(league_name)
        description = f"{correct_response} committed the most fouls in the {league_name} during the 2023/2024 season, showcasing their aggressive and physical style of play"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def most_corners(self, league_name):
        df = self.generate_df(team_stats_query.format(league_name))
        df = df.sort_values('cornerkicks',ascending=False).head(4)
        correct_response = df['team_name'].iloc[0]
        option_teams = list(df['team_name'].unique())
        option_teams.remove(correct_response)
        options = random.sample(option_teams, 3)
        options.append(correct_response)
        question_statement = "Which club earned the highest number of corner kicks in the 2023/2024 {} season?".format(league_name)
        description = f"{correct_response} earned the most corner kicks in the {league_name} during the 2023/2024 season, reflecting their persistent attacking strategy and ability to apply continuous pressure on their opponents"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def most_offsides(self, league_name):
        df = self.generate_df(team_stats_query.format(league_name))
        df = df.sort_values('offsides',ascending=False).head(4)
        correct_response = df['team_name'].iloc[0]
        option_teams = list(df['team_name'].unique())
        option_teams.remove(correct_response)
        options = random.sample(option_teams, 3)
        options.append(correct_response)
        question_statement = "Which team was caught offside the most times in the {} during the 2023/2024 season?".format(league_name)
        description = f"{correct_response} was caught offside the most times in the {league_name} during the 2023-2024 season, which highlights their aggressive forward play and attempts to break through opposition defenses"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def highest_avg_possesion(self, league_name):
        df = self.generate_df(team_stats_query.format(league_name))
        df = df.sort_values('avg_possession',ascending=False).head(4)
        correct_response = df['team_name'].iloc[0]
        option_teams = list(df['team_name'].unique())
        option_teams.remove(correct_response)
        options = random.sample(option_teams, 3)
        options.append(correct_response)
        question_statement = "Which club recorded the highest average possession percentage in the {} for the 2023/2024 season?".format(league_name)
        description = f"{correct_response} had the highest average ball possession during the 2023/2024 {league_name} season. This dominance in possession underscores their control-oriented style of play and ability to dictate the tempo of matches"
        question = self.question_template(question_statement, options, correct_response, description)
        return question

    def club_coach(self, league_name):
        df = self.generate_df(team_coach_query.format(league_name)).sample(4)
        coach_name = df['coach_name'].iloc[0]
        correct_response = df['team'].iloc[0]
        options = list(df['team'].unique())
        question_statement = "As of August 2024, which team is coached by {}?".format(coach_name)
        description = f"{coach_name} is the head coach of {correct_response}"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
        
    def first_cl_win(self):
        options = ['AC Milan',
                   'Real Madrid',
                   'Benfica',
                   'Barcelona']
        correct_response = 'Real Madrid'
        question_statement = "Which club was the first to win the European Cup (now the UEFA Champions League) in 1956?"
        description = "Real Madrid won the first European Cup title in 1956"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def oldest_club(self):
        options = ['Nottingham Forest',
                        'Sheffield FC',
                        'Everton FC',
                        'Middlesbrough FC']
        correct_response = 'Sheffield FC'
        question_statement = "Which is the oldest football club in the world?"
        description = "Sheffield FC, founded in 1857, is recognized as the oldest football club in the world still in existence"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def never_won_cl(self):
        df = self.generate_df(cl_titles_query)
        option_teams = ['AS Roma',
                        'Arsenal FC',
                        'Paris Saint-Germain',
                        'Sevilla FC',
                        'Atletico Madrid',
                        'Tottenham']
        champion_teams = list(df['team_name'].unique())
        correct_response = random.choice(option_teams)
        options = random.sample(champion_teams, 3)
        options.append(correct_response)
        question_statement = "Which club has never won the Champions League title?"
        description = f"""{correct_response} has never won the UEFA Champions League"""
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def winner_with_fewest_points(self, league_id):
        league_name = self.league_mapping[league_id]
        df = self.generate_df(won_league_with_fewest_points_query.format(league_id))
        correct_df = df.iloc[0]
        correct_response = correct_df['name']
        points = correct_df['points']
        season = correct_df['season_id']
        season_name = f"{season}/{season+1}"
        teams = list(df['name'].unique())
        teams.remove(correct_response)
        options = teams[:3]
        options.append(correct_response)
        question_statement = f"Which {league_name} team won the title with the fewest points in a 38-game season?"
        description = f"{correct_response} won the {league_name} title in the {season_name} season with {points} points, the lowest total for a 38-game season"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def most_cl_titles(self):
        df = self.generate_df(cl_titles_query).head(4)
        correct_response = df['team_name'].iloc[0]
        options = list(df['team_name'].unique())
        question_statement = "Which club has won the most UEFA Champions League titles?"
        description = f"""{correct_response} holds the record for the most UEFA Champions League titles, having won the competition 15 times. AC Milan has won the title 7 times. Liverpool FC and Bayern Munich 6 times each"""
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def most_el_titles(self):
        df = self.generate_df(el_titles_query).head(4)
        correct_response = df['team_name'].iloc[0]
        options = list(df['team_name'].unique())
        question_statement = "Which club has won the most Europe League titles?"
        description = f"""{correct_response} holds the record for the most Europe League titles, having won the competition 7 times. Inter Milan, Liverpool FC and Atlético de Madrid have won the title 3 times each"""
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def never_won_el(self):
        df = self.generate_df(el_titles_query)
        option_teams = ['AS Roma',
                        'Arsenal FC',
                        'Paris Saint-Germain',
                        ]
        champion_teams = list(df['team_name'].unique())
        correct_response = random.choice(option_teams)
        options = random.sample(champion_teams, 3)
        options.append(correct_response)
        question_statement = "Which club has never won the Europe League (former UEFA cup) title?"
        description = f"""{correct_response} has never won the Europe League"""
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def cup_titles(self, league_name):
        df = self.generate_df(cup_titles_query.format(league_name)).head(4)
        title_name_from_dataset = df['title'].iloc[0]
        s = title_name_from_dataset.split()
        title_name = ' '.join(s[:-1])
        correct_response = df['team_name'].iloc[0]
        number = int(df['number'].iloc[0])
        options = list(df['team_name'].unique())
        question_statement = f"Which club has won the most {title_name} titles?"
        description = f"{correct_response} holds the record for winning the most {title_name} titles having won the tournament {number} times"
        question = self.question_template(question_statement, options, correct_response, description)
        return question

    def most_domestic_championship_titles(self, league_id):
        league_name = self.league_mapping[league_id]
        df = self.generate_df(most_titles_won_query.format(league_id, league_id))
        ndf = df.head(4)
        correct_response = ndf['name'].iloc[0]
        number = int(ndf['title_cnt'].iloc[0])
        options = list(ndf['name'].unique())
        question_statement = "Which club has won the most {} titles?".format(league_name)
        description = f"{correct_response} is a {number}-times {league_name} champion which is a current record"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def never_won_domestic_championship_title(self, league_id):
        league_name = self.league_mapping[league_id]
        df = self.generate_df(most_titles_won_query.format(league_id, league_id))
        options_df = df[df['title_cnt']>0].sample(3)
        correct_df = df[df['title_cnt']==0].sample(1)
        correct_response = correct_df['name'].iloc[0]
        options = list(options_df['name'].unique())
        options.append(correct_response)
        question_statement = "Which club has never won the {} title?".format(league_name)
        description = f"As of 2024, {correct_response} has never been a {league_name} champion"
        question = self.question_template(question_statement, options, correct_response, description)
        return question

    def player_from_team(self):
        df = self.generate_df(player_for_the_team_query).head(150)
        ndf = df.groupby('international_team').apply(lambda x: x.sample(1)).reset_index(drop=True)
        ndf = ndf.sample(4)
        correct_response = ndf['international_team'].iloc[0]
        player = ndf['player_name'].iloc[0]
        options = list(ndf['international_team'].unique())
        question_statement1 = "Which national team does {} play for?".format(player)
        question_statement2 = "Which national team is {} a member of?".format(player)
        question_statement3 = "For which national team does {} play?".format(player)
        question_statement = random.choice([question_statement1,question_statement2,question_statement3])
        description = f"""{player} represents {correct_response}"""
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def team_position(self, season, league_id, title_won=False):
        league_name = self.league_mapping[league_id]
        df = self.generate_df(standing_query.format(season, league_id))
        df = df.sort_values('rank', ascending = True)[['club_name', 'rank']]
        season_name = f"{season}/{season+1}"
        if title_won:
            correct_response = df['club_name'].iloc[0]
            team1 = df['club_name'].iloc[1]
            team2 = df['club_name'].iloc[2]
            team3 = df['club_name'].iloc[3]
            question_statement = "Who won the {} title in the {} season?".format(league_name, season_name)
            description = f"""{correct_response} secured the title with an impressive performance throughout the season"""
            options = [correct_response, team1, team2, team3]
        else:
            rank = random.choice([4,5,6,7])
            correct_response = df['club_name'].iloc[rank]
            correct_val = df['rank'].iloc[rank]
            team1 = df['club_name'].iloc[0]
            team2 = df['club_name'].iloc[1]
            team3 = df['club_name'].iloc[2]
            team4 = df['club_name'].iloc[3]
            question_statement = "Which of the following teams did not finish in the top four in the {} {} season?".format(season_name, league_name)
            description = f"""The correct answer is {correct_response}, they ended the {season_name} {league_name} season in <b>{correct_val}th</b> place_ {team1}, {team2}, {team3} and {team4} finished in the top 4"""
            options = [correct_response, team1, team2, team4]
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def maradonna_club(self):
        options = ['AC Milan',
                   'SSC Napoli',
                   'Inter Milan',
                   'Juventus FC']
        correct_response = 'SSC Napoli'
        question_statement = "Which club did Diego Maradona famously play for, leading them to two Serie A titles in the late 1980s?"
        description = "Maradona is considered a football icon in Naples, and his time at the club is one of the most celebrated periods in its history"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def ronaldihno_club(self):
        options = ['AC Milan',
                   'FC Barcelona',
                   'Inter Milan',
                   'Real Madrid']
        correct_response = 'FC Barcelona'
        question_statement = "Which club did Ronaldinho play for when he won the FIFA World Player of the Year award twice in 2004 and 2005?"
        description = "Ronaldinho joined the club in 2003 and became one of the most influential players in their history, winning two La Liga titles (2004-05, 2005-06) and the UEFA Champions League in 2005-06"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def maldini_club(self):
        options = ['AC Milan',
                   'Juventus FC',
                   'Inter Milan',
                   'Real Madrid']
        correct_response = 'AC Milan'
        question_statement = "Which club did Paolo Maldini spend his entire career at, winning five UEFA Champions League titles?"
        description = "Paolo Maldini spent his entire professional career playing for AC Milan. Maldini played for AC Milan from 1984 to 2009, making over 900 appearances"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def henry_club(self):
        options = ['Arsenal FC',
                   'Manchester United',
                   'Chelsea FC',
                   'Liverpool FC']
        correct_response = 'Arsenal FC'
        question_statement = "Which club did Thierry Henry lead to an unbeaten Premier League season in 2003/2004, earning them the title 'The Invincibles'"
        description = "Thierry Henry is one of the Arsenal's most iconic players. His impact at Arsenal made him a legend at the club and in English football"
        question = self.question_template(question_statement, options, correct_response, description)
        return question