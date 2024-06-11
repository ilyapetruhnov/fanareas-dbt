import random
from dagster_fanareas.ops.utils import get_dim_name_and_id
from dagster_fanareas.quizzes.queries import guess_the_team_query, player_for_the_team_query
from dagster_fanareas.quizzes.quizzes import Quizzes
from datetime import datetime

class TeamQuizz(Quizzes):
    def __init__(self, title: str, description: str, quiz_type: int, is_demo: bool) -> None:
        super().__init__(title, description, quiz_type, is_demo)
        self.metrics = ['yellowcards_count',
                        'corners_count',
                        'cleansheets_count',
                        'ball_possession_average',
                        'goals_conceded_all_count'
                        ]
        self.quiz_collection = []

    def scored_num_goals(self):
        season = self.get_season()
        goals = [40, 45, 50]
        n_goals = random.choice(goals)
        df = self.generate_df(guess_the_team_query.format(season))
        df['goals_all_count'] = df['goals_all_count'].astype(int)
        correct_response = df[df['goals_all_count'] < n_goals]['team'].sample(1).iloc[0]
        options = list(df[df['goals_all_count'] > n_goals]['team'].sample(3))
        options.append(correct_response)
        question_statement = "Which team scored less than {} goals in {} season?".format(n_goals, season)
        question = self.question_template(question_statement, options, correct_response)
        return question
    
    def team_draws_wins_losses(self):
        season = self.get_season()
        df = self.generate_df(guess_the_team_query.format(season))
        metrics = ['team_draws_count', 'team_wins_count', 'team_lost_count']
        metric = random.choice(metrics)
        if metric == 'team_draws_count':
            val = 10
            correct_response = df[df[metric] > val]['team'].sample(1).iloc[0]
            options = list(df[df[metric] < val]['team'].sample(3))
            options.append(correct_response)
            question_statement = "Who played more than {} draws in {} season?".format(val, season)
        elif metric == 'team_wins_count':
            val = 25
            correct_response = df[df[metric] > val]['team'].sample(1).iloc[0]
            options = list(df[df[metric] < val]['team'].sample(3))
            options.append(correct_response)
            question_statement = "Who won more than {} matches in {} season?".format(val, season)
        elif metric == 'team_lost_count':
            val = 20
            correct_response = df[df[metric] > val]['team'].sample(1).iloc[0]
            options = list(df[df[metric] < val]['team'].sample(3))
            options.append(correct_response)
            question_statement = "Who lost more than {} matches in {} season?".format(val, season)
        question = self.question_template(question_statement, options, correct_response)
        return question
    
    def team_metrics(self):
        metric = random.choice(self.metrics)
        season = self.get_season()
        df = self.generate_df(guess_the_team_query.format(season))
        df = df.sample(4).sort_values(metric, ascending = False)[['team',metric]].drop_duplicates(metric)
        df[metric] = df[metric].astype(int)
        correct_response = df['team'].iloc[0]
        correct_val = df[metric].iloc[0]

        team1 = df['team'].iloc[1]
        val1 = df[metric].iloc[1]

        team2 = df['team'].iloc[2]
        val2 = df[metric].iloc[2]

        team3 = df['team'].iloc[3]
        val3 = df[metric].iloc[3]

        options = list(df['team'])

        if metric == 'yellowcards_count':
            question_statement = "Which team received more yellow cards in {} season?".format(season)
            description = f"""During the {season} Premier League season <i>{correct_response}</i> received <b>{correct_val}</b> yellow cards _ <i>{team1}</i> received <b>{val1}</b> yellow cards _ <i>{team2}</i> received <b>{val2}</b> yellow cards _ <i>{team3}</i> received <b>{val3}</b> yellow cards"""
        elif metric == 'ball_possession_average':
            question_statement = "Which team had a higher average ball possession in {} season?".format(season)
            description = f"""In the {season} Premier League season <i>{correct_response}'s</i> average ball possession was <b>{correct_val}%</b> _ <i>{team1}</i> ball possession was <b>{val1}%</b> _ <i>{team2}</i> ball possession was <b>{val2}%</b> _ <i>{team3}</i> ball possession was <b>{val3}%</b>"""
        elif metric == 'corners_count':
            question_statement = "Which team took more corners in {} season?".format(season)
            description = f"""During the {season} Premier League season <i>{correct_response}</i> took <b>{correct_val}</b> corners _ <i>{team1}</i> took <b>{val1}</b> corners _ <i>{team2}</i> took <b>{val2}</b> corners _ <i>{team3}</i> took <b>{val3}</b> corners"""
        elif metric == 'goals_conceded_all_count':
            question_statement = "Which team conceded more goals in {} season?".format(season)
            description = f"""During the {season} Premier League season <i>{correct_response}</i> conceded <b>{correct_val}</b> goals _ <i>{team1}</i> conceded <b>{val1}</b> goals _ <i>{team2}</i> conceded <b>{val2}</b> goals _ <i>{team3}</i> conceded <b>{val3}</b> goals"""
        elif metric == 'cleansheets_count':
            question_statement = "Which team kept more clean sheets in {} season?".format(season)
            description = f"""During the {season} Premier League season <i>{correct_response}</i> kept <b>{correct_val}</b> clean sheets _ <i>{team1}</i> kept <b>{val1}</b> clean sheets _ <i>{team2}</i> kept <b>{val2}</b> clean sheets _ <i>{team3}</i> kept <b>{val3}</b> clean sheets. A clean sheet means that the team did not concede any goals during a match."""
        question = self.question_template(question_statement, options, correct_response, description)
        self.metrics.remove(metric)
        return question
    
    def the_most_fewest_metrics(self):
        metric = random.choice(self.metrics)
        season = self.get_season()
        df = self.generate_df(guess_the_team_query.format(season))
        df = df.sort_values(metric, ascending = False)[['team', metric]].drop_duplicates(metric)
        df[metric] = df[metric].astype(int)
        df_size = len(df) - 1
        correct_response = df['team'].iloc[0]
        correct_val = df[metric].iloc[0]
        
        positions = random.sample(range(1, df_size), 3)
        positions.sort()
        team1 = df['team'].iloc[positions[0]]
        team2 = df['team'].iloc[positions[1]]
        team3 = df['team'].iloc[positions[2]]

        val1 = int(df[metric].iloc[positions[0]])
        val2 = int(df[metric].iloc[positions[1]])
        val3 = int(df[metric].iloc[positions[2]])

        options = [correct_response, team1, team2, team3]

        if metric == 'yellowcards_count':
            question_statement = "Which team received the most yellow cards in {} season?".format(season)
            description = f"""During the {season} Premier League season {correct_response} received {correct_val} yellow cards_ {team1} received {val1} yellow cards_  {team2} received {val2} yellow cards_  {team3} received {val3} yellow cards"""
        elif metric == 'ball_possession_average':
            question_statement = "Which team had the highest average ball possession in {} season?".format(season)
            description = f"""{correct_response} had the highest average ball possession of {correct_val}% in the {season} Premier League season which helped them dominate matches and create numerous scoring opportunities"""
        elif metric == 'corners_count':
            question_statement = "Which team took the most corners in {} season?".format(season)
            description = f"""In the {season} Premier League season {correct_response} earned a total of {correct_val} corners, demonstrating their offensive prowess and ability to maintain pressure on their opponents_ {team1} took {val1} corners_  {team2} took {val2} corners_  {team3} took {val3} corners"""
        elif metric == 'goals_conceded_all_count':
            question_statement = "Which team conceded the most goals in {} season?".format(season)
            description = f"""The correct answer is {correct_response}, who conceded the most goals in the {season} Premier League season, with a total of {correct_val} goals conceded._ {team1} conceded {val1} goals_  {team2} conceded {val2} goals_  {team3} conceded {val3} goals"""
        elif metric == 'cleansheets_count':
            question_statement = "Which team kept the most clean sheets in {} season?".format(season)
            description = f"""The correct answer is {correct_response}, who kept the {correct_val} clean sheets in the {season} Premier League season _ {team1} kept {val1} clean sheets _  {team2} kept {val2} clean sheets _  {team3} kept {val3} clean sheets"""
        question = self.question_template(question_statement, options, correct_response, description)
        self.metrics.remove(metric)
        return question
    
    def team_position(self, title_won=False):
        season = self.get_season()
        df = self.generate_df(guess_the_team_query.format(season))
        df = df.sort_values('position', ascending = True)[['team', 'position']]
        df_size = len(df) - 1
        if title_won:
            correct_response = df['team'].iloc[0]
            positions = random.sample(range(1, df_size), 3)
            positions.sort()
            team1 = df['team'].iloc[positions[0]]
            team2 = df['team'].iloc[positions[1]]
            team3 = df['team'].iloc[positions[2]]
            question_statement = "Who won the Premier League title in the {} season?".format(season)
            description = f"""{correct_response} secured the title with an impressive performance throughout the season"""
            options = [correct_response, team1, team2, team3]
        else:
            correct_response = df['team'].iloc[5]
            correct_val = df['position'].iloc[5]
            team1 = df['team'].iloc[0]
            team2 = df['team'].iloc[1]
            team3 = df['team'].iloc[2]
            team4 = df['team'].iloc[3]
            question_statement = "Which of the following teams did not finish in the top four in the {} Premier League season?".format(season)
            description = f"""The correct answer is {correct_response}, they ended the {season} Premier League season in <b>{correct_val}th</b> place_ {team1}, {team2}, {team3} and {team4} finished in the top 4"""
            options = [correct_response, team1, team2, team4]
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def team_relegated(self) -> dict:
        season = self.get_season()
        df = self.generate_df(guess_the_team_query.format(season))
        df = df.sort_values('position', ascending = False)[['team', 'position']]
        df_size = len(df) - 1
        correct_response = df['team'].iloc[0]
        positions = random.sample(range(6, df_size), 3)
        positions.sort()
        team1 = df['team'].iloc[positions[0]]
        team2 = df['team'].iloc[positions[1]]
        team3 = df['team'].iloc[positions[2]]
        question_statement = "In the {} Premier League season, which team was relegated to the Championship?".format(season)
        description = f"""The correct answer is {correct_response}, who were relegated to the Championship at the end of the {season} Premier League season"""
        options = [correct_response, team1, team2, team3]
        question = self.question_template(question_statement, options, correct_response, description)
        return question

    def home_venue(self):
        season = self.get_season()
        df = self.generate_df(guess_the_team_query.format(season))
        df = df.sort_values('position', ascending = True)[['team', 'venue']].head(13).sample(4)
        correct_response = df['team'].iloc[0]
        correct_val = df['venue'].iloc[0]

        options = [i for i in df['team']]
        question_statement = "Which team has {} as their home venue?".format(correct_val)
        description = f"""{correct_val} is the home stadium of {correct_response} FC"""

        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def player_from_team(self):
        season = '2023/2024'
        df = self.generate_df(player_for_the_team_query.format(season))
        df = df.sample(4)
        correct_response = df['team'].iloc[0]
        player = df['fullname'].iloc[0]
        options = list(df['team'].unique())
        question_statement = "As of the 2023/2024 season, which team does {} play for?".format(player)
        description = f"""As of the 2023/2024 season, {player} plays for {correct_response}"""
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def get_question(self, i):
        if i == 1:
            question = self.scored_num_goals()

        elif i == 2:
            question = self.team_draws_wins_losses()

        elif i == 3:
            question = self.team_metrics()

        elif i == 4:
            question = self.the_most_fewest_metrics()

        elif i == 5:
            question = self.team_position()

        elif i == 6:
            question = self.team_relegated()

        elif i == 7:
            question = self.home_venue()

        elif i == 8:
            question = self.player_from_team()
            
        return question 
    
    def fill_quiz_with_questions(self):
        for i in range(1, 8):
            question = self.get_question(i)
            self.collect_questions(question)
    

            