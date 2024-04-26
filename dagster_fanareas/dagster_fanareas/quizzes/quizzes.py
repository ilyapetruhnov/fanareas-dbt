import pandas as pd
import random
from dagster_fanareas.ops.utils import post_json, create_db_session
from dagster_fanareas.quizzes.queries import *
from dagster_fanareas.quizzes.maps import top_n_metric_map
import requests


class Quizzes:
    def __init__(self, title: str, description: str, quiz_type: int, is_demo: bool) -> None:
        self.title = title
        self.description = description
        self.quiz_type = quiz_type
        self.is_demo = is_demo
        self.url = "https://fanareas.com/api/quizzes/createQuizz"
        self.quiz_collection = []

    def quiz_template(self, 
                      questions, 
                      team_name, 
                      season_name, 
                      entityIdTeam, 
                      entityIdSeason, 
                      entityTypeTeam,
                      entityTypeSeason):
        
        json_data = {"title": self.title,
                     "type": self.quiz_type,
                     "description": self.description,
                     "questions": questions,
                     "isDemo": self.is_demo,
                     "quizzTags": [
                         {
                             "entityId": entityIdTeam,
                             "entityName": team_name,
                             "entityType": entityTypeTeam
                         },
                         {
                             "entityId": entityIdSeason,
                             "entityName": season_name,
                             "entityType": entityTypeSeason
                         }
                     ]
                     }

        return json_data

    def question_template(self, question_statement, options, correct_response) -> dict:
        if len(options) < 4:
            result = None
        else:
            random.shuffle(options)
            result = {
                "description": question_statement,
                "quizQuestionOptions": options,
                "correctAnswer": correct_response
            }
        return result

    def collect_questions(self, question):
        if question is not None:
            self.quiz_collection.append(question)
        return True

    @staticmethod
    def format_metric(metric: str) -> str:
        if metric == 'penalties':
            result = 'penalty goals'
        if metric == 'substitute_appearances':
            result = 'substitute appearances'
        if metric == 'goal_assists':
            result = 'goals + assists'
        else:
            result = metric.replace('_', ' ')
        return result

    def generate_df(self, query: str) -> pd.DataFrame:
        engine = create_db_session()
        return pd.read_sql(query, con=engine)

    def get_team_name_and_id() -> dict:
        engine = create_db_session()
        team_id = requests.get('https://fanareas.com/api/teams/generateId').json()
        team_qr = """select name from teams where id = {}""".format(team_id)
        df = pd.read_sql(team_qr, con=engine)
        team_name = df['name'].iloc[0]
        return {'team_name': team_name, 'team_id': team_id}
    
    # def generate_question(self, query: str, statement: str, team_name: str, cols: tuple) -> dict:
    #     df = self.generate_df(query)
    #     sample_df = df.sample(n=4)
    #     correct_idx = random.randint(0, 3)
    #     correct_row = sample_df.iloc[correct_idx]
    #     correct_vals = [correct_row[i] for i in cols]
    #     question_statement = statement.format(team_name, *correct_vals)
    #     options = list(sample_df['fullname'])
    #     correct_response = correct_row['fullname']
    #     question = self.question_template(question_statement, options, correct_response)
    #     return question

    def generate_player_metric_question(self, query: str, metric: str, season_name: str) -> dict:
        df = self.generate_df(query)
        sample_df = df.groupby(metric)['fullname'].apply(list).reset_index()
        if len(sample_df)>3:
            sample_df = sample_df.sample(4)
            sample_df['fullname'] = sample_df['fullname'].apply(lambda x: random.choice(x))
            correct_idx = 0
            correct_row = sample_df.iloc[correct_idx]
            correct_metric = correct_row[metric]
            correct_response = correct_row['fullname']

            if metric == 'nationality':
                question_statement = "Who is a citizen of {0}?".format(correct_metric)
            elif metric == 'position':
                question_statement = "Which player played at {0} position in the {1} season?".format(correct_metric,
                                                                                                    season_name)
            elif metric == 'jersey_number':
                question_statement = "Which player played under {0} jersey number in the {1} season?".format(correct_metric,
                                                                                                            season_name)

            options = list(sample_df['fullname'])
            question = self.question_template(question_statement, options, correct_response)
            return question
        else:
            return None

    def generate_player_joined_question(self, query: str, team_name: str, season_name: str) -> dict:
        df = self.generate_df(query)
        correct_df = df[df['season_name'] == season_name]
        if correct_df.empty == True:
            return None
        else:
            correct_response = correct_df['fullname'].iloc[0]
            options_df = df[df['season_name'] != season_name].sample(3)
            options = [i for i in options_df['fullname']]
            options.append(correct_response)
            question_statement = "Who joined {} in the {} season?".format(team_name, season_name)
            question = self.question_template(question_statement, options, correct_response)
            return question
    
    def generate_player_departure_question(self, query: str, team_name: str, season_name: str) -> dict:
        df = self.generate_df(query)
        correct_df = df[df['season_name'] == season_name]
        if correct_df.empty == True:
            return None
        else:
            correct_response = correct_df['fullname'].iloc[0]
            options_df = df[df['season_name'] != season_name].sample(3)
            options = [i for i in options_df['fullname']]
            options.append(correct_response)
            question_statement = "Who left {} in the {} season?".format(team_name, season_name)
            question = self.question_template(question_statement, options, correct_response)
            return question
        
    def generate_player_age_question(self, query: str, team_name: str, season_name: str, youngest: bool) -> dict:
        df = self.generate_df(query)
        sample_df = df.groupby('age')['fullname'].apply(list).reset_index().sort_values('age', ascending=youngest).head(4)
        sample_df['fullname'] = sample_df['fullname'].apply(lambda x: random.choice(x))
        correct_row = sample_df.iloc[0]
        correct_response = correct_row['fullname']
        if youngest:
            question_statement = "Who was the youngest player in {0} squad in the {1} season?".format(team_name,
                                                                                                    season_name)
        else:
            question_statement = "Who was the oldest player in {0} squad in the {1} season?".format(team_name,
                                                                                                  season_name)
        options = list(sample_df['fullname'])
        question = self.question_template(question_statement, options, correct_response)
        return question
    
    def generate_player_position_played_question(self, query: str, season_name: str) -> dict:
        df = self.generate_df(query)
        positions = [i for i in df['position'].unique()]
        position = random.choice(positions)
        correct_df = df[df['position'] == position]
        correct_response = random.choice(correct_df['fullname'].unique())
        outer = df.merge(correct_df, how='outer', indicator=True)
        #perform anti-join
        anti_join_df = outer[(outer._merge=='left_only')].drop('_merge', axis=1)
        sample_df = anti_join_df.sample(3)
        options = [i for i in sample_df.fullname]
        options.append(correct_response)
        question_statement = "Who played at {} position in the {} season?".format(position, season_name)
        return self.question_template(question_statement, options, correct_response)

    def generate_player_position_stats_question(self, query: str, season_name: str, metric: str) -> dict:
        df = self.generate_df(query)
        positions = [i for i in df['position'].unique()]
        random.shuffle(positions)
        for player_position in positions:
            if len(df[df['position'] == player_position]) > 3:
                result_df = df[df['position'] == player_position].sort_values(metric, ascending=False).head(4)
                correct_response = result_df.iloc[0]['fullname']
                options = [i for i in result_df.fullname]
                formatted_metric = self.format_metric(metric)
                if metric == 'substitute_appearances':
                    question_statement = "Which {} player had the most appearances coming off the bench in the {} season?".format(
                        player_position, season_name)
                    return self.question_template(question_statement, options, correct_response)
                else:
                    question_statement = "Which {} player had more {} in the {} season?".format(player_position,
                                                                                         formatted_metric,
                                                                                         season_name)
                    return self.question_template(question_statement, options, correct_response)
        return None
    
    def generate_player_stats_question(self, query: str, season_name: str, metric: str) -> dict:
        df = self.generate_df(query)
        grouped_df = df.groupby(f'{metric}_rn')['fullname'].apply(list).reset_index()

        correct_response = grouped_df[grouped_df[f'{metric}_rn'] == 1]['fullname'][0][0]
        options = [i[0] for i in grouped_df.fullname][:4]
        formatted_metric = self.format_metric(metric)
        if metric == 'substitute_appearances':
            question_statement = "Who had the most appearances coming off the bench in the {} season?".format(
                season_name)
        else:
            question_statement = "Who had more {} in the {} season?".format(formatted_metric, season_name)
        question = self.question_template(question_statement, options, correct_response)
        return question

    def generate_player_sent_off_question(self, query: str, season_name: str) -> dict:
        df = self.generate_df(query)
        metric = 'red_cards'
        df = df[~df['appearances'].isnull()]

        question_statement = "Who has been sent off at least in one match in the {} season?".format(
            season_name)

        correct_df = df[df[metric] > 0]
        if correct_df.empty:
            return None
        else:
            correct_response = correct_df['fullname'].iloc[0]

            options_df = df[df[metric].isnull()].sample(3)

            options = [i for i in options_df.fullname]
            options.append(correct_response)
            question = self.question_template(question_statement, options, correct_response)
            return question

    def generate_player_own_goal_question(self, query: str, season_name: str) -> dict:
        df = self.generate_df(query)
        metric = 'own_goals'
        df = df[~df['appearances'].isnull()]
        question_statement = "Which player scored an own goal in the {} season?".format(season_name)
        correct_df = df[df[metric] > 0]
        if correct_df.empty:
            return None
        else:
            correct_response = correct_df['fullname'].iloc[0]

            options_df = df[df[metric].isnull()].sample(3)

            options = [i for i in options_df.fullname]
            options.append(correct_response)
            question = self.question_template(question_statement, options, correct_response)
            return question

    def generate_player_2_metrics_question(self, query: str, season_name: str, metric: str) -> dict:
        df = self.generate_df(query)
        if metric == 'red_cards':
            correct_df = df[~df['red_cards'].isnull()][['fullname', 'team_name', 'yellow_cards', 'red_cards']]
            if len(correct_df)>3:
                yellow_cards = int(correct_df['yellow_cards'].iloc[0])
                red_cards = int(correct_df['red_cards'].iloc[0])
                correct_response = correct_df['fullname'].iloc[0]
                options_df = df[df['red_cards'].isnull()].sample(3)
                question_statement = "Which player had {} yellow cards and {} red cards in the {} season?".format(
                    yellow_cards, red_cards, season_name)
            else:
                return None
        else:
            correct_df = df[~df['goal_assists'].isnull()][['fullname', 'team_name', 'goals', 'assists', 'goal_assists']]
            if len(correct_df)>3:
                goals = int(correct_df['goals'].iloc[0])
                assists = int(correct_df['assists'].iloc[0])
                correct_response = correct_df['fullname'].iloc[0]
                options_df = correct_df.iloc[1:].sample(3)
                question_statement = "Who had {} goals and {} assists in the {} season?".format(goals, assists,
                                                                                                        season_name)
            else:
                return None
        options = [i for i in options_df.fullname]
        options.append(correct_response)
        question = self.question_template(question_statement, options, correct_response)
        return question

    def generate_player_more_than_n_question(self, query: str, season_name: str, metric: str) -> dict:
        df = self.generate_df(query)
        n = top_n_metric_map[metric]
        correct_df = df[df[metric] > n]
        if correct_df.empty == True:
            return None
        else:
            correct_response = correct_df['fullname'].iloc[0]
            options_df = df[df[metric] <= n].sample(3)
            options = [i for i in options_df.fullname]
            options.append(correct_response)
            formatted_metric = self.format_metric(metric)
            if metric == 'substitute_appearances':
                question_statement = "Which player had more than {} appearances coming off the bench in the {} season?".format(
                    n, season_name)
            else:
                question_statement = "Who had more than {} {} in the {} season?".format(n, formatted_metric,
                                                                                                 season_name)
            question = self.question_template(question_statement, options, correct_response)
            return question

    def generate_quiz_questions(self, query: str, statement: str, cols: tuple) -> list:
        df = self.generate_df(query)

        q_lst = []
        for i in range(10):
            dimension = cols[0]
            val_dim = df[df[dimension].map(df[dimension].value_counts()) > 4][dimension].value_counts().index.unique()[
                i]

            sample_df = df[df[dimension] == val_dim].sample(n=4)
            correct_idx = random.randint(0, 3)
            correct_row = sample_df.iloc[correct_idx]
            correct_vals = [correct_row[i] for i in cols]
            question_statement = statement.format(*correct_vals)
            options = list(sample_df['fullname'])
            correct_response = correct_row['fullname']
            question = self.question_template(question_statement, options, correct_response)
            q_lst.append(question)
        return q_lst

    def generate_simple_questions(self, query: str, statement: str, dimension: str):
        df = self.generate_df(query)
        q_lst = []
        for i in range(10):
            sample_df = df.sample(n=4).sort_values(dimension)
            options = list(sample_df['fullname'])
            correct_response = sample_df.iloc[3]['fullname']
            question = self.question_template(statement, options, correct_response)
            q_lst.append(question)
        return q_lst

    def generate_team_questions(self, query: str, statement: str, dimension: str):
        df = self.generate_df(query)
        seasons = list(df['season'].unique())[2:]
        metrics = [
            'losses', 'wins', 'draws',
            'goals', 'goals_conceded',
            'yellow_cards', 'red_cards',
            'clean_sheets', 'corners'
        ]
        q_lst = []
        counter = 0
        # check out dagster project linkedin
        while counter < 10:
            dim = random.choice(metrics)
            season = random.choice(seasons)
            formatted_dim = self.format_metric(dim)
            correct_vals = (formatted_dim, season)
            dimension = f'{dim}_rn'
            sample_df = df[(df['season'] == season) & (df[dimension] <= 4)]
            correct_row = sample_df[sample_df[dimension] == 1]
            if len(correct_row) > 1:
                pass
            else:
                counter += 1
                options = list(sample_df['team'])

                correct_response = correct_row['team'].iloc[0]

                question_statement = statement.format(*correct_vals)
                question = self.question_template(question_statement, options, correct_response)
                q_lst.append(question)
        return q_lst

    def generate_player_shirt_number_question(self):
        df = self.generate_df(query_player_shirt_number)
        teamid = requests.get('https://fanareas.com/api/teams/generateId').json()
        sample_df = df[df['team_id'] == str(teamid)].sample(4)
        team = sample_df['team'].iloc[0]
        jersey_number = sample_df['jersey_number'].iloc[0]
        correct_response = sample_df['fullname'].iloc[0]
        options = list(sample_df['fullname'])
        random.shuffle(options)
        statement = f"Who currently plays for {team} under {jersey_number} jersey number?"
        question = self.question_template(statement, options, correct_response)
        return question

    def generate_player_2_clubs_question(self):

        df = self.generate_df(query_player_2_clubs_played)
        sample_df = df.drop_duplicates('player_id').sample(n=4)
        club1 = sample_df['transfer_from_team'].iloc[0]
        club2 = sample_df['team'].iloc[0]
        options = list(sample_df['fullname'])
        correct_response = sample_df.iloc[0]['fullname']
        statement = f"Which player played for {club1} and {club2} in his career?"
        question = self.question_template(statement, options, correct_response)
        return question

    def generate_team_stats_question(self):
        df = self.generate_df(query_team_stats)
        seasons = list(df['season'].unique())
        metrics = [
            'losses', 'wins', 'draws',
            'goals', 'goals_conceded',
            'yellow_cards', 'red_cards',
            'clean_sheets', 'corners'
        ]
        # check out dagster project linkedin
        dim = random.choice(metrics)
        season = random.choice(seasons)
        formatted_dim = self.format_metric(dim)
        correct_vals = (formatted_dim, season)
        dimension = f'{dim}_rn'
        sample_df = \
        df[df['season'] == season].drop_duplicates(subset=['season', dimension], keep='first').sort_values(dimension)[
            ['team', 'season', dimension]].head(4)

        correct_row = sample_df[sample_df[dimension] == 1]
        statement_team_stats = "Which team had the most {} in the {} season?"

        options = list(sample_df['team'])

        correct_response = correct_row['team'].iloc[0]

        question_statement = statement_team_stats.format(*correct_vals)
        question = self.question_template(question_statement, options, correct_response)
        return question

    def generate_venue_question(self):
        df = self.generate_df(query_capacity_venue)
        sample_df = df[~df['team'].isin(['Brentford', 'Swansea City', 'Tottenham Hotspur'])].sample(4)[
            ['team', 'venue']]
        correct_response = sample_df.iloc[0]['venue']
        correct_team = sample_df.iloc[0]['team']
        statement = f"What is the home venue of {correct_team}?"
        options = list(sample_df['venue'])
        question = self.question_template(statement, options, correct_response)
        return question

    def generate_founded_question(self):
        df = self.generate_df(query_capacity_venue)
        sample_df = df.sample(4).sort_values('founded_rn')
        correct_response = sample_df.iloc[0]['team']
        statement = f"Which team was founded first?"
        options = list(sample_df['team'])
        question = self.question_template(statement, options, correct_response)
        return question

    def generate_capacity_question(self):
        df = self.generate_df(query_capacity_venue)
        df['venue_city'] = df['venue'] + ' ' + '(' + df['city'] + ')'
        sample_df = df.sample(4).sort_values('capacity_rn')
        correct_response = sample_df.iloc[0]['venue_city']
        statement = f"Which stadium has higher capacity?"
        options = list(sample_df['venue_city'])
        question = self.question_template(statement, options, correct_response)
        return question

    def generate_fewest_points_question(self):
        df = self.generate_df(query_standings)
        sample_df = df.head(4)
        correct_response = sample_df.iloc[0]['team']
        statement = f"Which team finished the 2007/2008 season with 11 points?"
        options = list(sample_df['team'])
        question = self.question_template(statement, options, correct_response)
        return question

    def generate_most_points_question(self):
        df = self.generate_df(query_standings)
        sample_df = df.sort_values('points', ascending=False).head(30)
        correct_response = sample_df.iloc[0]['team']
        statement = f"Which team finished the 2017/2018 season with 100 points (English Premier League record)?"
        options = list(sample_df['team'].unique())[:4]
        question = self.question_template(statement, options, correct_response)
        return question

    def generate_relegations_question(self):
        df = self.generate_df(query_relegations)
        sample_df = df.sample(1)
        correct_response = sample_df.iloc[0]['team_promoted']
        season = sample_df.iloc[0]['season']
        options = sample_df.iloc[0]['options']
        statement = f"Which team did not relegate in the {season} season?"
        question = self.question_template(statement, options, correct_response)
        return question

    def mix_quiz_questions(self):
        random.shuffle(self.quiz_collection)
        # result_list = self.quiz_collection[:10]
        # return result_list
        return self.quiz_collection

    def post_quiz(self, questions, team_name, season_name, entityIdTeam, entityIdSeason, entityTypeTeam,
                  entityTypeSeason):
        json_data = self.quiz_template(questions,
                                       team_name,
                                       season_name,
                                       entityIdTeam,
                                       entityIdSeason,
                                       entityTypeTeam,
                                       entityTypeSeason)
        return post_json(json_data, self.url)
