import random
from dagster_fanareas.quizzes.queries import *
from dagster_fanareas.quizzes.tm_queries import player_query, top_value_players_query, played_for_multiple_clubs_query, player_transfers_over_million_query, most_stats_in_a_league_query, comparison_query, cards_combined_query, played_in_4_major_leagues, played_in_less_4_leagues_query, goalkeeper_stats_query, other_goalkeepers_query, own_goals_query, own_goals_options_query, player_position_performance_query, player_position_performance_options_query
from dagster_fanareas.constants import nationality_mapping
from dagster_fanareas.quizzes.quizzes import Quizzes

class PlayerQuizzes(Quizzes):
    def __init__(self, title: str, description: str, quiz_type: int, is_demo: bool) -> None:
        super().__init__(title, description, quiz_type, is_demo)
        self.players = []
        self.quiz_collection = []
        self.nationality_mapping = nationality_mapping
    
    def player_photo_question(self) -> dict:
        df = self.generate_df(player_query)
        df = df.head(30)
        try:
            options_df = df.sample(4)
            correct_response = options_df['player_name'].iloc[0]
            question_statement = options_df['img'].iloc[0]
            question_statement = "What is the name of this player?" # to be adjusted
            options = [i for i in options_df['player_name']]
            question = self.question_template(question_statement, options, correct_response)
            return question
        except Exception:
            return None
        
    def player_national_team_and_club(self) -> dict:
        df = self.generate_df(top_value_players_query)
        countries = self.nationality_mapping.keys()
        df = df[df['international_team'].isin(countries)]
        ndf = df.groupby('international_team').apply(lambda x: x.sample(1)).reset_index(drop=True)
        ndf['nationality'] = ndf['international_team'].apply(lambda x: self.nationality_mapping[x])
        options_df = ndf.sample(4)
        correct_response = options_df['player_name'].iloc[0]
        position_group = options_df['position_group'].iloc[0]
        international_team = options_df['international_team'].iloc[0]
        nationality = options_df['nationality'].iloc[0]
        team_name = options_df['team'].iloc[0]
        options = [i for i in options_df['player_name']]

        question_statement1 = f"Which player represents {international_team} national team and currently plays for {team_name}?"
        question_statement2 = f"A key player for the national team of {international_team}, he also plays for {team_name}. Who is this player?"
        question_statement3 = f"Representing {international_team}, this {position_group} plays for {team_name}. Who is he?"
        question_statement4 = f"A {nationality} {position_group}, who is now playing at {team_name}. Who is this player?"
        question_statement5 = f"Which {nationality} {position_group} plays for {team_name}?"

        question_statement = random.choice([question_statement1,
                                            question_statement2, 
                                            question_statement3, 
                                            question_statement4, 
                                            question_statement5])

        question = self.question_template(question_statement, options, correct_response)
        return question
    
    def player_shirt_number(self) -> dict:
        df = self.generate_df(top_value_players_query)
        shirt_number = random.choice(['9','10'])
        if shirt_number == '9':
            ndf = df[df['player_shirt_number']=='9']
            options_df = ndf.sample(4)
            correct_response = options_df['player_name'].iloc[0]
            team_name = options_df['team'].iloc[0]
            options = [i for i in options_df['player_name']]
            question_statement = f"Who is the {team_name} number nine?"
        else:
            ndf = df[df['player_shirt_number']=='10']
            options_df = ndf.sample(4)
            correct_response = options_df['player_name'].iloc[0]
            team_name = options_df['team'].iloc[0]
            options = [i for i in options_df['player_name']]
            question_statement = f"Who is the {team_name} number ten?"

        question = self.question_template(question_statement, options, correct_response)
        return question


    def player_position_question(self) -> dict:
        df = self.generate_df(top_value_players_query)
        ndf = df.groupby('position_group').apply(lambda x: x.sample(1)).reset_index(drop=True)
        options_df = ndf.sample(4)

        position_group = options_df['position_group'].iloc[0]
        player_main_position = options_df['player_main_position'].iloc[0]
        team_name = options_df['team'].iloc[0]
        options = [i for i in options_df['player_name']]
        correct_response = options_df['player_name'].iloc[0]

        if position_group == 'midfielder':
            question_statement1 = f"Which {position_group}er, known for his playmaking abilities and vision, plays for {team_name}?"
            question_statement2 = f"Which palyer plays as {player_main_position}er at {team_name}?"
            question_statement3 = f"Who occupies the {player_main_position} position at {team_name}?"
            question_statement4 = f"Which midfielder is on the {team_name} roster?"
        else:
            question_statement1 = f"Which {player_main_position} plays for {team_name}?"
            question_statement2 = f"Which {player_main_position} is part of {team_name} squad?"
            question_statement3 = f"Who is the {team_name} {player_main_position}?"
            question_statement4 = f"Which {player_main_position} represents {team_name}?" 

        question_statement = random.choice([question_statement1,question_statement2, question_statement3, question_statement4])
        question = self.question_template(question_statement, options, correct_response)
        return question
        

    def transfer_and_club_question(self) -> dict:
        df = self.generate_df(player_transfers_over_million_query)
        countries = self.nationality_mapping.keys()
        df = df[df['international_team'].isin(countries)]
        df['nationality'] = df['international_team'].apply(lambda x: self.nationality_mapping[x])
        ndf = df.sample(4)
        ndf['year'] = ndf['transfer_date'].apply(lambda x: x.year)
        ndf['month'] = ndf['transfer_date'].apply(lambda x: x.strftime('%B'))
        options = [i for i in ndf['player_name']]
        correct_df = ndf.iloc[0]
        correct_response = correct_df['player_name']
        fee_value = int(correct_df['transfer_fee_value']/1000000)
        season = correct_df['season']
        nationality = correct_df['nationality']
        year = correct_df['year']
        month = correct_df['month']
        from_team = correct_df['from_team']
        to_team = correct_df['to_team']
        position_group = correct_df['position_group']
        q_statements = [
        f"Which player moved from {from_team} to {to_team} in the {season} season for a €{fee_value} million transfer fee?",
        f"Which {nationality} {position_group} joined {to_team} in the {season} season?",
        f"Who left {from_team} and joined {to_team} in {year}?",
        f"Which {position_group} transferred to {to_team} from {from_team} in {year}?",
        f"Which {position_group} transitioned from {from_team} to {to_team} in {month} {year}",
        f"Who moved from {from_team} to {to_team} in {month} {year}?",
        f"Which {nationality} {position_group} transferred from {from_team} to {to_team} in {year} for a reported fee of €{fee_value} million?",
        f"Which {nationality} {position_group} joined {to_team} in {year} after playing for {from_team}?",
        f"Which {position_group} made the move from {from_team} to {to_team} in {month} {year}?",
        f"Who transferred from {from_team} to {to_team} in {year} for a €{fee_value} million transfer fee?"
        ]
        question_statement = random.choice(q_statements)
        question = self.question_template(question_statement, options, correct_response)
        return question
    
    def record_transfer(self) -> dict:
        correct_response = 'Neymar'
        options = ['Zlatan Ibrahimović', 'Kylian Mbappé', 'Lionel Messi']
        options.append(correct_response)
        question_statement = "Which forward made a record-breaking transfer to Paris Saint-Germain from Barcelona in 2017?"
        description = f"""Neymar moved to Paris Saint-Germain (PSG) for a record €222 million, making it the highest transfer fee ever paid"""
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def played_for_multiple_clubs(self) -> dict:
        df = self.generate_df(played_for_multiple_clubs_query)
        countries = self.nationality_mapping.keys()
        df = df[df['international_team'].isin(countries)]
        df['nationality'] = df['international_team'].apply(lambda x: self.nationality_mapping[x])
        ndf = df.sample(4)
        options = [i for i in ndf['fullname']]
        correct_df = ndf.iloc[0]
        correct_response = correct_df['fullname']
        nationality = correct_df['nationality']
        position = correct_df['position_group']
        variables = correct_df['teams']
        if len(variables) == 3:
            variables.insert(0, nationality)
            q1 = "Which {} international has played for  {}, {}, and {}?".format(*variables)
            q2 = "Which {} player has played for {}, {}, and {}?".format(*variables)
            q3 = "Who represents {} national team and also played for {}, {}, and {}?".format(*variables)
            question_statement = random.choice([q1,q2,q3])
        elif len(variables) == 2:
            variables.insert(0, nationality)
            variables.insert(1, position)
            q1 = "Which {} {}'s career includes playing for {} and {}?".format(*variables)
            q2 = "Which {} {} has had a career at {} and {}?".format(*variables)
            q3 = "Which {} {} has graced the fields for {} and {}?".format(*variables)
            question_statement = random.choice([q1,q2,q3])
        else:
            q1 = "Who played for {}, {}, {} and {} in his career?".format(*variables)
            q2 = "Which player has played for {}, {}, {} and {}?".format(*variables)
            q3 = "Which player has been a part of {}, {}, {} and {}?".format(*variables)
            question_statement = random.choice([q1,q2,q3])
        question = self.question_template(question_statement, options, correct_response)
        return question
    
    def player_red_yellow_cards_combined(self, league_name) -> dict:
        df = self.generate_df(cards_combined_query.format(league_name))
        options = list(df['fullname'].unique())
        correct_response = options[0]
        question_statement = f"Who is the player with the highest record of combined total of yellow and red cards in {league_name}?"
        question = self.question_template(question_statement, options, correct_response)
        return question

    def player_played_in_4_leagues(self) -> dict:
        leagues = ['Premier League', 'LaLiga', 'Serie A','Ligue 1', 'Bundesliga']
        league_names = random.sample(leagues, 4)
        options_df = self.generate_df(played_in_less_4_leagues_query)
        correct_df = self.generate_df(played_in_4_major_leagues.format(*league_names))
        question_statement = "Which player has played in the {}, {}, {}, and {}?".format(*league_names)
        correct_response = correct_df.sample(1)['fullname'].iloc[0]
        options = list(options_df.sample(3)['fullname'].unique())
        options.append(correct_response)
        question = self.question_template(question_statement, options, correct_response)
        return question
    
    def player_played_in_all_major_leagues(self) -> dict:
        question_statement = f"Which player has played in all 5 major european leagues: Premier League, La Liga, Serie A, Bundesliga and Ligue 1?"
        options = ['Justin Kluivert','Edinson Cavani','Hélder Postiga', 'Zlatan Ibrahimović']
        correct_response = 'Justin Kluivert'
        question = self.question_template(question_statement, options, correct_response)
        return question

    def player_position_club_performance(self, league_name, position_group, metric) -> dict:
        if metric == 'own_goals':
            df = self.generate_df(own_goals_query.format(league_name))
            own_goals = int(df['own_goals'].iloc[0])
            goals = int(df['goals'].iloc[0])
            correct_response = df['fullname'].iloc[0]
            options_df = self.generate_df(own_goals_options_query.format(league_name)).sample(3)
            options = list(options_df['fullname'].unique())
            options.append(correct_response)
            q1 = f"Which player scored {own_goals} own goals in the {league_name}, while scoring just {goals} goals for his team?"
            q2 = f"This player scored more own goals ({own_goals}) than goals for his team ({goals}) in his {league_name} career"
        elif metric == 'goals':
            df = self.generate_df(player_position_performance_query.format(league_name, position_group, metric))
            goals = int(df['goals'].iloc[0])
            correct_response = df['fullname'].iloc[0]
            options_df = self.generate_df(player_position_performance_options_query.format(league_name, position_group, metric))
            options = list(options_df.sample(3)['fullname'].unique())
            options.append(correct_response)
            q1 = f"Who is the {league_name}'s highest-scoring {position_group}, with {goals} goals?"
            q2 = f"Who is the {league_name}'s all-time top-scoring {position_group},  with {goals} goals?"
        elif metric == 'assists':
            df = self.generate_df(player_position_performance_query.format(league_name, position_group, metric))
            assists = int(df['assists'].iloc[0])
            correct_response = df['fullname'].iloc[0]
            options_df = self.generate_df(player_position_performance_options_query.format(league_name, position_group, metric))
            options = list(options_df.sample(3)['fullname'].unique())
            options.append(correct_response)
            q1 = f"Which {position_group} has provided the most in assists in {league_name}'s history?"
            q2 = f"Who holds the record for the highest number of assists in {league_name} for a {position_group},  with {assists} assists?"
        question_statement = random.choice([q1,q2])
        question = self.question_template(question_statement, options, correct_response)
        return question
    
    def goalkeeper_goals(self) -> dict:
        correct_df = self.generate_df(goalkeeper_stats_query).sample(1)
        correct_response = correct_df['fullname'].iloc[0]
        goals = int(correct_df['goals'].iloc[0])
        country = correct_df['nationality'].iloc[0]
        nationality = self.nationality_mapping[country]
        options_df = self.generate_df(other_goalkeepers_query.format(correct_response, country)).sample(3)
        options = list(options_df.sample(3)['fullname'].unique())
        options.append(correct_response)
        question_statement = f"Which goalkeeper scored {goals} goals in his professional football career?"
        description = f"{nationality} goalkeeper {correct_response} scored {goals} goals during his career, including penalties and free kicks"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def player_top_league_stats(self, league_name, metric) -> dict:
        df = self.generate_df(most_stats_in_a_league_query.format(league_name, metric))
        metric_num = int(df.iloc[0][metric])
        correct_response = df.iloc[0]['fullname']
        options = list(df.iloc[0:4]['fullname'].unique())
        if metric == 'goals':
            question_statement = f"Which player is the all-time topscorer in {league_name}?"
        elif metric == 'assists':
            question_statement = f"Who holds the record for the most assists ({metric_num}) in {league_name} history?"
        elif metric == 'yellow_cards':
            question_statement = f"Who has the most number of bookings in {league_name}, having a record of {metric_num} yellow cards?"
        elif metric == 'red_cards':
            question_statement = f"Which player has been sent off the most times in {league_name} history, having a record of {metric_num} red cards?"
        elif metric == 'appearances':
            question_statement = f"Who holds the record for the most appearances in the {league_name}, with {metric_num} matches played?"
        elif metric == 'own_goals':
            question_statement = f"Which player holds the record for the most own goals in {league_name} history, having scored {metric_num} own goals?"
        question = self.question_template(question_statement, options, correct_response)
        return question
    
    def player_top_league_stats_comparison(self, league_name, metric, metric_top_limit) -> dict:
        metric_options_top_limit = int(metric_top_limit - 15)
        metric_options_bottom_limit = int(metric_options_top_limit - 30)
        variables = [league_name, 
                     metric, metric_top_limit, 
                     league_name, metric, 
                     metric_options_top_limit, 
                     metric, 
                     metric_options_bottom_limit
                     ]
        if metric == 'goals':
            q1 = f"Which player scored more than {metric_top_limit} in {league_name}?"
            q2 = f"Which player is famous for scoring more than {metric_top_limit} during his {league_name} career?"
        elif metric == 'assists':
            q1 = f"Who is known for providing more than {metric_top_limit} assists in his {league_name} career?"
            q2 = f"Which player delivered more than {metric_top_limit} assists in his {league_name} career?"
        df = self.generate_df(comparison_query.format(*variables))
        options = list(df['fullname'].unique())
        correct_response = df[df[metric]>metric_top_limit]['fullname'].iloc[0]
        question_statement = random.choice([q1,q2])
        question = self.question_template(question_statement, options, correct_response)
        return question 

    

    




        
    