import random
import time
import itertools
from dagster_fanareas.quizzes.queries import *
from dagster_fanareas.quizzes.tm_queries import player_query, top_value_players_query, played_for_multiple_clubs_query, player_transfers_over_fifty_million_query, most_stats_in_a_league_query, comparison_query, cards_combined_query, played_in_4_major_leagues, played_in_less_4_leagues_query, goalkeeper_stats_query, other_goalkeepers_query, own_goals_query, own_goals_options_query, player_position_performance_query, player_position_performance_options_query
from dagster_fanareas.constants import nationality_mapping
from dagster_fanareas.quizzes.quizzes import Quizzes

class PlayerQuizzes(Quizzes):
    def __init__(self, title: str, description: str, quiz_type: int, is_demo: bool) -> None:
        super().__init__(title, description, quiz_type, is_demo)
        self.players = []
        self.quiz_collection = []
        self.nationality_mapping = nationality_mapping
        self.functions = {
        "player_photo_question": self.player_photo_question,
        "player_national_team_and_club": self.player_national_team_and_club,
        "player_shirt_number": self.player_shirt_number,
        "player_position_question": self.player_position_question,
        "transfer_and_club_question": self.transfer_and_club_question,
        "player_top_league_stats": self.player_top_league_stats,
        "player_top_league_stats_comparison": self.player_top_league_stats_comparison,
        "goalkeeper_goals": self.goalkeeper_goals,
        "player_position_club_performance": self.player_position_club_performance,
        "played_for_multiple_clubs": self.played_for_multiple_clubs,
        "player_played_in_4_leagues": self.player_played_in_4_leagues,
        "player_played_in_all_major_leagues": self.player_played_in_all_major_leagues,
        "player_red_yellow_cards_combined": self.player_red_yellow_cards_combined,
        "record_transfer": self.record_transfer
    }

    def create_quiz(self):
        categories = {
        "player_shirt_number": 10,
        "player_played_in_4_leagues": 5,
        "player_national_team_and_club": 30,
        "player_photo_question": 55,
        "player_position_question": 30,
        "transfer_and_club_question": 40,
        "player_top_league_stats": 30,
        "player_top_league_stats_comparison": 10,
        "goalkeeper_goals": 5,
        "player_position_club_performance": 15,
        "played_for_multiple_clubs": 25,
        "player_played_in_all_major_leagues": 1,
        "player_red_yellow_cards_combined": 5,
        "record_transfer": 1,
    }
        while True:
            for category, num_questions in categories.items():
                questions = self.functions[category](num_questions)
                self.quiz_collection.extend(questions)
            break
        return self.quiz_collection

    
    def player_photo_question(self, q_num) -> list:
        df = self.generate_df(top_value_players_query)
        df = df.head(250)
        questions = []
        while q_num > 0:
            options_df = df.sample(4)
            correct_response = options_df['player_name'].iloc[0]
            if correct_response in self.players:
                continue
            self.players.append(correct_response)
            image_url = options_df['img'].iloc[0]
            q1 = "Who is on the photo?"
            q2 = "Which player is on the photo?"
            question_statement = random.choice([q1,q2])
            options = [i for i in options_df['player_name']]
            question = self.question_template(question_statement = question_statement,
                                              correct_response = correct_response,
                                              options = options, 
                                              image_url = image_url, 
                                              image_size = 1)
            questions.append(question)
            q_num -= 1
             
            if q_num == 0:
                break
        return questions
        
    def player_national_team_and_club(self, q_num) -> list:
        df = self.generate_df(top_value_players_query)
        questions = []
        df['nationality'] = df['international_team'].apply(lambda x: self.nationality_mapping[x])
        nationality_df = df.groupby('international_team').count().sort_values('id').reset_index()
        nationality_df = nationality_df[nationality_df['id']>3]
        countries = list(nationality_df['international_team'])
        for i in countries:
            if i == 'Croatia':
                country_df = df[df['international_team']==i]
                correct_response = country_df['player_name'].iloc[0]
                position_group = country_df['position_group'].iloc[0]
                international_team = country_df['international_team'].iloc[0]
                nationality = country_df['nationality'].iloc[0]
                team_name = country_df['team'].iloc[0]
                options = ['Josko Gvardiol', 'Luca Modric', 'Josip Stanisic', 'Lovro Majer']
            else:
                country_df = df[df['international_team']==i].drop_duplicates(subset='team').sample(4)
                correct_response = country_df['player_name'].iloc[0]
                league_name = country_df['league'].iloc[0]   
                position_group = country_df['position_group'].iloc[0]
                international_team = country_df['international_team'].iloc[0]
                nationality = country_df['nationality'].iloc[0]
                team_name = country_df['team'].iloc[0]
                options = [i for i in country_df['player_name']]

            question_statement1 = f"Which player represents {international_team} national team and played for {team_name} in the 23/24 season?"
            question_statement2 = f"A key player for the national team of {international_team}, he also played for {team_name} in the 23/24 season. Who is this player?"
            question_statement3 = f"Representing {international_team}, this {position_group} was part of {team_name} in the 23/24 season?. Who is he?"
            question_statement4 = f"A {nationality} {position_group}, who was playing at {team_name} in the 23/24 season. Who is this player?"

            question_statement = random.choice(
                [
                    question_statement1,
                    question_statement2,
                    question_statement3,
                    question_statement4
                ]
                                                )

            description = f"""{correct_response} represents {international_team}, he was in the {team_name} roster in the 23/24 {league_name} season"""
            question = self.question_template(question_statement, options, correct_response, description)
            questions.append(question)

        return questions
    
    def player_shirt_number(self, q_num) -> list:
        df = self.generate_df(top_value_players_query)
        time.sleep(5)
        questions = []
        while q_num > 0:
            shirt_number = random.choice(['9','10'])
            if shirt_number == '9':
                ndf = df[df['player_shirt_number']=='9']
                options_df = ndf.sample(4)
                correct_response = options_df['player_name'].iloc[0]
                if correct_response in self.players:
                    continue   
                team_name = options_df['team'].iloc[0]
                options = [i for i in options_df['player_name']]
                q1 = f"Who was the {team_name} number nine in the 23/24 season?"
                q2 = f"Who weared the number nine jersey for {team_name} in the 23/24 season?"
                question_statement = random.choice([q1, q2])

            else:
                ndf = df[df['player_shirt_number']=='10']
                options_df = ndf.sample(4)
                correct_response = options_df['player_name'].iloc[0]
                if correct_response in self.players:
                    continue   
                team_name = options_df['team'].iloc[0]
                options = [i for i in options_df['player_name']]
                q1 = f"Who was the {team_name} number ten in the 23/24 season?"
                q2 = f"Who weared the number ten jersey for {team_name} in the 23/24 season?"
                question_statement = random.choice([q1, q2])

            description = f"""{correct_response} played under number {shirt_number} for {team_name} in the 23/24 season"""
            question = self.question_template(question_statement, options, correct_response, description)
            questions.append(question)
            q_num -= 1
            if q_num == 0:
                break
        return questions


    def player_position_question(self, q_num) -> list:
        df = self.generate_df(top_value_players_query)
        time.sleep(5)
        questions = []
        while q_num > 0:
            ndf = df.groupby('position_group').apply(lambda x: x.sample(1)).reset_index(drop=True)
            selected_df = ndf.sample(1)
            correct_response = selected_df['player_name'].iloc[0]
            if correct_response in self.players:
                continue
            position_group = selected_df['position_group'].iloc[0]
            player_main_position = selected_df['player_main_position'].iloc[0]
            team_name = selected_df['team'].iloc[0]

            options_df = df[df['position_group'] == position_group]
            options_df = options_df[options_df['team'] != team_name].sample(3)

            options = [i for i in options_df['player_name']]
            options.append(correct_response)

            if position_group == 'midfielder':
                question_statement1 = f"Which midfielder played for {team_name} in the 23/24 season?"
                question_statement2 = f"Which palyer played as {player_main_position}er at {team_name} in the 23/24 season?"
                question_statement3 = f"Who occupied the {player_main_position} position at {team_name} in the 23/24 season?"
                question_statement4 = f"Which midfielder was on the {team_name} roster in the 23/24 season?"
            else:
                question_statement1 = f"Which {player_main_position} played for {team_name} in the 23/24 season?"
                question_statement2 = f"Which {player_main_position} was part of {team_name} squad in the 23/24 season?"
                question_statement3 = f"Who was the {team_name} {player_main_position} in the 23/24 season?"
                question_statement4 = f"Which {player_main_position} represented {team_name} in the 23/24 season?" 

            description = f"""{correct_response} played for {team_name} in 23/24 season"""
            question_statement = random.choice([question_statement1,question_statement2, question_statement3, question_statement4])
            question = self.question_template(question_statement, options, correct_response, description)
            questions.append(question)
            q_num -= 1
            if q_num == 0:
                break
        return questions
        

    def transfer_and_club_question(self, q_num) -> list:
        df = self.generate_df(player_transfers_over_fifty_million_query)
        time.sleep(5)
        questions = []
        countries = self.nationality_mapping.keys()
        df = df[df['international_team'].isin(countries)]
        df['nationality'] = df['international_team'].apply(lambda x: self.nationality_mapping[x])
        df['year'] = df['transfer_date'].apply(lambda x: x.year)
        df['month'] = df['transfer_date'].apply(lambda x: x.strftime('%B'))
        sample_df = df.sample(q_num)

        for i in range(len(sample_df)):
            correct_df = sample_df.iloc[i]
            correct_response = correct_df['player_name']
            options_df = df[df['player_name'] != correct_response].sample(3)
            options = [i for i in options_df['player_name']]
            options.append(correct_response)
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

            description = f"""{correct_response} transferred from {from_team} to {to_team} in {year} for a €{fee_value} million transfer fee"""
            question = self.question_template(question_statement, options, correct_response, description)
            questions.append(question)
        return questions
    
    def record_transfer(self, q_num) -> list:
        correct_response = 'Neymar'
        options = ['Zlatan Ibrahimović', 'Kylian Mbappé', 'Lionel Messi']
        options.append(correct_response)
        question_statement = "Which forward made a record-breaking transfer to Paris Saint-Germain from Barcelona in 2017?"
        description = f"""Neymar moved to Paris Saint-Germain (PSG) for a record €222 million, making it the highest transfer fee ever paid"""
        question = self.question_template(question_statement, options, correct_response, description)
        return [question]
    
    def played_for_multiple_clubs(self, q_num) -> list:
        questions = []
        query_df = self.generate_df(played_for_multiple_clubs_query)
        df = query_df.sample(q_num)
        selected_players = list(df['fullname'].unique())
        others_df = query_df[~query_df['fullname'].isin(selected_players)]
        countries = self.nationality_mapping.keys()
        df = df[df['international_team'].isin(countries)]
        df['nationality'] = df['international_team'].apply(lambda x: self.nationality_mapping[x])
        for i in range(len(df)):
            correct_df = df.iloc[i]
            options_df = others_df.sample(3)
            options = [i for i in options_df['fullname']]
            correct_response = correct_df['fullname']
            options.append(correct_response)
            position = correct_df['position_group']
            variables = correct_df['teams']
            if len(variables) == 3:
                q1 = "Which player played for  {}, {}, and {}?".format(*variables)
                q2 = "Who played for {}, {}, and {} in his career?".format(*variables)
                question_statement = random.choice([q1,q2])
                variables.insert(0, correct_response)
                description = "{} played for {}, {} and {}?".format(*variables)
            elif len(variables) == 2:
                variables.insert(0, position)
                q1 = "Which {}'s career includes playing for {} and {}?".format(*variables)
                q2 = "Which {} had a career at {} and {}?".format(*variables)
                q3 = "Which {} has graced the fields for {} and {}?".format(*variables)
                question_statement = random.choice([q1,q2,q3])
                variables.insert(0, correct_response)
                description = "{} has played for {} and {}?".format(*variables)
            else:
                q1 = "Who played for {}, {}, {} and {} in his career?".format(*variables)
                q2 = "Which player played for {}, {}, {} and {}?".format(*variables)
                q3 = "Which player has been a part of {}, {}, {} and {} in his career?".format(*variables)
                question_statement = random.choice([q1,q2,q3])
                variables.insert(0, correct_response)
                description = "{} has played for {}, {}, {} and {}?".format(*variables)
            question = self.question_template(question_statement, options, correct_response, description)
            questions.append(question)
        return questions
    
    def player_red_yellow_cards_combined(self, q_num) -> list:
        questions = []
        leagues = ['Premier League', 'LaLiga', 'Serie A','Ligue 1', 'Bundesliga']
        for league_name in leagues:
            df = self.generate_df(cards_combined_query.format(league_name))
            time.sleep(15)
            combined_cards = df['combined_cards'].iloc[0]
            red_cards = df['red_cards'].iloc[0]
            options = list(df['fullname'].unique())
            correct_response = options[0]
            question_statement = f"Who is the player with the highest record of combined total of yellow and red cards in {league_name}?"
            description = f"{correct_response} has received a total of {combined_cards} cards including {red_cards} red cards"
            question = self.question_template(question_statement, options, correct_response, description)
            questions.append(question)
        return questions

    def player_played_in_4_leagues(self, q_num) -> list:
        questions = []
        combinations = [
            ['LaLiga', 'Serie A','Ligue 1', 'Bundesliga'],
            ['Premier League', 'LaLiga', 'Serie A','Ligue 1'],
            ['Premier League', 'Serie A','Ligue 1', 'Bundesliga'],
            ['Premier League', 'LaLiga', 'Ligue 1', 'Bundesliga'],
            ['Premier League', 'LaLiga', 'Serie A','Bundesliga']
            ]
        for league_names in combinations:
            options_df = self.generate_df(played_in_less_4_leagues_query)
            time.sleep(5)
            correct_df = self.generate_df(played_in_4_major_leagues.format(*league_names))
            time.sleep(5)
            question_statement = "Which player played in the {}, {}, {}, and {}?".format(*league_names)
            correct_response = correct_df.sample(1)['fullname'].iloc[0]

            options = list(options_df.sample(3)['fullname'].unique())
            options.append(correct_response)
            variables = league_names
            variables.insert(0, correct_response)
            description = "{} played in 4 major European leagues including {}, {}, {} and {}".format(*variables)
            question = self.question_template(question_statement, options, correct_response, description)
            questions.append(question)
        return questions
    
    def player_played_in_all_major_leagues(self, q_num) -> list:
        question_statement = f"Which player has played in all 5 major european leagues: Premier League, La Liga, Serie A, Bundesliga and Ligue 1?"
        options = ['Justin Kluivert','Edinson Cavani','Hélder Postiga', 'Zlatan Ibrahimović']
        correct_response = 'Justin Kluivert'
        description = "Only Justin Kluivert played in 5 major european football leagues"
        question = self.question_template(question_statement, options, correct_response, description)
        return [question]
    
    def own_goals(self, q_num) -> list:
        questions = []
        df = self.generate_df(own_goals_query)
        for i in range(len(df)):
            correct_df = df.iloc[i]
            correct_response = correct_df['fullname']
            league_name = correct_df['league']
            own_goals = int(correct_df['own_goals'])
            goals = int(correct_df['goals'])
            options_df = self.generate_df(own_goals_options_query).sample(3)
            time.sleep(10)
            options = list(options_df['fullname'].unique())
            options.append(correct_response)
            q1 = f"Which player scored {own_goals} own goals in the {league_name}, while scoring just {goals} goals for his team?"
            q2 = f"This player scored more own goals ({own_goals}) than goals for his team ({goals}) in his {league_name} career"
            description = f"{correct_response} is well-known for having scored more own goals than regular goals"
            question_statement = random.choice([q1,q2])
            question = self.question_template(question_statement, options, correct_response, description)
            questions.append(question)
        return questions

    def player_position_club_performance(self, q_num) -> list:
        questions = []
        combinations = [
            ('Premier League', 'defender', 'assists'),
            ('Premier League', 'midfielder', 'goals'),
            ('Premier League', 'defender', 'goals'),
            ('LaLiga', 'defender', 'goals'),
            ('LaLiga', 'midfielder', 'assists'),
            ('Serie A', 'midfielder', 'goals'),
            ('Serie A', 'defender', 'assists'),
            ('Serie A', 'defender', 'goals'),
            ('Bundesliga', 'defender', 'goals'),
            ('Bundesliga', 'midfielder', 'goals')
        ]
        for i in combinations:
            time.sleep(15)
            league_name = i[0]
            position_group = i[1]
            metric =  i[2]

            if metric == 'goals':
                df = self.generate_df(player_position_performance_query.format(league_name, position_group, metric))
                goals = int(df['goals'].iloc[0])
                correct_response = df['fullname'].iloc[0]
                options_df = self.generate_df(player_position_performance_options_query.format(league_name, position_group, metric))
                options = list(options_df.sample(3)['fullname'].unique())
                options.append(correct_response)
                q1 = f"Who is the {league_name}'s highest-scoring {position_group}?"
                q2 = f"Who is the {league_name}'s all-time top-scoring {position_group}?"
                description = f"{correct_response} is a famously known {league_name} player who scored a record amount of goals for a {position_group}, with {goals} goals"
            elif metric == 'assists':
                df = self.generate_df(player_position_performance_query.format(league_name, position_group, metric))
                assists = int(df['assists'].iloc[0])
                correct_response = df['fullname'].iloc[0]
                options_df = self.generate_df(player_position_performance_options_query.format(league_name, position_group, metric))
                options = list(options_df.sample(3)['fullname'].unique())
                options.append(correct_response)
                q1 = f"Which {position_group} has provided the most assists in {league_name}'s history?"
                q2 = f"Who holds the record for the highest number of assists in {league_name} for a {position_group}?"
                description = f"{correct_response} is until today the {league_name} top-assistant with {assists} assists"
            question_statement = random.choice([q1,q2])
            question = self.question_template(question_statement, options, correct_response, description)
            questions.append(question)
            
        return questions
    
    def goalkeeper_goals(self, q_num) -> list:
        questions = []
        df = self.generate_df(goalkeeper_stats_query)
        for i in range(len(df)):
            correct_df = df.iloc[i]
            correct_response = correct_df['fullname']
            goals = int(correct_df['goals'])
            country = correct_df['nationality']
            nationality = self.nationality_mapping[country]
            options_df = self.generate_df(other_goalkeepers_query.format(correct_response, country)).sample(3)
            options = list(options_df.sample(3)['fullname'].unique())
            options.append(correct_response)
            question_statement = f"Which goalkeeper scored {goals} goals in his professional football career?"
            description = f"{nationality} goalkeeper {correct_response} scored {goals} goals during his career, including penalties and free kicks"
            question = self.question_template(question_statement, options, correct_response, description)
            questions.append(question)
        return questions
    
    def player_top_league_stats(self, q_num) -> list:
        questions = []
        leagues = ['Premier League', 'LaLiga', 'Serie A','Bundesliga','Ligue 1']
        metrics = ['goals','assists','yellow_cards','red_cards','appearances','own_goals']
        combinations = list(itertools.product(leagues, metrics))
        for i in combinations:
            league_name = i[0]
            metric =  i[1]
            df = self.generate_df(most_stats_in_a_league_query.format(league_name, metric))
            time.sleep(15)
            metric_num = int(df.iloc[0][metric])
            correct_response = df.iloc[0]['fullname']
            country = df.iloc[0]['nationality']
            position_group = df.iloc[0]['position_group']
            nationality = self.nationality_mapping[country]
            options = list(df.iloc[0:4]['fullname'].unique())
            if metric == 'goals':
                question_statement = f"Which player is the all-time topscorer in {league_name}?"
                description = f"{nationality} {position_group} {correct_response} holds a record of {metric_num} {metric} in {league_name}"
            elif metric == 'assists':
                question_statement = f"Who holds the record for the most assists in {league_name} history?"
                description = f"{nationality} {position_group} {correct_response} holds a record of {metric_num} {metric} in {league_name}"
            elif metric == 'yellow_cards':
                question_statement = f"Who has the most number of bookings (yellow cards) in {league_name}?"
                description = f"{nationality} {position_group} {correct_response} holds a record of {metric_num} yellow cards in {league_name}"
            elif metric == 'red_cards':
                question_statement = f"Which player has been sent off the most times in {league_name} history?"
                description = f"{nationality} {position_group} {correct_response} received {metric_num} red cards which is a record in {league_name}"
            elif metric == 'appearances':
                question_statement = f"Who holds the record for the most appearances in the {league_name}, with {metric_num} matches played?"
                description = f"{nationality} {position_group} {correct_response} played {metric_num} matches which is a record in {league_name}"
            elif metric == 'own_goals':
                question_statement = f"Which player holds the record for the most own goals in {league_name} history?"
                description = f"{nationality} {position_group} {correct_response} has scored {metric_num} own goals which is a record in {league_name}"
            question = self.question_template(question_statement, options, correct_response, description)
            questions.append(question)
            
        return questions
    
    def player_top_league_stats_comparison(self, q_num) -> list:
        questions = []
        leagues = ['Premier League', 'LaLiga', 'Serie A','Bundesliga','Ligue 1']
        metrics = ['goals','assists']
        combinations = list(itertools.product(leagues, metrics))

        for i in combinations:
            time.sleep(15)
            league_name = i[0]
            metric =  i[1]
            metric_top_limit = 100

            metric_options_top_limit = int(metric_top_limit - 15)
            metric_options_bottom_limit = int(metric_options_top_limit - 30)
            variables = [league_name, 
                        metric, 
                        metric_top_limit, 
                        league_name, 
                        metric, 
                        metric_options_top_limit, 
                        metric, 
                        metric_options_bottom_limit
                        ]
            df = self.generate_df(comparison_query.format(*variables))
            options = list(df['fullname'].unique())
            correct_response = df[df[metric]>metric_top_limit]['fullname'].iloc[0]
            goals = int(df[df[metric]>metric_top_limit]['goals'].iloc[0])
            assists = int(df[df[metric]>metric_top_limit]['assists'].iloc[0])
            position_group = df[df[metric]>metric_top_limit]['position_group'].iloc[0]
            country = df[df[metric]>metric_top_limit]['nationality'].iloc[0]
            nationality = self.nationality_mapping[country]
            if metric == 'goals':
                q1 = f"Which player scored more than {metric_top_limit} goals in {league_name}?"
                q2 = f"Which player is famous for scoring more than {metric_top_limit} goals during his {league_name} career?"
                description = f"{nationality} {position_group} {correct_response} has scored {goals} goals in {league_name}"
            elif metric == 'assists':
                q1 = f"Who is known for providing more than {metric_top_limit} assists in his {league_name} career?"
                q2 = f"Which player delivered more than {metric_top_limit} assists in his {league_name} career?"
                description = f"{nationality} {position_group} {correct_response} has delivered {assists} assists in {league_name}"
            question_statement = random.choice([q1,q2])
            question = self.question_template(question_statement, options, correct_response, description)
            questions.append(question)
        return questions
    
    def player_gif_quiz(self) -> list:
        questions = []
        options1 = [
            'Dejan Stankovich',
            'Wesley Sneijder',
            'Diego Milito',
            'Thiago Motta'
        ]

        options2 = [
            'Alessandro Del Piero',
            'Mauro Camoranesi',
            'Claudio Marchisio',
            'Sebastian Giovinco'
        ]

        options3 = [
            'Dennis Bergkamp',
            'Eric Cantona',
            'Roberto Baggio',
            'Gianfranco Zola'
        ]

        options4 = [
            "Juninho",
            'Alessandro Del Piero',
            'Andrea Pirlo',
            'David Beckham'
        ]

        options5 = [
            "Wayne Rooney",
            'Paul Scholes',
            'Carlos Tévez',
            'Michael Owen'
        ]

        # options6 = [
        #     'Aaron Ramsey',
        #     'Ilkay Gündogan',
        #     'Steven Gerrard',
        #     'Martin Ødegaard'
        # ]
        
        lst = [options1,options2,options3,options4,options5]
        question_statement = 'Who scored this goal?'
        for options in lst:
            correct_response = options[0]
            correct_url = ''.join(correct_response.split()[1:]).lower()
            image_url = f'/gifs/{correct_url}.gif'
            question = self.question_template(question_statement = question_statement,
                                              correct_response = correct_response,
                                              options = options, 
                                              image_url = image_url, 
                                              image_size = 4)
            questions.append(question)

        return questions

    

    




        
    