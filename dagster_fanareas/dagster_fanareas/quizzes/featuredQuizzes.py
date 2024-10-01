from dagster_fanareas.constants import league_mapping, ordinal_mapping, times_mapping, team_mapping
from dagster_fanareas.quizzes.featured_queries import top_player_stats_query, part_of_squad_query, position_played_query, standing_query, sent_off_query, own_goals_query, biggest_win_query, team_options_query, trophies_query, other_trophies_query, transfers_query, other_transfers_query
from dagster_fanareas.quizzes.quizzes import Quizzes
from dagster_fanareas.ops.utils import post_json
import random



class FeaturedQuizzes(Quizzes):
    def __init__(self, title: str, description: str, quiz_type: int, is_demo: bool) -> None:
        super().__init__(title, description, quiz_type, is_demo)
        self.quiz_collection = []

    def featured_quiz_template(self, quiz_title, quiz_description, questions, tags = None):
        
        json_data = {
            "title": quiz_title,
            "type": self.quiz_type,
            "description": quiz_description,
            "questions": questions,
            "isDemo": self.is_demo,
            "quizzTags": tags
                     }

        return json_data
    
    def post_featured_quiz(self, quiz_title, quiz_description, questions, tags):
        random.shuffle(questions)
        json_data = self.featured_quiz_template(quiz_title, quiz_description, questions, tags)
        return post_json(json_data, self.url)
    
    def collect_featured_quiz_questions(self, team_id, season_id):
        self.clear_collection()
        self.collect_questions(self.top_player_stats(team_id, season_id))
        self.collect_questions(self.part_of_squad(team_id, season_id))
        self.collect_questions(self.player_position(team_id, season_id))
        self.collect_questions(self.team_standing(team_id, season_id))
        self.collect_questions(self.sent_off(team_id, season_id))
        self.collect_questions(self.own_goals(team_id, season_id))
        self.collect_questions(self.trophies_won(team_id, season_id))
        self.collect_questions(self.biggest_win(team_id, season_id))
        self.collect_questions(self.transfers(team_id, season_id))
        return True

    def create_quiz(self, team_id, season_id):
        team_name = team_mapping[team_id]
        season_name = f"{season_id}/{season_id+1}"
        quiz_title = f"{team_name} in {season_name} season quiz"
        quiz_description = f"5-question quiz on {team_name} in {season_name} season"
        self.collect_featured_quiz_questions(team_id, season_id)
        questions = random.sample(self.quiz_collection, 5)
        tags = self.quiz_tags(
            team_name, 
            season_name, 
            entityIdTeam=team_id, 
            entityIdSeason=season_id,
            entityTypeTeam=1,
            entityTypeSeason=2
                              )
        return self.post_featured_quiz(quiz_title, quiz_description, questions, tags = tags)

    def top_player_stats(self, team_id, season_id):
        questions = []
        df = self.generate_df(top_player_stats_query.format(team_id, season_id))
        metrics = ['goals','matches_coming_off_the_bench']
        team_name = df['team_arr'].iloc[0][0]
        league_name = df['league_name'].iloc[0]
        season_name = f"{season_id}/{season_id+1}"

        for i in metrics:
            correct_df = df.sort_values(i, ascending=False).head(4)[['fullname','appearances',i]]
            correct_response = correct_df['fullname'].iloc[0]
            metric_num = int(correct_df.iloc[0][i])
            # num_appearances = int(correct_df.iloc[0]['appearances'])
            options = list(correct_df['fullname'].unique())
            if i == 'goals':
                question_statement = f"Who was {team_name} top striker during the {season_name} season?"
                description = f"{correct_response} was {team_name} top scorer that season, scoring {metric_num} goals in the {league_name}"
            elif i == 'matches_coming_off_the_bench':
                question_statement = f"Which player made the most appearances coming off the bench for {team_name} in the {season_name} season?"
                description = f"{correct_response} was known for his role as a super-sub during that season, being brought on in {metric_num} matches to change the dynamic of games"
            # elif i == 'yellow_cards':
            #     question_statement = f"Which {team_name} player received the most bookings (yellow cards) during the {season_name} season?"
            #     description = f"{correct_response} has often been on the referee's radar. He received {metric_num} yellow cards in his {num_appearances} {league_name} appearances"
            question = self.question_template(question_statement, options, correct_response, description)
            questions.append(question)
        return questions
    
    
    def part_of_squad(self, team_id, season_id):
        season_prev = season_id - 2
        df = self.generate_df(part_of_squad_query.format(team_id, season_id, team_id, season_prev, season_id ))
        team_name = df[df['current_player']==True]['team_name'].iloc[0]
        other_team_name = df[df['current_player']==False]['team_name'].iloc[0]
        season_name = f"{season_id}/{season_id+1}"

        correct_response = df[df['current_player']==False]['name'].iloc[0]
        options = list(df['name'].unique())

        question_statement = f"Which player was not in {team_name} squad during the {season_name} season?"
        description = f"""{correct_response} left {team_name} for {other_team_name} before the start of the {season_name} season"""
        question = self.question_template(question_statement, options, correct_response, description)
        return [question]
    
    def player_position(self, team_id, season_id):
        df = self.generate_df(position_played_query.format(team_id, season_id))
        team_name = df['team_name'].iloc[0]
        season_name = f"{season_id}/{season_id+1}"
        correct_df = df.sample(1)
        correct_response = correct_df['fullname'].iloc[0]
        correct_position = correct_df['position'].iloc[0]
        correct_position_group = correct_df['position_group'].iloc[0]
        options = list(df['fullname'].unique())
        question_statement = f"Who played as a {correct_position} for {team_name} during the {season_name} season?"
        description = f"""{correct_response} was a key {correct_position_group} for Arsenal in the {season_name} season"""
        question = self.question_template(question_statement, options, correct_response, description)
        return [question]
    
    def team_standing(self, team_id, season_id):
        df = self.generate_df(standing_query.format(team_id, season_id))
        df['rnk'] = df['rank'].apply(lambda x: ordinal_mapping[x])
        df['league_name'] = df['league_id'].apply(lambda x: league_mapping[x])
        points = df['points'].iloc[0]
        team_name = df['club_name'].iloc[0]
        league_name = df['league_name'].iloc[0]
        correct_response = df['rnk'].iloc[0]
        season_name = f"{season_id}/{season_id+1}"
        option_lst = ["1st","2nd","3rd","4th","5th","6th","7th","8th"]
        option_lst.remove(correct_response)
        options = random.sample(option_lst, 3)
        options.append(correct_response)
        question_statement = f"What position did {team_name} finish in the {league_name} during the {season_name} season?"
        if correct_response == '1st':
            description = f"""{team_name} finished {correct_response} in the {league_name}, winning the title with {points} points"""
        else:
            description = f"""{team_name} finished {correct_response} in the {league_name} with {points} points"""

        question = self.question_template(question_statement, options, correct_response, description)
        return [question]
    
    def sent_off(self, team_id, season_id):
        df = self.generate_df(sent_off_query.format(team_id, season_id, team_id, season_id))
        if df.empty:
            return None
        df['total_reds'] = df['second_yellow_cards'] + df['red_cards']
        correct_df = df[df['sent_off'] == True]
        correct_response = correct_df['fullname'].iloc[0]
        total_reds = int(correct_df['total_reds'].iloc[0])
        times = times_mapping[total_reds]
        options = list(df['fullname'].unique())
        team_name = df['team_name'].iloc[0]
        league_name = df['league_name'].iloc[0]

        season_name = f"{season_id}/{season_id+1}"
        question_statement = f"Which {team_name} player was sent off during a {league_name} match in the {season_name} season?"
        description = f"""{correct_response}. He was sent off {times} during the {season_name} season"""
        question = self.question_template(question_statement, options, correct_response, description)
        return [question]
    
        
    def own_goals(self, team_id, season_id):
        df = self.generate_df(own_goals_query.format(team_id, season_id, team_id, season_id))
        correct_df = df[df['own_goal_scored'] == True]
        if correct_df.empty:
            return None
        correct_response = correct_df['fullname'].iloc[0]
        total_own_goals = int(correct_df['own_goals'].iloc[0])
        position = correct_df['position_group'].iloc[0]
        options = list(df['fullname'].unique())

        team_name = df['team_name'].iloc[0]
        league_name = df['league_name'].iloc[0]
        season_name = f"{season_id}/{season_id+1}"

        question_statement = f"Which {team_name} player scored an own goal during a {league_name} match in the {season_name} season?"
        if total_own_goals == 1:
            description = f"""{team_name} {position} {correct_response} scored {total_own_goals} own goal during the {season_name} season"""
        else:
            description = f"""{team_name} {position} {correct_response} scored {total_own_goals} own goals during the {season_name} season"""
        question = self.question_template(question_statement, options, correct_response, description)
        return [question]
    

    def biggest_win(self, team_id, season_id):
        correct_df = self.generate_df(biggest_win_query.format(season_id, team_id))
        if correct_df.empty:
            return None
        correct_response = correct_df['loser_team_name'].iloc[0]
        winner_team = correct_df['winner_team_name'].iloc[0]
        score = str(correct_df['final_score'].iloc[0])
        league_name = correct_df['competition_name'].iloc[0]

        league_id = [k for k, v in league_mapping.items() if v == league_name][0]
        options_df = self.generate_df(team_options_query.format(league_id, season_id))
        option_lst = list(options_df['club_name'].unique())
        option_lst.remove(correct_response)
        options = random.sample(option_lst, 3)
        options.append(correct_response)

        season_name = f"{season_id}/{season_id+1}"
        question_statement = f"{winner_team} secured their largest {league_name} win of the season with a {score} scoreline. Which team did they defeat by this margin?"
        description = f"""The biggest win of the {season_name} season came against {correct_response}, with an emphatic {score} victory"""
        question = self.question_template(question_statement, options, correct_response, description)
        return [question]
    
    def trophies_won(self, team_id, season_id):
        correct_df = self.generate_df(trophies_query.format(team_id, season_id))
        team_name = team_mapping[team_id]
        if correct_df.empty:
            correct_response = 'None'
        else:
            correct_response = ', '.join(map(str, correct_df['trophies'].iloc[0]))

        other_df = self.generate_df(other_trophies_query.format(team_id))
        b = list(set(other_df['trophies'].iloc[0]))
        b.append('None')

        if correct_response == 'None':
            b.remove(correct_response)
        else:
            b.remove(correct_response)
            b.remove('None')

        random.shuffle(b)

        n1 = random.randint(1, min(3, len(b) - 3))  # Ensure at least 2 elements are left for the other arrays
        n2 = random.randint(1, min(3, len(b) - n1 - 2))  # Ensure at least 1 element is left for the third array
        # n3 = random.randint(1, min(3, len(b) - n1))
        # The number of elements for the third array is whatever is left
        n3 = len(b) - (n1 + n2)

        # Split the shuffled array into three unique arrays
        array1 = b[:n1]
        array2 = b[n1:n1 + n2]
        array3 = b[n1 + n3:]

        option1 = ', '.join(map(str, array1))
        option2 = ', '.join(map(str, array2))
        option3 = ', '.join(map(str, array3))

        options = [option1, option2, option3, correct_response]
        season_name = f"{season_id}/{season_id+1}"
        question_statement = f"Which of the following trophies did {team_name} win during the {season_name} season?"
        if correct_response == 'None':
            description = f"{team_name} did not win any major trophies during the {season_name} season"
        else:
            description = f"""{correct_response}. {team_name} did not win any other major trophies that season"""
        question = self.question_template(question_statement, options, correct_response, description)
        return [question]
    
    def transfers(self, team_id, season_id):
        season = str(season_id)[-2:]
        season_1 = str(season_id+1)[-2:]
        season_name = f"{season}/{season_1}"

        correct_df = self.generate_df(transfers_query.format(team_id))
        if season_name not in correct_df['season'].unique():
            return None
        competition_id = correct_df['to_competition_id'].iloc[0]
        others_df = self.generate_df(other_transfers_query.format(team_id, season_name, competition_id))
        correct_response = random.choice(list(correct_df[correct_df['season'] == season_name]['player_name']))
        options = random.sample(list(others_df['player_name'].unique()),3)
        options.append(correct_response)
        fee_value = int(correct_df[correct_df['player_name']==correct_response]['transfer_fee_value'].iloc[0])/1000000
        team_name = correct_df['to_team_name'].iloc[0]
        from_team = correct_df[correct_df['season'] == season_name]['from_team_name'].iloc[0]

        question_statement = f"Which of the following players joined {team_name} during the {season_name} season?"
        description = f"""{correct_response} joined {team_name} from {from_team} for a €{fee_value} million transfer fee"""
        question = self.question_template(question_statement, options, correct_response, description)
        return [question]
    






