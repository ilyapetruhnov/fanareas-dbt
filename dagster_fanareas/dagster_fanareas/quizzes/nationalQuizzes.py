import random
from dagster_fanareas.quizzes.queries import *
from dagster_fanareas.quizzes.tm_queries import national_champions_query, both_national_cups_query
from dagster_fanareas.quizzes.quizzes import Quizzes

class NationalTeamQuizzes(Quizzes):
    def __init__(self, title: str, description: str, quiz_type: int, is_demo: bool) -> None:
        super().__init__(title, description, quiz_type, is_demo)
        self.players = []
        self.quiz_collection = []
    
    def first_winner_question(self, title_name) -> dict:
        if title_name == 'euro':
            title = 'UEFA European Championship'
            options = ['Italy','Spain','Germany']
        else:
            title = 'FIFA World Cup'
            options = ['Argentina','Brazil','Germany']
        df = self.generate_df(national_champions_query.format(title)).sort_values('season')
        df = df.head(4)
        correct_response = df['country_name'].iloc[0]
        options.append(correct_response)
        season = df['season'].iloc[0]
        question_statement = f"Which country won the first {title} in {season}?"
        description = f"{correct_response} was the first team to win the {title}"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def most_title_question(self, title_name) -> dict:
        if title_name == 'euro':
            title = 'UEFA European Championship'
        else:
            title = 'FIFA World Cup'
        df = self.generate_df(national_champions_query.format(title))
        df = df.groupby('country_name').count().reset_index().sort_values('season',ascending=False).head(4)
        number = int(df['season'].iloc[0])
        correct_response = df['country_name'].iloc[0]
        options = [i for i in df['country_name']]
        question_statement = f"Which country has won the most {title} titles?"
        description = f"{correct_response} holds the record for the most {title} titles, having won the tournament {number} times"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def nation_first_title_question(self, title_name) -> dict:
        if title_name == 'euro':
            title = 'UEFA European Championship'
        else:
            title = 'FIFA World Cup'
        df = self.generate_df(national_champions_query.format(title))
        country_name = random.choice(df['country_name'].unique())
        ndf = df[df['country_name']==country_name].sort_values('season_id')
        correct_response = ndf['season'].iloc[0]
        df = df[df['season'] != correct_response]
        df['season'] = df['season'].astype('str')
        options = random.sample(list(df['season'].unique()),3)
        options.append(str(correct_response))
        question_statement = f"In which year did {country_name} win their first {title}?"
        description = f"{country_name} had their first {title} triumph in {correct_response}"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def year_single_time_winner_question(self, title_name) -> dict:
        if title_name == 'euro':
            title = 'UEFA European Championship'
        else:
            title = 'FIFA World Cup'
        df = self.generate_df(national_champions_query.format(title))
        ndf = df.groupby('country_name').count().reset_index().sort_values('season_id',ascending=False)
        countries = ndf[ndf['season_id']==1]['country_name'].unique()
        country_name = random.choice(countries)
        correct_response = df[df['country_name']==country_name]['season'].iloc[0]

        df = df[df['season'] != correct_response]

        options = [str(i) for i in df['season'].sample(3)]
        options.append(str(correct_response))

        question_statement = f"In which year did {country_name} win the {title}?"
        description = f"{country_name} won the {title} in {correct_response}"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def winner_coach_question(self, title_name) -> dict:
        if title_name == 'euro':
            title = 'UEFA European Championship'
        else:
            title = 'FIFA World Cup'
        df = self.generate_df(national_champions_query.format(title))
        df = df[df['season']>1985].sample(4)

        correct_response = df['coach_name'].iloc[0]
        country_name = df['country_name'].iloc[0]
        season = df['season'].iloc[0]

        options = [i for i in df['coach_name']]
        question_statement = f"Who was the coach of {country_name} when they won {title} {season}?"
        description = f"{correct_response} was the coach of {country_name} when they won the {title} {season}, leading the team to a historic victory"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def never_won_question(self, title_name) -> dict:
        if title_name == 'euro':
            title = 'UEFA European Championship'
            df = self.generate_df(national_champions_query.format(title))
            correct_response = random.choice(['England','Croatia','Switzerland','Turkey'])
            options = random.sample(list(df['country_name'].unique()),3)
        else:
            title = 'FIFA World Cup'
            df = self.generate_df(national_champions_query.format(title))
            correct_response = random.choice(['Portugal','Belgium','Netherlands','Japan'])
            options = random.sample(list(df['country_name'].unique()),3)
        options.append(correct_response)
        question_statement = f"Which team has never won the {title}"
        description = f"As of 2024, {correct_response} has never been the {title} champion"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def both_cups_winner_question(self) -> dict:
        df = self.generate_df(both_national_cups_query)
        grouped_df = df.groupby('country_name').agg({'success_id': 'nunique'}).reset_index().sort_values('success_id',ascending=False).head(4)
        correct_response = random.choice(grouped_df['country_name'].unique())
        options = random.sample(['Portugal','England','Netherlands','Denmark'],3)
        options.append(correct_response)
        description_df = df[df['country_name'] == correct_response]
        world_seasons = list(description_df[description_df['title'] == 'FIFA World Cup']['season'].unique())
        euro_seasons = list(description_df[description_df['title'] == 'UEFA European Championship']['season'].unique())
        world_str = ", ".join(str(x) for x in world_seasons)
        euro_str = ", ".join(str(x) for x in euro_seasons)
        question_statement = "Which country has won both FIFA World Cup and UEFA European Championship titles?"
        description = "{} is the country that has won both the FIFA World Cup and the UEFA European Championship titles. They won the World Cup in {}, and the European Championship in {}".format(correct_response, world_str, euro_str)
        question = self.question_template(question_statement, options, correct_response, description)
        return question
    
    def fill_quiz_with_questions(self):
        while len(self.quiz_collection) < 10:
            question = self.get_question()
            self.collect_questions(question)
            