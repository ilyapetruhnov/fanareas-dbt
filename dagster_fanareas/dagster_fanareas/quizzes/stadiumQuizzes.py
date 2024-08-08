import random
from dagster_fanareas.quizzes.queries import *
from dagster_fanareas.quizzes.tm_queries import team_query
from dagster_fanareas.quizzes.quizzes import Quizzes

class StadiumQuizzes(Quizzes):
    def __init__(self, title: str, description: str, quiz_type: int, is_demo: bool) -> None:
        super().__init__(title, description, quiz_type, is_demo)
        self.players = []
        self.quiz_collection = []
    
    def stadium_photo_question(self) -> dict:
        df = self.generate_df(team_query)
        df = df.head(30)
        try:
            options_df = df.sample(4)
            correct_response = options_df['stadium_name'].iloc[0]
            question_statement = options_df['image_path'].iloc[0]
            question_statement = "What is the name of this stadium?" # to be adjusted
            options = [i for i in options_df['stadium_name']]
            image_url = options_df['stadium_image'].iloc[0]
            question = self.question_template(question_statement, options, image_url, correct_response)
            return question
        except Exception:
            return None
    
    def home_stadium_question(self, league_name) -> dict:
        df = self.generate_df(team_query)
        leagues = ['Bundesliga','Premier League','Serie A','LaLiga','Ligue 1']
        #league_name = random.choice(leagues)
        ndf = df[df['league_name']==league_name]
        gr = ndf.groupby(['stadium_name','team_name']).max('total_capacity').reset_index().sort_values('total_capacity',ascending=False).head(6)
        correct_df = gr.sample(1)
        correct_response = correct_df['stadium_name'].iloc[0]
        team_name = correct_df['team_name'].iloc[0]
        options_df = gr[gr['stadium_name']!=correct_response]
        options = random.sample(list(options_df['stadium_name'].unique()),3)
        options.append(correct_response)
        question_statement1 = f"What is the official name of the stadium where {team_name} plays its home games?"
        question_statement2 = f"What is the name of the home venue of {team_name}?"
        question_statement = random.choice([question_statement1,question_statement2])
        question = self.question_template(question_statement, options, correct_response)
        return question
    
    def stadium_city_question(self) -> dict:
        df = self.generate_df(team_query)
        df = df.head(30)
        correct_df = df.sample(1)
        correct_response = correct_df['city'].iloc[0]
        stadium_name = correct_df['stadium_name'].iloc[0]
        options_df = df[df['city']!=correct_response]
        options = random.sample(list(options_df['city'].unique()),3)
        options.append(correct_response)
        question_statement = f"In which city is the {stadium_name} Stadium located?"
        question = self.question_template(question_statement, options, correct_response)
        return question
    
    def specific_team_stadium_question(self, q: int) -> dict:
        option_lst = [
            'Camp Nou',
            'Santiago Bernabéu',
            'Allianz Arena',
            'Old Trafford',
            'Olimpico di Roma',
            'Civitas Metropolitano',
            'Orange Vélodrome',
            'London Stadium',
            'Veltins-Arena',
            'Anfield',
            'Benito Villamarín',
            'Emirates Stadium',
            'San Nicola'
                    ]
        question_statement1 = {"Which stadium is famously known for its 'Yellow Wall'?": 'Signal Iduna Park'}
        question_statement2 = {"Which English stadium is known as 'The Home of Football'?":'Wembley Stadium'}
        question_statement3 = {"Which stadium is built on a hill and is home to Athletic Bilbao?" : 'San Mamés'}
        question_statement4 = {"What is the name of the stadium that hosts the annual FA Cup final in England?":'Wembley Stadium'}
        question_statement5 = {"What is the oldest stadium in England?": 'Bramall Lane'}
        question_statement6 = {"Which stadium located in Milan is shared by two major Italian football clubs?":'San Siro'}
        # question_dict = random.choice(
        #     [
        #         question_statement1,
        #         question_statement2,
        #         question_statement3,
        #         question_statement4,
        #         question_statement5,
        #         question_statement6
        #     ]
        #         )
        #for question_statement, correct_response in question_dict.items():
        if q == 1:
            question = question_statement1
        elif q == 2:
            question = question_statement2
        elif q == 3:
            question = question_statement3
        elif q == 4:
            question = question_statement4
        elif q == 5:
            question = question_statement5
        elif q == 6:
            question = question_statement6
        correct_response = str(question.value())
        question_statement = str(question.key())
        options = random.sample(option_lst,3)
        options.append(correct_response)
        question = self.question_template(question_statement, options, correct_response)
        return question
    
    def stadium_capacity_question(self, league_name, metric) -> dict:
        df = self.generate_df(team_query)
        leagues = ['Bundesliga','Premier League','Serie A','LaLiga','Ligue 1']
        #metric = random.choice(['largest','smalles'])
        #league_name = random.choice(leagues)
        ndf = df[df['league_name']==league_name]
        ndf = ndf.groupby('stadium_name').max('total_capacity').reset_index().sort_values('total_capacity')
        if metric == 'largest':
            correct_response = ndf.tail(1)['stadium_name'].iloc[0]
            options = list(ndf[:-1].sample(3)['stadium_name'].unique())
            options.append(correct_response)
        elif metric == 'smallest':
            correct_response = ndf.head(1)['stadium_name'].iloc[0]
            options = list(ndf[1:].sample(3)['stadium_name'].unique())
            options.append(correct_response)
        question_statement = f"Which stadium is the {metric} in the {league_name} by capacity?"
        question = self.question_template(question_statement, options, correct_response)
        return question
    
    def largest_stadium(self) -> dict:
        question_statement = "What is the largest stadium in Europe by capacity?"
        df = self.generate_df(team_query)
        df = df.head(4)
        correct_response = df.iloc[0]['stadium_name']
        total_capacity = int(df.iloc[0]['total_capacity'])
        options = df['stadium_name'].unique()
        description = f"{correct_response} is the largest stadium in Europe with a total capacity of {total_capacity}"
        question = self.question_template(question_statement, options, correct_response, description)
        return question
            