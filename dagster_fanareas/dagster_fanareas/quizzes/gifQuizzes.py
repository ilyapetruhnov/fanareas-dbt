from dagster_fanareas.quizzes.queries import *
from dagster_fanareas.quizzes.quizzes import Quizzes

class GIFQuizzes(Quizzes):
    def __init__(self, title: str, description: str, quiz_type: int, is_demo: bool) -> None:
        super().__init__(title, description, quiz_type, is_demo)
        self.quiz_collection = []

    def create_gif_question(self, options: list, correct_response: str, question_statement: str):
        correct_url = ''.join(correct_response.split()[1:]).lower()
        image_url = f'/gifs/{correct_url}.gif'
        question = self.question_template(question_statement = question_statement,
                                          correct_response = correct_response,
                                          options = options, 
                                          image_url = image_url, 
                                          image_size = 4)
        self.quiz_collection.append(question)
        return True
    
    def bouns_question(self, question_statement: str , options: list):
        correct_response = options[0]
        self.create_gif_question(
                options = options, 
                question_statement = question_statement, 
                correct_response = correct_response
                                )
        return True

    def bonus_quiz_1(self):
        self.clear_collection()
        question_statement = 'Who scored this goal?'
        self.bouns_question(question_statement, 
                            options = [
                                        'Roberto Carlos',
                                        'Zico',
                                        'Adriano',
                                        'Ronaldo'
                                        ]
        )
        self.bouns_question(question_statement, 
                            options = [
                                        'Alessandro Del Piero',
                                        'Mauro Camoranesi',
                                        'Claudio Marchisio',
                                        'Sebastian Giovinco'
                                    ]
        )
        self.bouns_question(question_statement, 
                            options = [
                                        'Robin van Persie',
                                        'Dimitar Berbatov',
                                        'Zlatan Ibrahimovic',
                                        'Fernando Torres'
                                        ]
        )
        self.bouns_question(question_statement, 
                            options = [
                                        'Dennis Bergkamp',
                                        'Eric Cantona',
                                        'Roberto Baggio',
                                        'Gianfranco Zola'
                                    ]
        )
        self.bouns_question(question_statement, 
                            options = [
                                        "Wayne Rooney",
                                        'Paul Scholes',
                                        'Carlos Tevez',
                                        'Michael Owen'
                                    ]
        )
        return self.quiz_collection 

    
    def bonus_quiz_2(self):
        self.clear_collection()
        question_statement = 'Who scored this goal?'
        self.bouns_question(question_statement, 
                            options = [
                                        'Papiss Cisse',
                                        'Demba Ba',
                                        'Asamoah Gyan',
                                        'Obafemi Martins'
                                        ]
        )
        self.bouns_question(question_statement, 
                            options = [
                                        'Federico Valverde',
                                        'Toni Kross',
                                        'Gareth Bale',
                                        'Karim Benzema'
                                        ]
        )
        self.bouns_question(question_statement, 
                            options = [
                                        "Juninho",
                                        'Alessandro Del Piero',
                                        'Andrea Pirlo',
                                        'David Beckham'
                                    ]
        )
        self.bouns_question(question_statement, 
                            options = [
                                        'Dejan Stankovich',
                                        'Wesley Sneijder',
                                        'Diego Milito',
                                        'Thiago Motta'
                                        ]
        )
        self.bouns_question(question_statement, 
                            options = [
                                        'Zlatan Ibrahimovic',
                                        'Olivier Giroud',
                                        'Dusan Vlahovic',
                                        'Christian Vieri'
                                        ]
        )
        return self.quiz_collection 
    
    def bonus_quiz_3(self):
        self.clear_collection()
        question_statement = 'Who is the goalkeeper making this save?'
        self.bouns_question(question_statement, 
                            options = [
                                        'Grégory Coupet',
                                        'Iker Casillas',
                                        'Fabien Barthez',
                                        'Santiago Cañizares'
                                        ]
        )
        self.bouns_question(question_statement, 
                            options = [
                                        'Jerzy Dudek',
                                        'Petr Cech',
                                        'Oliver Kahn',
                                        'Fabien Barthez'
                                        ]
        )

        self.bouns_question(question_statement, 
                            options = [
                                        'Lukasz Fabianski',
                                        'Bernd Leno',
                                        'Brad Friedel',
                                        'Shay Given'
                                        ]
        )

        self.bouns_question(question_statement, 
                            options = [
                                        'Manuel Neuer',
                                        'Sven Ulreich',
                                        'Marc-André ter Stegen',
                                        'Thibaut Courtois'
                                        ]
        )

        self.bouns_question(question_statement, 
                            options = [
                                        'René Higuita',
                                        'Bruce Grobbelaar',
                                        'José Luis Chilavert',
                                        'Jorge Campos'
                                        ]
        )
        return self.quiz_collection 

    

        
