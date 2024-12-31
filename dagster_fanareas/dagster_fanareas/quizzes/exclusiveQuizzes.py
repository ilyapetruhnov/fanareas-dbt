from dagster_fanareas.quizzes.queries import *
from dagster_fanareas.quizzes.quizzes import Quizzes

class RulesQuizzes(Quizzes):
    def __init__(self, title: str, description: str, quiz_type: int, is_demo: bool) -> None:
        super().__init__(title, description, quiz_type, is_demo)
        self.quiz_collection = []

    def rules_question(self, question_statement: str , correct_response: str, options: list):
        question = self.question_template(
                question_statement = question_statement, 
                correct_response = correct_response,
                options = options
                                )
        self.quiz_collection.append(question)
        return True

    def create_rules_quiz(self):
        self.clear_collection()
        question_statement = 'What is the official diameter of a soccer ball used in professional matches?'
        correct_response = '22-23 cm'
        self.rules_question(question_statement,
                            correct_response,
                            options = [
                                        '20-21 cm',
                                        '22-23 cm',
                                        '24-25 cm',
                                        '26-27 cm'
                                        ]
        )
        question_statement = 'What is the official distance between the goalposts in a regulation football goal?'
        correct_response = '7.32 meters'
        self.rules_question(question_statement,
                            correct_response,
                            options = [
                                        '6.32 meters',
                                        '7.32 meters',
                                        '8.32 meters',
                                        '9.32 meters'
                                    ]
        )
        question_statement = 'How far must a defensive wall stand from the ball during a free kick?'
        correct_response = '10 yards (9.15 meters)'
        self.rules_question(question_statement,
                            correct_response,
                            options = [
                                        '7 yards (6.40 meters)',
                                        '8 yards (7.31 meters)',
                                        '9 yards (8.22 meters)',
                                        '10 yards (9.15 meters)'
                                        ]
        )
        question_statement = 'What is the maximum weight of a regulation soccer ball at the start of a match?'
        correct_response = '450 grams'
        self.rules_question(question_statement,
                            correct_response,
                            options = [
                                        '400 grams',
                                        '410 grams',
                                        '450 grams',
                                        '500 grams'
                                    ]
        )
        question_statement = 'What is the minimum height of a soccer corner flag?'
        correct_response = '1.5 meters'
        self.rules_question(question_statement,
                            correct_response,
                            options = [
                                        '1 meter',
                                        '1.2 meters',
                                        '1.5 meters',
                                        '1.75 meters'
                                    ]
        )
        return self.quiz_collection 
    