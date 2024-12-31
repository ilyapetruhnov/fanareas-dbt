from dagster_fanareas.quizzes.queries import *
from dagster_fanareas.quizzes.quizzes import Quizzes
import random

class CareerPathQuizzes(Quizzes):
    def __init__(self, title: str, description: str, quiz_type: int, is_demo: bool) -> None:
        super().__init__(title, description, quiz_type, is_demo)
        self.quiz_collection = []

    def create_career_path_question(self, options: list, img_name: str, question_statement: str):
        image_url = f'/career_path/{img_name}.jpg'
        correct_response = self.format_player_name(img_name)

        options.remove(correct_response)
        q_options = random.sample(options, 3)
        q_options.append(correct_response)

        question = self.question_template(
            question_statement = question_statement,
            correct_response = correct_response,
            options = options,
            image_url = image_url,
            image_size = 4
            )
        self.quiz_collection.append(question)
        return True
    
    def format_player_name(self, player_name: str):
        return " ".join(word.capitalize() for word in player_name[:-4].split('-'))

    def collect_career_path_questions(self):
        self.clear_collection()
        images = [
        'erling-haaland.jpg'          
        ,'alexis-sanchez.jpg '         
        ,'andriy-shevchenko.jpg'       
        ,'arjen-robben.jpg '           
        ,'clarence-seedorf.jpg'        
        ,'didier-drogba.jpg '          
        ,'dimitar-berbatov.jpg '       
        ,'fernando-torres.jpg'         
        ,'frank-ribery.jpg'
        ,'hakan-calhanoglu.jpg' 
        ,'kevin-De-Bruyne.jpg'  
        ,'kylian-mbappe.jpg'    
        ,'luis-figo.jpg'        
        ,'marcus-Thuram.jpg'    
        ,'matthijs-de-ligt.jpg' 
        ,'mesut-ozil.jpg'       
        ,'michael-ballack.jpg'  
        ,'michael-owen.jpg'
        ,'mikel-arteta.jpg'
        ,'mohamed-salah.jpg'
        ,'ousmane-dembele.jpg'
        ,'rafael-van-der-vaart.jpg'
        ,'ruud-van-nistelrooy.jpg'
        ,'thierry-henry.jpg'
        ,'xabi-alonso.jpg'
        ,'zinedine-zidane.jpg'
        ]
        question_statement = 'Guess the player'
        player_options = [
            "Diego Maradona", "Roberto Baggio", "George Weah", "Eric Cantona", "Gabriel Batistuta",
            "Paolo Maldini", "Romário", "Hristo Stoichkov", "Dennis Bergkamp", "Peter Schmeichel",
            "Zinedine Zidane", "Ronaldo Nazário", "Ronaldinho", "Thierry Henry", "David Beckham",
            "Luis Figo", "Michael Ballack", "Ruud van Nistelrooy", "Didier Drogba", "Alessandro Del Piero",
            "Frank Lampard", "Steven Gerrard", "Ryan Giggs", "Paul Scholes", "Andrea Pirlo",
            "Lionel Messi", "Cristiano Ronaldo", "Xavi Hernández", "Andrés Iniesta", "Zlatan Ibrahimović",
            "Neymar Jr.", "Mohamed Salah", "Luka Modrić", "Sergio Ramos", "Robert Lewandowski",
            "Harry Kane", "Kylian Mbappé", "Erling Haaland", "Marcelo", "Gianluigi Buffon",
            "Franck Ribéry", "Arjen Robben", "Wayne Rooney", "Xabi Alonso", "Samuel Eto'o",
            "Fernando Torres", "Carlos Tevez", "Eden Hazard", "Philipp Lahm", "Petr Čech",
            "Manuel Neuer", "Pavel Nedvěd", "Clarence Seedorf", "Cafu", "Dani Alves",
            "Patrick Vieira", "Claude Makélélé", "Nemanja Vidić", "John Terry", "Rivaldo",
            "Marco van Basten", "Ruud Gullit", "Johan Cruyff", "Karl-Heinz Rummenigge", "Gerd Müller",
            "Michel Platini", "Francesco Totti", "Gianluca Zambrotta", "Andriy Shevchenko", "Filippo Inzaghi",
            "Roberto Carlos", "Kaká", "Rivaldo", "Davor Šuker", "Lilian Thuram",
            "Patrick Kluivert", "Fabio Cannavaro", "Alessandro Nesta", "David Villa", "Diego Forlán",
            "Oscar Ruggeri", "Edinson Cavani", "Angel Di María", "Mario Kempes", "Teemu Pukki",
            "Raúl González", "Iker Casillas", "Vinícius Júnior", "Gareth Bale", "Toni Kroos",
            "Gonzalo Higuaín", "Christian Eriksen", "Paolo Rossi", "Jean-Pierre Papin",
            "Jan Oblak", "Ederson Moraes", "Victor Valdés", "Hugo Lloris", "Alisson Becker",
            "Keylor Navas", "Dida", "Claudio Bravo", "Sokratis Papastathopoulos", "Thiago Silva"
        ]
        
        random.shuffle(images)
        for img_name in images:
            self.create_career_path_question(player_options, img_name, question_statement)
        return self.quiz_collection
    