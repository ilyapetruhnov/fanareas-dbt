from dagster import asset
from dagster_fanareas.quizzes.quizzes import Quizzes
from dagster_fanareas.quizzes.transferQuizzes import TransferQuizzes
from dagster_fanareas.quizzes.photoQuizzes import PhotoQuizzes
from dagster_fanareas.quizzes.teamQuizzes import TeamQuizzes
from dagster_fanareas.quizzes.teamQuizz import TeamQuizz
from dagster_fanareas.quizzes.playerQuizzes import PlayerQuizzes
from dagster_fanareas.quizzes.nationalQuizzes import NationalTeamQuizzes
from dagster_fanareas.quizzes.stadiumQuizzes import StadiumQuizzes
from dagster_fanareas.quizzes.queries import *
from dagster_fanareas.ops.utils import get_dim_name_and_id
from dagster_fanareas.quizzes.quiz_collection import validate_team_season, post_guess_team_player_quiz

@asset(group_name="quizzes")
def guess_team_player_quiz() -> bool:
    #team generation
    generated_team = get_dim_name_and_id('teams')
    team_name = generated_team['name']
    team_id = generated_team['id']
    #season generation
    generated_season = get_dim_name_and_id('seasons')
    season_name = generated_season['name']
    season_id = generated_season['id']
    season = int(season_name[:4])
    if validate_team_season(team_id, season_id):
        post_guess_team_player_quiz(team_id, team_name, season, season_id, season_name)
        return True
    else:
        guess_team_player_quiz()
    return True


@asset(group_name="quizzes")
def transfers_quiz() -> bool:
    title = "Transfers"
    description = "Answer 5 questions about Premier League transfers"
    quiz_type = 1
    is_demo = False
    quiz_obj = TransferQuizzes(title, description, quiz_type, is_demo)
    quiz_obj.fill_quiz_with_questions()
    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="quizzes")
def guess_the_team_quiz() -> bool:
    title = f"Guess the Premier League team"
    description = f"Guess 5 Premier League teams"
    quiz_type = 3
    is_demo = False
    quiz_obj = TeamQuizz(title, description, quiz_type, is_demo)
    quiz_obj.fill_quiz_with_questions()
    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="quizzes")
def photo_quiz() -> bool:
    title = "Daily guess the player by photo"
    description = "Guess 5 Premier League players by photo"
    quiz_type = 2
    is_demo = False
    quiz_obj = PhotoQuizzes(title, description, quiz_type, is_demo)
    quiz_obj.fill_quiz_with_questions()
    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True


@asset(group_name="quizzes")
def small_demo_quiz() -> bool:
    title = "English Premier League"
    description = "Answer 5 questions about Premier League"
    quiz_type = -1
    is_demo = True
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)

    quiz_obj.collect_questions(quiz_obj.generate_venue_question())
    quiz_obj.collect_questions(quiz_obj.generate_player_played_for_team_question())

    quiz_obj.collect_questions(quiz_obj.generate_capacity_question())
    quiz_obj.collect_questions(quiz_obj.generate_fewest_points_question())

    quiz_obj.collect_questions(quiz_obj.generate_player_shirt_number_question())

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True


@asset(group_name="quizzes")
def demo_quiz() -> bool:
    title = "English Premier League"
    description = "Answer 10 questions about Premier League"
    quiz_type = -1
    is_demo = True
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(quiz_obj.generate_player_shirt_number_question())
    quiz_obj.collect_questions(quiz_obj.generate_player_2_clubs_question())
    quiz_obj.collect_questions(quiz_obj.generate_team_stats_question())
    quiz_obj.collect_questions(quiz_obj.generate_team_stats_question())
    quiz_obj.collect_questions(quiz_obj.generate_venue_question())
    quiz_obj.collect_questions(quiz_obj.generate_founded_question())
    quiz_obj.collect_questions(quiz_obj.generate_capacity_question())
    quiz_obj.collect_questions(quiz_obj.generate_fewest_points_question())
    quiz_obj.collect_questions(quiz_obj.generate_most_points_question())
    quiz_obj.collect_questions(quiz_obj.generate_relegations_question())

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="quizzes")
def tm_demo_quiz() -> bool:
    title = "Demo quiz"
    description = "Answer 5 questions"
    quiz_type = -1
    is_demo = True
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    player_quiz = PlayerQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(player_quiz.player_top_league_stats(league_name = 'Premier League',metric='goals'))
    quiz_obj.collect_questions(player_quiz.goalkeeper_goals())
    quiz_obj.collect_questions(player_quiz.record_transfer())
    quiz_obj.collect_questions(team_quiz.never_won_cl())
    quiz_obj.collect_questions(team_quiz.unbeaten(league_id='GB1'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="quizzes")
def team_quiz_1() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.club_nickname('Manchester United'))
    quiz_obj.collect_questions(stadium_quiz.largest_stadium())
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(team_quiz.never_won_cl())
    quiz_obj.collect_questions(national_quiz.most_title_question('world'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="quizzes")
def team_quiz_2() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.unbeaten('L1'))
    quiz_obj.collect_questions(stadium_quiz.home_stadium_question('Bundesliga'))
    quiz_obj.collect_questions(stadium_quiz.stadium_photo_question())
    quiz_obj.collect_questions(national_quiz.first_winner_question('world'))
    quiz_obj.collect_questions(team_quiz.team_logo())

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="quizzes")
def team_quiz_3() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.never_won_domestic_championship_title('IT1'))
    quiz_obj.collect_questions(stadium_quiz.specific_team_stadium_question(q=2))
    quiz_obj.collect_questions(team_quiz.team_position(season=2023,league_id='GB1',title_won=True))
    quiz_obj.collect_questions(team_quiz.club_coach('GB1'))
    quiz_obj.collect_questions(stadium_quiz.stadium_city_question())

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="quizzes")
def team_quiz_4() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.most_el_titles())
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(national_quiz.year_single_time_winner_question('euro'))
    quiz_obj.collect_questions(team_quiz.first_to_reach_100_points_in_league('GB1'))
    quiz_obj.collect_questions(stadium_quiz.stadium_capacity_question(league_name='LaLiga',metric='smallest'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="quizzes")
def team_quiz_5() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.club_nickname('Arsenal'))
    quiz_obj.collect_questions(stadium_quiz.specific_team_stadium_question(q=1))
    quiz_obj.collect_questions(national_quiz.nation_first_title_question('world'))
    quiz_obj.collect_questions(team_quiz.team_logo('Wolverhampton Wanderers'))
    quiz_obj.collect_questions(national_quiz.both_cups_winner_question())

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="quizzes")
def team_quiz_6() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(stadium_quiz.stadium_capacity_question(league_name = 'Bundesliga', metric='largest'))
    quiz_obj.collect_questions(national_quiz.winner_coach_question(title_name = 'euro'))
    quiz_obj.collect_questions(team_quiz.cl_final_2005())
    quiz_obj.collect_questions(team_quiz.unbeaten(league_id='IT1'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True