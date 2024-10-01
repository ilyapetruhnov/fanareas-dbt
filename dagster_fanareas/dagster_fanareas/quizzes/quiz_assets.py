from dagster import asset
import random
from itertools import product
import time
from dagster_fanareas.quizzes.quizzes import Quizzes
from dagster_fanareas.quizzes.transferQuizzes import TransferQuizzes
from dagster_fanareas.quizzes.photoQuizzes import PhotoQuizzes
from dagster_fanareas.quizzes.teamQuizzes import TeamQuizzes
from dagster_fanareas.quizzes.teamQuizz import TeamQuizz
from dagster_fanareas.quizzes.playerQuizzes import PlayerQuizzes
from dagster_fanareas.quizzes.nationalQuizzes import NationalTeamQuizzes
from dagster_fanareas.quizzes.stadiumQuizzes import StadiumQuizzes
from dagster_fanareas.quizzes.featuredQuizzes import FeaturedQuizzes
from dagster_fanareas.quizzes.queries import *
from dagster_fanareas.ops.utils import get_dim_name_and_id
from dagster_fanareas.quizzes.quiz_collection import validate_team_season, post_guess_team_player_quiz



@asset(group_name="quizzes")
def new_guess_the_player_quiz(context) -> bool:
    title = "Guess the player"
    description = "5 questions about football players"
    quiz_type = 2
    is_demo = False
    player_quiz = PlayerQuizzes(title, description, quiz_type, is_demo)
    result = player_quiz.create_quiz()
    random.shuffle(result)
    context.log.info(f"generated {len(result)} questions")
    # Generate 50 batches with 5 items each
    random.shuffle(result)
    batches = [result[i:i + 5] for i in range(0, len(result), 5)]
    context.log.info(f"generated {len(batches)} batches")
    random.shuffle(batches)
    for idx, batch in enumerate(batches, start=1):
        context.log.info(f"generated quiz {idx}")
        context.log.info(f"batch size {len(batch)}")
        random.shuffle(batch)
        player_quiz.post_quiz(batch)
        time.sleep(10)
    return True

@asset(group_name="quizzes")
def new_featured_quiz(context) -> bool:
    title = 'Featured quiz'
    description = 'Featured quiz description'
    quiz_type = 0
    is_demo = False
    featured_quiz = FeaturedQuizzes(title, description, quiz_type, is_demo)
    # team_ids = [11,985,631,148,31,131,418,506,12,6195,46,27,16,5]
    # season_ids = [2005,2006,2007,2008,2009,2010,2011,2012,2013,2014]
    team_ids = [11,31,5]
    season_ids = [2009,2011]
    for team_id, season_id in product(team_ids, season_ids):
        featured_quiz.create_quiz(team_id, season_id)
        context.log.info(f"generated quiz for {len(team_ids, season_ids)}")
        time.sleep(5)
    return True

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

@asset(group_name="new_quizzes")
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

@asset(group_name="new_quizzes")
def team_quiz_2() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.unbeaten('L1'))
    quiz_obj.collect_questions(team_quiz.oldest_club())
    quiz_obj.collect_questions(team_quiz.never_won_el())
    quiz_obj.collect_questions(team_quiz.team_logo('Fulham FC'))
    quiz_obj.collect_questions(stadium_quiz.stadium_photo_question())
    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
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
    quiz_obj.collect_questions(team_quiz.club_coach('Premier League'))
    quiz_obj.collect_questions(stadium_quiz.stadium_city_question())

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_4() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.most_el_titles())
    quiz_obj.collect_questions(stadium_quiz.home_stadium_question('Bundesliga'))
    quiz_obj.collect_questions(national_quiz.year_single_time_winner_question('euro','Czechoslovakia'))
    quiz_obj.collect_questions(team_quiz.first_to_reach_100_points_in_league('GB1'))
    quiz_obj.collect_questions(team_quiz.team_logo('UC Sampdoria'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
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

@asset(group_name="new_quizzes")
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
    quiz_obj.collect_questions(stadium_quiz.stadium_capacity_question(league_name = 'Bundesliga'))
    quiz_obj.collect_questions(national_quiz.winner_coach_question(title_name = 'euro'))
    quiz_obj.collect_questions(team_quiz.cl_final_2005())
    quiz_obj.collect_questions(team_quiz.unbeaten(league_id='GB1'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_7() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(stadium_quiz.stadium_photo_question())
    quiz_obj.collect_questions(national_quiz.year_single_time_winner_question('world','England'))
    quiz_obj.collect_questions(team_quiz.first_cl_win())
    quiz_obj.collect_questions(national_quiz.never_won_question('euro'))
    quiz_obj.collect_questions(team_quiz.cup_titles('Premier League'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_8() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.most_cl_titles())
    quiz_obj.collect_questions(team_quiz.club_coach('Bundesliga'))
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(team_quiz.team_logo('SV Werder Bremen'))
    quiz_obj.collect_questions(team_quiz.most_offsides('Premier League'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_9() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(stadium_quiz.stadium_photo_question())
    quiz_obj.collect_questions(national_quiz.first_winner_question('world'))
    quiz_obj.collect_questions(stadium_quiz.home_stadium_question('Premier League'))
    quiz_obj.collect_questions(team_quiz.most_domestic_championship_titles('GB1'))
    quiz_obj.collect_questions(team_quiz.cup_titles('Serie A'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_10() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.first_cl_win())
    quiz_obj.collect_questions(team_quiz.most_corners('Premier League'))
    quiz_obj.collect_questions(team_quiz.team_position(season=2023, league_id='ES1', title_won=True))
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(national_quiz.winner_coach_question('euro'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_11() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(stadium_quiz.specific_team_stadium_question(q=3))
    quiz_obj.collect_questions(team_quiz.team_logo('Olympique Marseille'))
    quiz_obj.collect_questions(team_quiz.team_position(season=2023, league_id='IT1', title_won=True))
    quiz_obj.collect_questions(team_quiz.fewest_points('GB1'))
    quiz_obj.collect_questions(national_quiz.year_single_time_winner_question('euro','Denmark'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_12() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(stadium_quiz.stadium_city_question())
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(team_quiz.most_points(league_id='IT1'))
    quiz_obj.collect_questions(team_quiz.club_nickname('Inter Milan'))
    quiz_obj.collect_questions(team_quiz.highest_avg_possesion('Premier League'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_13() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(national_quiz.most_title_question('euro'))
    quiz_obj.collect_questions(stadium_quiz.stadium_photo_question())
    quiz_obj.collect_questions(team_quiz.never_won_domestic_championship_title(league_id='GB1'))
    quiz_obj.collect_questions(team_quiz.cup_titles('LaLiga'))
    quiz_obj.collect_questions(team_quiz.never_won_el())

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_14() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(stadium_quiz.specific_team_stadium_question(q=4))
    quiz_obj.collect_questions(national_quiz.winner_coach_question('world'))
    quiz_obj.collect_questions(stadium_quiz.stadium_capacity_question(league_name='LaLiga'))
    quiz_obj.collect_questions(team_quiz.club_coach('LaLiga'))
    quiz_obj.collect_questions(team_quiz.team_position(season=2023, league_id='ES1', title_won=False))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_15() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.most_corners('Bundesliga'))
    quiz_obj.collect_questions(stadium_quiz.home_stadium_question('Premier League'))
    quiz_obj.collect_questions(national_quiz.first_winner_question('euro'))
    quiz_obj.collect_questions(team_quiz.never_won_cl())
    quiz_obj.collect_questions(team_quiz.team_logo('Leeds United'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True


@asset(group_name="new_quizzes")
def team_quiz_16() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.club_nickname('Everton'))
    quiz_obj.collect_questions(stadium_quiz.stadium_city_question())
    quiz_obj.collect_questions(team_quiz.fewest_points('ES1'))
    quiz_obj.collect_questions(team_quiz.most_domestic_championship_titles('L1'))
    quiz_obj.collect_questions(team_quiz.conceded_most_goals('GB1'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True


@asset(group_name="new_quizzes")
def team_quiz_17() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.team_position(season=2023,league_id='L1',title_won=True))
    quiz_obj.collect_questions(stadium_quiz.stadium_photo_question())
    quiz_obj.collect_questions(stadium_quiz.home_stadium_question('Serie A'))
    quiz_obj.collect_questions(team_quiz.scored_most_goals('GB1'))
    quiz_obj.collect_questions(team_quiz.first_to_reach_100_points_in_league('ES1'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True


@asset(group_name="new_quizzes")
def team_quiz_18() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.highest_avg_possesion('Bundesliga'))
    quiz_obj.collect_questions(team_quiz.team_position(season=2023,league_id='GB1',title_won=False))
    quiz_obj.collect_questions(team_quiz.most_offsides('LaLiga'))
    quiz_obj.collect_questions(stadium_quiz.home_stadium_question('LaLiga'))
    quiz_obj.collect_questions(team_quiz.team_logo('Villarreal CF'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_19() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.club_nickname('AC Milan'))
    quiz_obj.collect_questions(team_quiz.club_coach('Serie A'))
    quiz_obj.collect_questions(team_quiz.most_fouls('LaLiga'))
    quiz_obj.collect_questions(team_quiz.most_points('GB1'))
    quiz_obj.collect_questions(stadium_quiz.stadium_capacity_question(league_name='Serie A'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_20() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(stadium_quiz.home_stadium_question('Bundesliga'))
    quiz_obj.collect_questions(team_quiz.fewest_points('IT1'))
    quiz_obj.collect_questions(team_quiz.team_position(season=2023, league_id='IT1', title_won=False))
    quiz_obj.collect_questions(team_quiz.conceded_most_goals('ES1'))
    quiz_obj.collect_questions(team_quiz.never_won_domestic_championship_title('L1'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_21() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.team_logo('Sevilla FC'))
    quiz_obj.collect_questions(stadium_quiz.stadium_photo_question())
    quiz_obj.collect_questions(stadium_quiz.stadium_city_question())
    quiz_obj.collect_questions(team_quiz.most_corners('LaLiga'))
    quiz_obj.collect_questions(team_quiz.cup_titles('Bundesliga'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_22() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(stadium_quiz.home_stadium_question('Premier League'))
    quiz_obj.collect_questions(national_quiz.year_single_time_winner_question('euro','Greece')) # greece 
    quiz_obj.collect_questions(team_quiz.most_fouls('Bundesliga'))
    quiz_obj.collect_questions(national_quiz.nation_first_title_question('world'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_23() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.team_position(season=2023,league_id='GB1',title_won=False))
    quiz_obj.collect_questions(stadium_quiz.home_stadium_question('LaLiga'))
    quiz_obj.collect_questions(team_quiz.club_nickname('Newcastle United'))
    quiz_obj.collect_questions(national_quiz.never_won_question('world'))
    quiz_obj.collect_questions(team_quiz.scored_most_goals('L1'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_24() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(stadium_quiz.stadium_photo_question())
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(team_quiz.team_position(season=2023,league_id='FR1',title_won=False))
    quiz_obj.collect_questions(team_quiz.club_coach('Ligue 1'))
    quiz_obj.collect_questions(team_quiz.highest_avg_possesion('LaLiga'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_25() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(national_quiz.both_cups_winner_question())
    quiz_obj.collect_questions(stadium_quiz.stadium_city_question())
    quiz_obj.collect_questions(national_quiz.year_single_time_winner_question('euro','Netherlands'))
    quiz_obj.collect_questions(team_quiz.team_position(season=2023,league_id='ES1',title_won=False))
    quiz_obj.collect_questions(stadium_quiz.specific_team_stadium_question(q=5))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_26() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.team_logo('Atlético de Madrid'))
    quiz_obj.collect_questions(stadium_quiz.home_stadium_question('Ligue 1'))
    quiz_obj.collect_questions(team_quiz.club_coach('Premier League'))
    quiz_obj.collect_questions(team_quiz.never_won_domestic_championship_title('FR1'))
    quiz_obj.collect_questions(team_quiz.conceded_most_goals('IT1'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_27() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(stadium_quiz.stadium_capacity_question('Premier League'))
    quiz_obj.collect_questions(national_quiz.year_single_time_winner_question('world','Spain'))
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(team_quiz.most_points('FR1'))
    quiz_obj.collect_questions(team_quiz.club_nickname('Real Madrid'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_28() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.team_position(season=2023,league_id='L1',title_won=False))
    quiz_obj.collect_questions(stadium_quiz.stadium_photo_question())
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(team_quiz.most_domestic_championship_titles('IT1'))
    quiz_obj.collect_questions(team_quiz.club_coach('LaLiga'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_29() -> bool:
    title = "Team quiz"
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(stadium_quiz.specific_team_stadium_question(q=6))
    quiz_obj.collect_questions(team_quiz.maradonna_club())
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(team_quiz.team_logo('Inter Milan'))
    quiz_obj.collect_questions(team_quiz.club_nickname('Atletico Madrid'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_30() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(stadium_quiz.stadium_photo_question())
    quiz_obj.collect_questions(national_quiz.nation_first_title_question('world'))
    quiz_obj.collect_questions(team_quiz.most_domestic_championship_titles('ES1'))
    quiz_obj.collect_questions(team_quiz.cup_titles('Ligue 1'))
    quiz_obj.collect_questions(team_quiz.most_corners('Ligue 1'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_31() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.team_position(season=2023,league_id='IT1',title_won=False))
    quiz_obj.collect_questions(stadium_quiz.home_stadium_question('Serie A'))
    quiz_obj.collect_questions(team_quiz.team_logo('Real Sociedad'))
    quiz_obj.collect_questions(team_quiz.club_coach('Bundesliga'))
    quiz_obj.collect_questions(team_quiz.highest_avg_possesion('Ligue 1'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_32() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(stadium_quiz.stadium_photo_question())
    quiz_obj.collect_questions(team_quiz.never_won_cl())
    quiz_obj.collect_questions(national_quiz.winner_coach_question('world'))
    quiz_obj.collect_questions(team_quiz.fewest_points('FR1'))
    quiz_obj.collect_questions(team_quiz.player_from_team())

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_33() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.team_position(season=2023,league_id='GB1',title_won=False))
    quiz_obj.collect_questions(team_quiz.most_points('L1'))
    quiz_obj.collect_questions(team_quiz.most_domestic_championship_titles('FR1'))
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(team_quiz.most_offsides('Serie A'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_34() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(stadium_quiz.stadium_photo_question())
    quiz_obj.collect_questions(national_quiz.nation_first_title_question('euro'))
    quiz_obj.collect_questions(team_quiz.team_logo('Bologna FC 1909'))
    quiz_obj.collect_questions(team_quiz.fewest_points('L1'))
    quiz_obj.collect_questions(team_quiz.player_from_team())

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_35() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(stadium_quiz.stadium_city_question())
    quiz_obj.collect_questions(team_quiz.ronaldihno_club())
    quiz_obj.collect_questions(team_quiz.never_won_domestic_championship_title('ES1'))
    quiz_obj.collect_questions(stadium_quiz.home_stadium_question('Ligue 1'))
    quiz_obj.collect_questions(team_quiz.most_offsides('Bundesliga'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_36() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.team_logo('ACF Fiorentina'))
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(team_quiz.most_points('ES1'))
    quiz_obj.collect_questions(team_quiz.winner_with_fewest_points('ES1'))
    quiz_obj.collect_questions(team_quiz.club_coach('Serie A'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_37() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(stadium_quiz.home_stadium_question('LaLiga'))
    quiz_obj.collect_questions(team_quiz.maldini_club())
    quiz_obj.collect_questions(team_quiz.most_fouls('Premier League'))
    quiz_obj.collect_questions(team_quiz.team_position(season=2023,league_id='FR1',title_won=False))
    quiz_obj.collect_questions(stadium_quiz.stadium_city_question())

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_38() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(stadium_quiz.stadium_photo_question())
    quiz_obj.collect_questions(national_quiz.nation_first_title_question('world'))
    quiz_obj.collect_questions(team_quiz.team_logo('Juventus FC'))
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(team_quiz.most_offsides('Ligue 1'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_39() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(stadium_quiz.home_stadium_question('Bundesliga'))
    quiz_obj.collect_questions(national_quiz.year_single_time_winner_question('euro','Portugal'))
    quiz_obj.collect_questions(team_quiz.club_nickname('Manchester City'))
    quiz_obj.collect_questions(team_quiz.club_coach('Ligue 1'))
    quiz_obj.collect_questions(team_quiz.first_to_reach_100_points_in_league('L1'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_40() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.team_logo('AC Milan'))
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(team_quiz.scored_most_goals('FR1'))
    quiz_obj.collect_questions(stadium_quiz.stadium_city_question())
    quiz_obj.collect_questions(team_quiz.most_fouls('Serie A'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_41() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.most_corners('Serie A'))
    quiz_obj.collect_questions(team_quiz.club_nickname('Stoke City'))
    quiz_obj.collect_questions(stadium_quiz.home_stadium_question('Ligue 1'))
    quiz_obj.collect_questions(stadium_quiz.stadium_photo_question())
    quiz_obj.collect_questions(team_quiz.henry_club())
    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_42() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.team_position(season=2023,league_id='ES1',title_won=False))
    quiz_obj.collect_questions(team_quiz.team_logo('VfB Stuttgart'))
    quiz_obj.collect_questions(stadium_quiz.stadium_city_question())
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(team_quiz.club_coach('Premier League'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_43() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(stadium_quiz.specific_team_stadium_question(q=7))
    quiz_obj.collect_questions(team_quiz.most_fouls('Ligue 1'))
    quiz_obj.collect_questions(stadium_quiz.home_stadium_question('Serie A'))
    quiz_obj.collect_questions(team_quiz.club_nickname('Tottenham'))
    quiz_obj.collect_questions(national_quiz.never_won_question('euro'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True


@asset(group_name="new_quizzes")
def team_quiz_44() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(stadium_quiz.stadium_photo_question())
    quiz_obj.collect_questions(national_quiz.nation_first_title_question('euro'))
    quiz_obj.collect_questions(team_quiz.team_logo('Eintracht Frankfurt'))
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(team_quiz.scored_most_goals('ES1'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_45() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.team_position(season=2023,league_id='IT1',title_won=False))
    quiz_obj.collect_questions(stadium_quiz.home_stadium_question('Premier League'))
    quiz_obj.collect_questions(national_quiz.year_single_time_winner_question('euro','Soviet Union'))
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(team_quiz.club_coach('Bundesliga'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_46() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.team_logo('Borussia Dortmund'))
    quiz_obj.collect_questions(stadium_quiz.stadium_city_question())
    quiz_obj.collect_questions(team_quiz.club_nickname('Southampton'))
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(stadium_quiz.specific_team_stadium_question(q=8))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_47() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(national_quiz.first_winner_question('euro'))
    quiz_obj.collect_questions(team_quiz.winner_with_fewest_points('GB1'))
    quiz_obj.collect_questions(national_quiz.never_won_question('world'))
    quiz_obj.collect_questions(team_quiz.club_coach('Ligue 1'))
    quiz_obj.collect_questions(team_quiz.highest_avg_possesion('Serie A'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_48() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.team_position(season=2023,league_id='L1',title_won=False))
    quiz_obj.collect_questions(stadium_quiz.stadium_photo_question())
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(team_quiz.team_logo('Borussia Mönchengladbach'))
    quiz_obj.collect_questions(team_quiz.never_won_domestic_championship_title('IT1'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_49() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(stadium_quiz.home_stadium_question('Premier League'))
    quiz_obj.collect_questions(team_quiz.club_nickname('AFC Bournemouth'))
    quiz_obj.collect_questions(national_quiz.winner_coach_question('euro'))
    quiz_obj.collect_questions(team_quiz.winner_with_fewest_points('IT1'))
    quiz_obj.collect_questions(team_quiz.conceded_most_goals('FR1'))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True

@asset(group_name="new_quizzes")
def team_quiz_50() -> bool:
    title = "Team quiz" 
    description = "Answer 5 questions"
    quiz_type = 3
    is_demo = False
    team_quiz = TeamQuizzes(title, description, quiz_type, is_demo)
    stadium_quiz = StadiumQuizzes(title, description, quiz_type, is_demo)
    national_quiz = NationalTeamQuizzes(title, description, quiz_type, is_demo)
    quiz_obj = Quizzes(title, description, quiz_type, is_demo)
    quiz_obj.collect_questions(team_quiz.team_logo('SC Freiburg'))
    quiz_obj.collect_questions(team_quiz.player_from_team())
    quiz_obj.collect_questions(stadium_quiz.specific_team_stadium_question(q=9))
    quiz_obj.collect_questions(national_quiz.nation_first_title_question('euro'))
    quiz_obj.collect_questions(team_quiz.team_position(season=2023,league_id='FR1',title_won=False))

    quiz_obj.post_quiz(questions = quiz_obj.quiz_collection)
    return True


