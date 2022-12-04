from .mysql import get_data


def calculated_challengers():
    mix_df = get_data(title="mysql_calculated/mixpanel")
    db_df = get_data(title="mysql_calculated/challenge_join")
