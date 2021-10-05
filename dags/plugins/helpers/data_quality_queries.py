class DataQualityQueries:
    staging_events_day_count = """
        SELECT count(*) from (
            SELECT TIMESTAMP 'epoch' + ts/1000
                * interval '1 second' as start_time
            FROM staging_events
            WHERE
                EXTRACT(year from start_time) = {execution_date.year} AND
                EXTRACT(month from start_time) = {execution_date.month} AND
                EXTRACT(day from start_time) = {execution_date.day} AND
                page = 'NextSong'
        )
    """

    songplays_day_count = """
        SELECT count(*)
        FROM songplays
        WHERE
            EXTRACT(year from start_time) = {execution_date.year} AND
            EXTRACT(month from start_time) = {execution_date.month} AND
            EXTRACT(day from start_time) = {execution_date.day}
    """

    songs_null_check = """
        SELECT count(*)
        FROM  songs
        WHERE title = '' or title is NULL;
    """

    users_name_check = """
        SELECT count(*)
        FROM users
        WHERE
            (first_name = '' OR first_name is NULL) OR
            (last_name = '' OR last_name is NULL)
    """
    artists_name_check = """
        SELECT count(*)
        FROM artists
        WHERE name = '' or name is NULL;
    """

    comparison_pairs = (
        (staging_events_day_count, songplays_day_count),
        (songs_null_check, 0,),
        (users_name_check, 0,),
        (artists_name_check, 0,),
    )
