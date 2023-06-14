from sqlalchemy import create_engine


def mysql_connector():  # Connection to mysql
    return create_engine(
        url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
            'root', 'Mysql_pwd1', "localhost", 3306, "yt_data"
        )
    )


def execute_query(query):  # execute the query and fetching the records
    with mysql_connector().connect() as connection:
        result = connection.execute(query).fetchall()
    return result
