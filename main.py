import pandas as pd
from db_conn import *

# pandas 라이브러리를 사용해서 "top_movies.csv" 파일을 df라는 변수에 저장하는 과정
file_name = 'top_movies.csv'
df = pd.read_csv(file_name)

# db에 저장하기 위한 "top_movies.csv"의 컬럼 종류와 매칭시킨 insert 명령어 포맷
insert_sql = """insert into movie ( id, movie_name, year_of_release,
                    watch_time, movie_rating, metascore_of_movie, gross, votes, description)
                values(%s, %s,%s, %s,%s, %s,%s, %s, %s);"""

# db에 커넥션을 연결시키는 과정
conn, cur = open_db('DS2023')

# db에 이전 테이블이 존재하면 테이블을 삭제하는 sql
drop_table_sql = """drop table movie"""
cur.execute(drop_table_sql)

# db에 저장하기 위한 테이블을 생성하는 명령어 포맷
create_table_sql = """CREATE TABLE movie (
    id INT PRIMARY KEY,
    movie_name VARCHAR(255),
    year_of_release VARCHAR(255),
    watch_time VARCHAR(255),
    movie_rating VARCHAR(255),
    metascore_of_movie VARCHAR(255),
    gross VARCHAR(255),
    votes VARCHAR(255),
    description TEXT
);"""
cur.execute(create_table_sql)

# truncate_sql = """truncate table movie;"""
# drop_table_sql = """drop table movie"""
# cur.execute(drop_table_sql)

# db에 drop 및 create sql을 실행
conn.commit()

# df 변수에 존재하는 행을 반복 조회하는 과정
# 해당 값이 Nan 값이라면 Mysql에 입력하기 위해 Null 처리를 위해 notnull 함수 사용
for index, r in df.iterrows():
    t = (
        r['id'] if pd.notnull(r['id']) else None,
        r['Movie Name'] if pd.notnull(r['Movie Name']) else None,
        r['Year of Release'] if pd.notnull(r['Year of Release']) else None,
        r['Watch Time'] if pd.notnull(r['Watch Time']) else None,
        r['Movie Rating'] if pd.notnull(r['Movie Rating']) else None,
        r["Metascore of movie"] if pd.notnull(r["Metascore of movie"]) else None,
        r["Gross"].replace(",", "") if pd.notnull(r["Gross"]) else None,
        r["Votes"].replace(",", "") if pd.notnull(r["Votes"]) else None,
        r["Description"] if pd.notnull(r["Description"]) else None)

    # insert sql 포맷에 맞춰 해당 행을 대입하고 sql을 실행
    try:
        cur.execute(insert_sql, t)

    # 만약 예외가 발생한다면 에러를 출력하고 반복 break
    except Exception as e:
        print(t)
        print(e)
        break

# insert sql을 db에 저장하는 과정
conn.commit()

# db와의 커넥션을 종료하는 과정
close_db(conn, cur)
