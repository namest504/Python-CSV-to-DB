import pymysql

from pymysql.constants.CLIENT import MULTI_STATEMENTS

# pymysql 라이브러리를 사용해서 db와 커넥션을 연결하는 함수
def open_db(dbname='DS2023'):
    conn = pymysql.connect(host='localhost',
                           user='root',
                           passwd='1234',
                           db="movie",
                           client_flag=MULTI_STATEMENTS,
                           charset='utf8mb4')

    cursor = conn.cursor(pymysql.cursors.DictCursor)

    return conn, cursor

# db와의 커넥션을 종료하는 함수
def close_db(conn, cur):
    cur.close()
    conn.close()

# 직접 실행될 경우 db 커넥션 연결과 종료 실험
if __name__ == '__main__':
    conn, cur = open_db()
    close_db(conn, cur)
