import string
import time

import pandas as pd
import pymysql
from tqdm import tqdm


class ExcelToMySql:
    def __init__(self):
        global excel_path, file_name
        excel_path = r"C:\Users\flussberg\PycharmProjects\dt_from_excel_to_mysql\data/"
        file_name = "revenue.xlsx"
        self.df = None
        self.conn = None
        self.curs = None
        self.type_ls = []
        self.columns = None
        self.mk_table_sql = None
        self.insert_sql = None

    def load_execl(self):
        sheet_name = str(input("사용할 sheet 이름을 입력하시오 (없을 경우 Enter, 첫번째 sheet가 사용됩니다) : "))
        if sheet_name == "":
            df = pd.read_excel(excel_path + file_name)
        else:
            df = pd.read_excel(excel_path + file_name, sheet_name=sheet_name)
        print(f"첫 열 줄 \n{df.head(10)}")
        header = int(input("컬럼명으로 사용할 행의 번호를 입력하시오 : ")) + 1
        df = pd.read_excel(excel_path + file_name, header=header)
        print(f"첫 열 줄 \n{df.head(10)}")
        df = df.where((pd.notnull(df)), None)
        ls = []
        for i in range(len(df)):
            if None in df.values[i]:
                continue
            else:
                for x, y in enumerate(df.values[i]):
                    ls.append(str(type(y)))
                break
        for i in ls:
            i = i.replace("<class 'pandas._libs.tslibs.timestamps.Timestamp'>", "datetime")
            i = i.replace("<class 'str'>", "varchar(100)")
            i = i.replace("<class 'float'>", "float")
            self.type_ls.append(i)
        self.df = df

    def make_query(self):
        columns = []
        for i in self.df.columns:
            result = i.translate(str.maketrans('', '', string.punctuation))
            columns.append(result)
        sql_ls = []
        for i, j in enumerate(columns):
            sql_ls.append(f"{j} {self.type_ls[i]}")
        mk_table_sql = ', '.join(s for s in sql_ls)
        mk_table_sql = f'create table if not exists {file_name.split(".")[0]}(' \
                   f'id int not null auto_increment, ' \
                   f'{mk_table_sql}, ' \
                   f'primary key (id))'
        print(f"테이블 생성 쿼리 : \n{mk_table_sql}")
        self.mk_table_sql = mk_table_sql
        insert_sql = f'insert into {file_name.split(".")[0]} ({", ".join(s for s in columns)}) values({("%s," * len(columns)).strip(",")})'
        print(f"데이터 입력 쿼리 : \n{insert_sql}")
        self.insert_sql = insert_sql
        self.columns = columns

    def connect_db(self):
        conn = pymysql.connect(host='localhost', user='root', password='root', db='mydb')
        self.curs = conn.cursor()
        self.conn = conn
        print("DB 연결 성공")

    def create_table(self):
        self.curs.execute(self.mk_table_sql)
        print("테이블 생성 완료")

    def insert_data(self):
        pbar = tqdm(total=100)
        for i in range(len(self.df)):
            self.curs.execute(self.insert_sql, tuple(self.df.values[i]))
            pbar.update(100/len(self.df))
        pbar.close()
        self.conn.commit()
        print("데이터 입력 완료")

    def close_db(self):
        self.curs.close()
        self.conn.close()


if __name__ == "__main__":
    e = ExcelToMySql()
    while True:
        print("-"*100)
        menu = ["close",
                "엑셀 파일 데이터 프레임 전환",
                "쿼리 만들기",
                "DB 연결",
                "테이블 생성",
                "데이터 입력",
                "DB 연결 종료"]
        for i, j in enumerate(menu):
            print(f'{i}.{j}')
        choice = int(input('메뉴 입력 : '))
        print(menu[choice])
        if choice == 0:
            break
        elif choice == 1:
            e.load_execl()
        elif choice == 2:
            e.make_query()
        elif choice == 3:
            e.connect_db()
        elif choice == 4:
            e.create_table()
        elif choice == 5:
            e.insert_data()
        elif choice == 6:
            e.close_db()
        else:
            print(" 잘못된 메뉴 입력 ")


