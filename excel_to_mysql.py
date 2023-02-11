import pandas as pd
import pymysql


class ExcelToMySql:
    def __init__(self):
        self.excel_path = r"C:\Users\Administrator\PycharmProjects\pythonProject\revenue.xlsx"
        self.df = None
        self.conn = None
        self.curs = None

    def load_execl(self):
        self.df = pd.read_excel(self.excel_path, header=2)
        self.df = self.df.where((pd.notnull(self.df)), None)
        print(self.df)
        print("#"*100)
        print(self.df.index)
        print("#"*100)
        print(self.df.columns)
        print("#"*100)
        print(self.df.values)
        print("#"*100)
        for i in range(len(self.df.values[0])):
            print(f"'{self.df.values[0][i]}' type is {type(self.df.values[0][i])}")

    def connect_db(self):
        self.conn = pymysql.connect(host='localhost', user='root', password='root', db='mydb')
        self.curs = self.conn.cursor()

    def create_table(self):
        mk_table = 'create table if not exists revenue(' \
                   'id int not null auto_increment,' \
                   'cal_date datetime,' \
                   'cal_day varchar(100),' \
                   'holiday varchar(100),' \
                   'dayonoff varchar(100),' \
                   'product varchar(100),' \
                   'price float not null,' \
                   'primary key (id)' \
                   ')'
        self.curs.execute(mk_table)

    def insert_data(self):
        sql = 'insert into revenue (cal_date, cal_day, holiday, dayonoff, product, price) values(%s, %s, %s, %s, %s, %s)'
        for idx in range(len(self.df)):
            self.curs.execute(sql, tuple(self.df.values[idx]))
        self.conn.commit()

    def close_db(self):
        self.curs.close()
        self.conn.close()


if __name__ == '__main__':
    e = ExcelToMySql()
    while True:
        print('-'*100)
        menu = int(input("choose menu: "))
        if menu == 0:
            print('close')
            break
        elif menu == 1:
            print("엑셀 파일 데이터 프레임 전환")
            e.load_execl()
        elif menu == 2:
            print("DB 연결")
            e.connect_db()
        elif menu == 3:
            print("테이블 생성")
            e.create_table()
        elif menu == 4:
            print("데이터 입력 시작")
            e.insert_data()
            print("데이터 입력 완료")
        elif menu == 5:
            print("DB 연결 종료")
            e.close_db()
        else:
            print("wrong menu")


