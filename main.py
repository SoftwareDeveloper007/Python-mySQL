import pymysql
import datetime

class main():
    def __init__(self):
        print("Connectting to DB")

        DB_HOST = 'localhost'
        DB_USER = 'root'
        DB_PASSWORD = 'passion1989'
        DB_NAME = 'adams4'

        self.db = pymysql.connect(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)
        self.results = []

    def process_condition(self):
        # prepare a cursor object using cursor() method
        self.cur = self.db.cursor()

        sql = "SELECT beaumaster.keyid, beaumaster.uniquemessageid, " \
              "beaumaster.apptdate, beaumaster.appttime, beaumaster.lenghtappt, beaumaster.apptid," \
              "beaumaster.numappt, beaumaster.deptid, beaumaster.provid, beaumaster.patientid, " \
              "beaumaster.patlastname, beaumaster.patfirstname, beaumaster.phone, beaumaster.newpat, " \
              "beaumaster.smsflag, beaumaster.smsnumber, messages.msgid  " \
              "FROM beaumaster, messages " \
              "where beaumaster.newpat=1 and beaumaster.deptid = messages.epicid and messages.newptflag = 1"


        self.cur.execute(sql)

        results = self.cur.fetchall()
        print("==== First Condition ====")
        for item in results:
            print(item)
        self.results.extend(results)

        sql = "SELECT beaumaster.keyid, beaumaster.uniquemessageid, " \
              "beaumaster.apptdate, beaumaster.appttime, beaumaster.lenghtappt, beaumaster.apptid," \
              "beaumaster.numappt, beaumaster.deptid, beaumaster.provid, beaumaster.patientid, " \
              "beaumaster.patlastname, beaumaster.patfirstname, beaumaster.phone, beaumaster.newpat, " \
              "beaumaster.smsflag, beaumaster.smsnumber, messages.msgid  " \
              "FROM beaumaster, messages " \
              "where beaumaster.newpat=0 and beaumaster.deptid = messages.epicid and beaumaster.apptid = messages.apptcode and beaumaster.provid = messages.drid"

        self.cur.execute(sql)

        results = self.cur.fetchall()
        print("\n==== Second Condition ====")
        for item in results:
            print(item)
        self.results.extend(results)

        print("\n==== Total Result ====")
        self.final_results = []
        now = datetime.datetime.now()
        today = now.strftime("%Y%m%d")
        for item in self.results:
            item = item + (today, "")
            print(item)
            self.final_results.append(item)

        print('Successfully Done!!!')

    def make_dprocess(self):
        for row in self.final_results:
            sql = "INSERT INTO dprocess(beaukeyid, uniquemessageid, apptdate, appttime, lenghtappt, apptid, numappt, deptid, provid, patientid, " \
              "patlastname, patfirstname, phone, newpat, smsflag, smsnumber, msgid, datetocall, status_) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, " \
                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s); "
            self.cur.execute(sql, (
                row[0], row[1], ''.join(str(row[2]).split('-')), ''.join(str(row[3]).split(':')), row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18]
            ))
            self.db.commit()
            print(row[0], row[1], str(row[2]), str(row[3]), int(row[4]), int(row[5]), int(row[6]), row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15],
                row[16], row[17], row[18])


if __name__ == '__main__':
    app = main()
    app.process_condition()
    app.make_dprocess()


