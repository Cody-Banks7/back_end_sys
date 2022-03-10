from pyhive import hive

conn = hive.Connection(host='10.20.120.63', port=10000, username='hive',password='dbgroupDBGROUP1314', database='default',auth='NOSASL')
cursor = conn.cursor()

def get_data():
    query_sql = "select * from test"
    cursor.execute(query_sql)
    for result in cursor.fetchall():
        print(result)


def main():
    get_data()

if __name__ == "__main__":
    main()