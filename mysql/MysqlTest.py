import BaseConfig
import pymysql

# 打开数据库连接
db = pymysql.connect(host='localhost',
                     port=3306,
                     user='root',
                     password='root',
                     database='python_db')

# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()

# 使用 execute()  方法执行 SQL 查询
cursor.execute("SELECT VERSION()")

# 使用 fetchone() 方法获取单条数据.
data = cursor.fetchone()

print("Database version : %s " % data)

# 关闭数据库连接
db.close()