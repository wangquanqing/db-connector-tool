import jaydebeapi

# print("Python JayDeBeApi JDBC 测试程序开始运行.")
# conn = jaydebeapi.connect(
#     "org.postgresql.Driver",
#     "jdbc:postgresql://172.26.223.102:5432/db_station",
#     ["cvicse", "Cvicsejszx@2022"],
#     "D:/wangq/Documents/postgresql-42.7.8.jar",
# )
# print(conn.__dict__)
# mycursor = conn.cursor()
# mycursor.execute("select version()")
# rows = mycursor.fetchall()
# print(rows)
# mycursor.close()
# conn.close()
#
# print("Python JayDeBeApi JDBC 测试程序结束运行.")

# print("Python JayDeBeApi JDBC 测试程序开始运行.")
# conn = jaydebeapi.connect(
#     "com.mysql.cj.jdbc.Driver",
#     "jdbc:mysql://localhost:3306/db_station",
#     ["cvicse", "Cvicsejszx@2022"],
#     "D:/wangq/Documents/mysql-connector-j-9.5.0.jar",
# )
# mycursor = conn.cursor()
# mycursor.execute("select version()")
# rows = mycursor.fetchall()
# print(rows)
# mycursor.close()
# conn.close()
# 
# print("Python JayDeBeApi JDBC 测试程序结束运行.")
import os
os.environ['JAVA_HOME'] = 'C:\\Program Files\\Microsoft\\jdk-25.0.0.36-hotspot\\bin'
print("Python JayDeBeApi JDBC 测试程序开始运行.")
conn = jaydebeapi.connect(
    "com.gbasedbt.jdbc.Driver",
    "jdbc:gbasedbt-sqli://127.0.0.1:9088/db_station:GBASEDBTSERVER=gbase01",
    ["cvicse", "Cvicsejszx@2022"],
    "D:/wangq/Documents/gbasedbtjdbc_3.6.3_3X2_1_377ee9.jar",
)
print(conn.__dict__)
mycursor = conn.cursor()
mycursor.execute("select dbinfo('version_gbase','full') from dual;")
rows = mycursor.fetchall()
print(rows)
mycursor.close()
conn.close()

print("Python JayDeBeApi JDBC 测试程序结束运行.")
