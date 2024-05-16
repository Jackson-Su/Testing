import pandas as pd
from flask import Flask, jsonify
import mysql.connector

# 创建一个web-api框架
app = Flask(__name__)


@app.route('/api/datas', methods=['GET'])
def get_data():
    # 连接MySql数据库
    connection = mysql.connector.connect(user='test_user', password='08020902', host='localhost')

    # 创建一个MySql游标
    cursor = connection.cursor()

    # 创建自主化模式选择
    def database_cal(database_name):
        # 选择要操作的数据库
        cursor.execute(f"USE `{database_name}`")

        # 执行查询获取所有表名
        cursor.execute("SHOW TABLES")

        # 获取查询结果
        tables = cursor.fetchall()

        # 遍历每个表并读取数据
        dfs = {}
        '''
        将每个表的数据转换为DataFrame，然后再将DataFrame转换为字典格式，存储在 dfs 字典中。
        最后返回的是 dfs 的 JSON 格式数据。这样可以确保数据能够被正确序列化为 JSON 格式。
        '''
        for table in tables:
            table_name = table[0]

            # 查询表中的所有数据
            query = f"SELECT * FROM `{table_name}`"
            cursor.execute(query)

            # 获取查询结果
            rows = cursor.fetchall()

            # 将数据转换为DataFrame
            '''
            （1）cursor.description 返回一个元组的列表，每个元组包含了列的信息，比如列名、数据类型等。
            （2）[i[0] for i in cursor.description] 这部分代码是一个列表推导式，用来从cursor.description中提取出列名，存放在一个列表中。
            （3）pd.DataFrame(rows, columns=...) 使用提取出来的列名作为DataFrame的列，rows作为数据，创建一个DataFrame对象。
            '''
            df = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])

            # 将DataFrame转换为字典
            df_dict = df.to_dict(orient='records')

            # 添加到字典中
            dfs[table_name] = df_dict
        return dfs
    dfs = database_cal('cci-databases')
    idsutry = database_cal('行业数据')
    rate_strategy = database_cal('策略盈亏情况')
    rate_industry = database_cal('策略行业盈亏情况')
    financial_day = database_cal('财报日')

    # 关闭游标和连接
    cursor.close()
    connection.close()
    # 合并两个字典
    combined_data = {
        'cci-databases': dfs,
        '行业数据': idsutry,
        '策略盈亏': rate_strategy,
        '策略行业盈亏': rate_industry,
        '财报日': financial_day
    }

    # 返回字典数据的JSON格式
    return jsonify(combined_data)


if __name__ == '__main__':
    app.run(host='0.0.0.0')
