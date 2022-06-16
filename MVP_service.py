#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import datetime
import warnings

from flask import Flask, jsonify, render_template, request
from keras.models import load_model
import numpy as np
import pymysql

from config import db_name, host, logger, model_name, password, port, user

warnings.filterwarnings("ignore")

app = Flask(__name__)

try:
    connection = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db_name,
        cursorclass=pymysql.cursors.DictCursor
    )
    logger.info("Успешное подключение к БД.")
    try:
        with connection.cursor() as cursor:
            create_table_query = "CREATE TABLE requests (id int AUTO_INCREMENT,"                                  "user_id varchar(32) ,"                                  "req_date datetime,"                                  "vector varchar(256),"                                  "action_list varchar(32), "                                  "PRIMARY KEY (id));"
            cursor.execute(create_table_query)
            connection.commit()
            logger.info("Успешное создание таблицы")
    except Exception as exc:
        logger.error(f"Таблица уже создана. Ошибка: {exc}")
except Exception as exc:
    logger.error(f"Не удалось подключиться к БД. Ошибка: {exc}")


def insert_values(user_id, req_date, vector, test_actions):
    try:
        with connection.cursor() as cursor:
            insert_query = "INSERT INTO requests (user_id, req_date, vector, action_list) VALUES (%s, %s,"                            "%s, %s);"
            cursor.execute(insert_query, (user_id, req_date.strftime('%Y-%m-%d %H:%M:%S'), vector, test_actions))
            connection.commit()
    except Exception as exc:
        logger.error(f"Ошибка при добавлении данных: {exc}")


def select_all():
    try:
        with connection.cursor() as cursor:
            cursor.execute('select * from requests')
            dataset = cursor.fetchall()
            for data in dataset:
                data.update({'req_date': datetime.datetime.strftime(data.get('req_date'), '%Y-%m-%d %H:%M:%S')})
            connection.commit()
            return dataset
    except Exception as exc:
        logger.error(f"Ошибка при выборке данных: {exc}")


@app.route('/')
def first_page():
    return render_template('index.html')


@app.route('/staff', methods=['GET'])
def get_staff():
    dataset = select_all()
    for data in dataset:
        logger.debug(data)
    return jsonify(dataset)


@app.route('/staff', methods=['POST'])
def get_action():
    output = {}
    action_list = []
    model = load_model(model_name)
    req = request.json
    for data in req.keys():
        if "action" in data:
            for el in req[data].split(' '):
                action_list.append(el)
    embedding_output = model.predict(action_list)
    kumulative_sum = np.zeros(len(embedding_output[0]))

    for every_action_embedding in embedding_output:
        kumulative_sum += every_action_embedding
    for el in req.keys():
        if "id" in el:
            output['user_id'] = req[el]
    output['actions_list'] = action_list
    output['vector'] = kumulative_sum.tolist()
    logger.debug(type(output['vector']))
    output['date'] = datetime.datetime.now()
    insert_values(str(output['user_id']), output['date'],
                  ' '.join(str(el) for el in output['vector']), ' '.join(str(el) for el in output['actions_list']))
    return jsonify(output)


if __name__ == '__main__':
    app.run()

