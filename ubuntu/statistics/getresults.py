#!flask/bin/python
from flask import Flask, jsonify
from flask import abort

from flask import request
app = Flask(__name__)
import json

data = []
with open('/home/ubuntu/Analyser/Result/prolific_user_of_time.txt') as f:
    for line in f:
        data.append(json.loads(line))

@app.route('/todo/api/v1.0/users/', methods=['GET'])
def get_most_prolific_user():
    if request.method == 'GET':
         year = request.args.get('year', 2015)
         sentiment = request.args.get('sentiment', 'positive')
         hour = request.args.get('hour', 1)
         task = [task for task in data if task['year'] == str(year) and task['sentiment'] == sentiment and task['time'] == hour]
         if len(task) == 0:
            abort(404)
         return jsonify({'user':task[0]['user']['url']})

@app.route('/todo/api/v1.0/users/sentiment/<string:sentiment>', methods=['GET'])
def get_user_on_mood(sentiment):

         task = [task for task in data if task['sentiment'] == sentiment]
         if len(task) == 0:
            abort(404)
         return jsonify({'user':task})



@app.route('/todo/api/v1.0/users/year/<int:year>', methods=['GET'])
def get_users_based_on_year(year):

         task = [task for task in data if task['year'] == str(year)]
         if len(task) == 0:
            abort(404)
         return jsonify({'user':task})


@app.route('/todo/api/v1.0/users/hour/<int:time>', methods=['GET'])
def get_users_based_on_time(time):

         task = [task for task in data if task['time'] == time]
         if len(task) == 0:
            abort(404)
         return jsonify({'user':task})




if __name__ == '__main__':
    app.run(debug=True,port=1988)

