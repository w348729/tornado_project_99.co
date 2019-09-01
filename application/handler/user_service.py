import tornado.web
import logging
import json
import time


class UsersHandler(tornado.web.RequestHandler):
    def write_json(self, obj, status_code=200):
        self.set_header("Content-Type", "application/json")
        self.set_status(status_code)
        self.write(json.dumps(obj))

    def db_commit(self):
        try:
            self.application.db.commit()
        except BaseException as e:
            self.application.db.rollback()

    @tornado.gen.coroutine
    def get(self, user_id=None):
        page_num = self.get_argument("page_num", 1)
        page_size = self.get_argument("page_size", 10)
        try:
            page_num = int(page_num)
        except:
            logging.exception("Error while parsing page_num: {}".format(page_num))
            self.write_json({"result": False, "errors": "invalid page_num"}, status_code=400)
            return

        try:
            page_size = int(page_size)
        except:
            logging.exception("Error while parsing page_size: {}".format(page_size))
            self.write_json({"result": False, "errors": "invalid page_size"}, status_code=400)
            return
        if user_id:
            try:
                user_id = int(user_id)
            except:
                self.write_json({"result": False, "errors": "invalid user_id"}, status_code=400)
                return
        select_sql = 'select * from users'
        if user_id:
            select_sql = select_sql + ' where user_id = ? '
        limit = page_size
        offset = (page_num - 1) * page_size
        select_sql = select_sql + ' order by created_at DESC limit ? offset ? '
        if user_id:
            args = (user_id, limit, offset)
        else:
            args = (limit, offset)
        cursor = self.application.db.cursor()
        results = cursor.execute(select_sql, args)
        users_list = []
        for each in results:
            fields = ['id', 'name', 'created_at', 'updated_at']
            temp = {
                f: each[f] for f in fields
            }
            if not user_id:
                users_list.append(temp)
        if users_list:
            self.write_json({"result": True, "user": users_list})
        else:
            self.write_json({"result": True, "user": temp})

    @tornado.gen.coroutine
    def post(self):
        user_name = self.get_argument('user_name')
        timestamp = int(time.time() * 1e6)
        if not user_name:
            self.write_json({'result': False, 'errors': 'invalid user name'}, status_code=400)
        sql_insert = 'insert into users ' \
                     '(name, created_at, updated_at) ' \
                     'values (?, ?, ?)'
        cursor = self.application.db.cursor()
        cursor.execute(sql_insert, (user_name, timestamp, timestamp))
        self.db_commit()
        if not cursor.lastrowid:
            self.write_json({"result": False, "errors": ["Error while user listing to db"]}, status_code=500)
            return
        user_detail = {
            'id': cursor.lastrowid,
            'name': user_name,
            'created_at': timestamp,
            'updated_at': timestamp
        }
        self.write_json({"result": True, "user": user_detail})