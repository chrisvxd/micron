import json
import time
import threading
import sys

def none_to_blank(val=None, _list=None, _dict=None):
    """
    Converts None values to empty strings
    Wrapper for _none_to_blank that accepts lists and dicts
    """

    def _none_to_blank(val):
        return '' if val is None else val

    if _list:
        return [_none_to_blank(_val) for _val in _list]
    if _dict:
        return {key: _none_to_blank(_val) for key, _val in _dict.items()}
    return '' if val is None else val


class ProcessMsg(threading.Thread):

    def __init__(self, micron, data, func, msg_key, db_key_prefix, *args, **kwargs):
        self.micron = micron
        self.data = data
        self.msg_dict = {
            'error': None,
            'id': data['id'],
            'obj': {},
            'tracking': data['tracking'],
            'status_code': 200,  # using http status codes
        }
        self.func = func
        self.msg_key = msg_key
        self.db_key_prefix = db_key_prefix
        super(ProcessMsg, self).__init__(*args, **kwargs)

    def msg_all_keys(self):
        if self.msg_key:
            self.micron.msg(self.msg_key, self.msg)

        if self.db_key_prefix:
            self.micron.r.set(self.db_key_prefix + self.data['id'], self.msg)
            self.micron.r.expire(self.db_key_prefix + self.data['id'], self.micron.redis_expiration)

    def run(self, *args, **kwargs):
        try:
            self.msg_dict['obj'] = self.func(meta=self.data, **self.data['obj'])
            self.msg_dict['obj'] = none_to_blank(_dict=self.msg_dict['obj'])
            self.msg = json.dumps(self.msg_dict)
            self.msg_all_keys()
        except Exception as e:
            self.msg_dict['status_code'] = 500
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.msg_dict['error'] = {
                'internal_message': str(e),
                'exc_type': str(exc_type),
                'exc_value': str(exc_value),
                'exc_traceback': str(exc_traceback),
                'user_message': "Something went wrong!",
                'func': self.func.__name__,
                'msg_key': self.msg_key if self.msg_key else None,
            }
            self.msg = json.dumps(self.msg_dict)
            self.micron.msg('msg:error', self.msg)
            self.msg_all_keys()
            self.micron.active_threads -= 1
            raise

        self.micron.active_threads -= 1


class Micron(object):

    def __init__(self, id, r, mapping={}, redis_expiration=30, max_threads=4):
        self.id = id
        self.r = r
        self.mapping = mapping
        self.redis_expiration = redis_expiration
        self.active_threads = 0
        self.max_threads = max_threads

        # Clear old hooks
        for key in self.r.keys('hooks:*'):
            hook_ids = list(set(self.r.lrange(key, 0, -1)))

            if self.id in hook_ids:
                hook_ids.remove(self.id)

            # Regenerate hooks
            self.r.delete(key)
            for hook_id in hook_ids:
                self.r.rpush(key, hook_id)

    def map(self, request_msg_key, fn, response_msg_key=None,
            db_key_prefix=None):
        self.mapping["%s:%s" % (request_msg_key, self.id)] = (
            fn,
            response_msg_key,
            db_key_prefix
        )

        # Register this queue
        self.r.rpush('hooks:%s' % request_msg_key, self.id)

    def msg(self, key, msg):
        hook_ids = self.r.lrange('hooks:%s' % key, 0, -1)

        for hook_id in hook_ids:
            new_key = "%s:%s" % (key, hook_id)
            self.r.rpush(new_key, msg)
            self.r.expire(new_key, self.redis_expiration)

    def db(self, key, msg):
        self.r.set(key, msg)
        self.r.expire(key, self.redis_expiration)

    @property
    def mapped_keys(self):
        return self.mapping.keys()

    def wait_for_slot(self):
        # Wait for a thread slot to become available
        # Other services may time out, but that's a scaling issue and can be solved
        # by increasing the max_threads limit
        print "{count} active threads".format(count=self.active_threads)
        waited_for_slot = False
        if self.active_threads >= self.max_threads:
            print "Waiting for thread slot..."
            waited_for_slot = True
        while self.active_threads >= self.max_threads:
            time.sleep(0.1)
        if waited_for_slot:
            print "New slot available!"

    def go(self):
        # Watch message queue
        print 'Watching message broker...'
        while True:

            self.wait_for_slot()

            print "Waiting for message..."
            key, msg = self.r.blpop(self.mapped_keys)

            data = json.loads(msg)
            func, msg_key, db_key_prefix = self.mapping[key]

            print '(%s) %s \t----->\t %s' % (data['id'], key, func.__name__)

            # Fire up a message worker
            self.active_threads += 1
            ProcessMsg(self, data, func, msg_key, db_key_prefix).start()
