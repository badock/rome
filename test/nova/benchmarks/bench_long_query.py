__author__ = 'jonathan'

import test.nova._fixtures as models
from lib.rome.core.orm.query import Query
from lib.rome.core.orm.query import and_, or_

current_milli_time = lambda: int(round(time.time() * 1000))

from threading import Thread
import time

class SelectorThread(Thread):
    def __init__(self):
        Thread.__init__(self)

    def run(self):
        """SELECT fixed_ips.id FROM fixed_ips WHERE 1==1  and (fixed_ips.network_id == 18) and (fixed_ips.id == 90)"""
        query = Query(models.FixedIp, models.FixedIp.network_id==18, models.FixedIp.id==90)
        # query = Query(models.FixedIp).filter(models.FixedIp.network_id==18).filter(models.FixedIp.id==90)
        result = query.all()

if __name__ == '__main__':

    import logging
    logging.getLogger().setLevel(logging.DEBUG)

    n = 100

    for i in range(0, n):
        thread_1 = SelectorThread()
        thread_1.start()
        thread_1.join()
