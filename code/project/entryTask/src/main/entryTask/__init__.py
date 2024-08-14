import json
import random
from datagenkafka import log_kafka
import time

# order_id 订单id，15位随机数字，不重复出现
# user_id 用户id,11位随机数字，可重复出现
# order_tz 时区，用户时区，从列表中选择，为简单化仅选择北京时间
# amount 付费金额，0-10000之间随机数字
# currency 货币种类，默认人人民币
# channel_id 渠道ID，1到200中随机数字，可重复
# order_time 订单时间，默认数据产生时间
while (True):
    data = {}
    data['order_id'] = random.randint(100000000000000, 999999999999999)
    data['user_id'] = random.randint(1, 5000)
    data['order_tz'] = 'beijing'
    data['amount'] = random.randint(1, 10000)
    data['currency'] = 'rmb'
    data['channel_id'] = random.randint(0, 200)
    data['order_time'] = int(time.time())
    data = json.dumps(data)
    log_kafka(data)
    sleep_time = random.randint(3, 10)  # 3-10秒内随机生产一条订单
    time.sleep(sleep_time)

