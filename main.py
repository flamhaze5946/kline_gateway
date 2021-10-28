from pusher import RedisPusher
from config import config_dict

if __name__ == '__main__':
    pusher_type = config_dict['pusher_type']
    clean_klines = config_dict['clean_klines']
    klines_save_days = config_dict['klines_save_days']
    if pusher_type == 'redis':
        pusher = RedisPusher(config_dict)
    else:
        raise Exception(f'not support pusher type: {pusher_type}')
    pusher.start()
