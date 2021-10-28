from pusher import RedisPusher
from config import config_dict

if __name__ == '__main__':
    pusher_type = config_dict['pusher_type']
    if pusher_type == 'redis':
        pusher = RedisPusher(config_dict)
    else:
        raise Exception(f'not support pusher type: {pusher_type}')
    pusher.start_stream_update()

