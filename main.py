from pusher import RedisPusher, ArcticPusher
from config import config_dict

pusher_constructor_map = {
    'redis': RedisPusher,
    'arctic': ArcticPusher
}

if __name__ == '__main__':
    pusher_type = config_dict['pusher_type']
    if pusher_type not in pusher_constructor_map.keys():
        raise Exception(f'not support pusher type: {pusher_type}')
    pusher = pusher_constructor_map[pusher_type](config_dict)
    pusher.start()
