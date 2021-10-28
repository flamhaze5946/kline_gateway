import json


with open(r'config.json', encoding='utf-8') as config_file:
    config_dict = json.load(config_file)

timezone = config_dict['timezone']
clean_klines = config_dict['clean_klines']
klines_save_days = config_dict['klines_save_days']
klines_web_fetch_worker = config_dict['klines_web_fetch_worker']
save_buffer_millseconds = config_dict['save_buffer_millseconds']
