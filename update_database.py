from library.earnings import Earnings
from library.database import JsonManager
import os
import json


if __name__ == '__main__':
    json_path = 'E:\\Libraries\\Documents\\Stock Assistant\\database\\json_db - 15'

    with open(os.path.join(json_path, 'blacklist.json'), 'r') as read:
        blacklist = json.load(read)

    db = JsonManager(
        path_to_database=os.path.join(json_path, 'stocks'),
        api_key='bYoNpNAQNbpLSKQaMkcwrI68rniyZQDXL7B7aqYNPsHMrr0CRLIe3UYCfkHF',
        tolerance=0.2,
        incomplete_handler='move',
        move_to=os.path.join(json_path, 'test'),
        parallel_mode='multiprocess',
        blacklist='move',
        surpress_message=True
    )

    earnings = Earnings('earnings.json')
    earnings.update()
    earnings.save()

    to_update = [ticker for ticker in earnings.all_stocks() if ticker not in db.all_tickers]
    to_update += db.all_tickers
    to_update = set(to_update) - set(blacklist)

    db.download_list(list(to_update))
    with open(os.path.join(json_path, 'blacklist.json'), 'w') as write:
        json.dump(db.blacklist, write, indent=4)

    market_db = JsonManager(
        path_to_database=os.path.join(json_path, 'markets'),
        api_key='bYoNpNAQNbpLSKQaMkcwrI68rniyZQDXL7B7aqYNPsHMrr0CRLIe3UYCfkHF',
        tolerance=0.2,
        incomplete_handler='ignore',
        parallel_mode='multiprocess',
        surpress_message=True
    )
    market_db.update()

    input('Finished Updating, press enter to exit')
    os.sys.exit()
