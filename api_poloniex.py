""" Python v.3.8.7  PostgresSQL v.13  API-Poloniex web_api asyncio js18user """

import asyncio
import websockets
import json
import random
import time
import math
import psycopg2
from psycopg2 import Error


class PrError(Exception):
    pass


class ClientError(PrError):
    pass


""" It is a information for operate  with API 

https://docs.poloniex.com/#websocket-api     API doc
https://docs.poloniex.com/#ticker-data       API doc

wss://api2.poloniex.com                      Connection with API
{"command": "subscribe", "channel": 1002}    Ticker channel request
[1002,1]                                     Ticker channel confirmation
                                             Information structure
[ <id>, null, [ <currency pair id>, "<last trade price>", "<lowest ask>", "<highest bid>", "<percent change in last 24 hours>", "<base currency volume in last 24 hours>", "<quote currency volume in last 24 hours>", <is frozen>, "<highest trade price in last 24 hours>", "<lowest trade price in last 24 hours>", "<post only>", "<maintenance mode>" ], ... ]
                                             For example:                                             
[1002,null,[149,"219.42870877","219.85995997","219.00000016","0.01830508","1617829.38863451","7334.31837942",0,"224.44803729","214.87902002",0,0]]

Ended information for operate """


async def wbs_analyze(number):
    """  It is a main procedure for operate  """

    print(f'Start of process ( {number} )')

    try:
        exchange, timestamp, instrument, big, ask = 0, 1, 2, 3, 4

        ticker_number = 0
        skip = '\n'
        list_dicts, dicts = list(), dict()
        list_confirmation = [1002, 1]
        list_dicts.clear()

        interval = 8
        """ According to the terms of reference, the interval is 8 seconds """

        query = 'INSERT INTO tickers (exchange, timestamp, symbol, big, ask) VALUES (%s,%s,%s,%s,%s)'

        connection = psycopg2.connect('dbname=fintech user=postgres password=aa4401 host=localhost port=5432')
        await asyncio.sleep(random.randint(0, 2) * 0.0001)

        async with websockets.connect('wss://api2.poloniex.com') as api:
            """ Socket subscription with tickers set to request """

            start_time = time.time()
            """ run until interrupt --> Ctrl C """

            request = json.dumps({"command": "subscribe", "channel": 1002})

            await api.send(request)

            confirmation = json.loads(await api.recv())

            if confirmation == list_confirmation is False:
                return()
            else:
                pass

            while 1:

                ticker = json.loads(await api.recv())[2]
                """ Reading ticker, type(ticket) ---> list  """
                # print(ticker)

                ticker_number += 1

                """ Create dicts ---> operate dictionary  """
                dicts[exchange] = 'poloniex'
                dicts[timestamp] = int(time.time())
                dicts[instrument] = ticker[0]
                dicts[big] = float(ticker[8])
                dicts[ask] = float(ticker[9])

                """ Process control printing """
                # print(dicts)

                """ Creating list of dicts for insert in tickers table(db = fintech, PostgreSQL)"""
                """ New instrument(dicts) is appended to list """
                """ Update dicts information in list of dicts if instrument is repeated """
                if ticker_number == 1:
                    list_dicts.append(dicts.copy())

                else:
                    length_list_dicts = len(list_dicts)

                    if length_list_dicts >= 1:
                        number_on_the_list, update_indicator = 0, 0

                        while (number_on_the_list < length_list_dicts) and (update_indicator == 0):

                            if list_dicts[number_on_the_list][instrument] == dicts[instrument]:

                                list_dicts[number_on_the_list][timestamp] = dicts[timestamp]
                                list_dicts[number_on_the_list][big] = dicts[big]
                                list_dicts[number_on_the_list][ask] = dicts[ask]

                                update_indicator = 1

                            else:
                                pass

                            number_on_the_list += 1

                        else:
                            pass

                        if update_indicator == 0:
                            list_dicts.append(dicts.copy())

                        else:
                            pass

                    else:
                        pass

                await asyncio.sleep(random.randint(0, 2) * 0.00001)

                """ the process continues for a time interval in sec """
                if (time.time() - start_time) >= interval:

                    length_list_dicts = len(list_dicts)

                    """ After the time interval has elapsed, 
                        the list of dictionaries is converted into a list of tuples 
                        and written to the database """

                    # [print(list_dicts[x]) for x in range(length_list_dicts)]
                    """ process control printing """

                    s_time = time.time()

                    """ Writing tickers to the database """

                    with connection.cursor() as cursor:

                        await asyncio.sleep(random.randint(0, 2) * 0.00001)

                        cursor.executemany(query, [tuple(v) for v in map(dict.values, list_dicts)])

                        await asyncio.sleep(random.randint(0, 2) * 0.00001)

                        connection.commit()

                        await asyncio.sleep(random.randint(0, 2) * 0.00001)

                    list_dicts.clear()

                    """ Process control printing """
                    print(skip,
                          f'process number is  --->, ({number})', skip,
                          'time for insert in db ->', math.floor((time.time() - s_time) * 1000), skip,
                          'actual time in m/sec -->',
                          math.floor(((time.time() - start_time - interval) * 1000)), skip,
                          'com tickers by time --->', ticker_number, skip,
                          'com tickers insert  --->', length_list_dicts, skip)

                    ticker_number = 0

                    start_time = time.time()

                else:
                    pass

            else:
                pass

    except KeyboardInterrupt:
        pass

    except (ClientError, Error, psycopg2.DatabaseError, GeneratorExit) as err:

        print('error : ', err)
        pass

    finally:
        cursor.close()

        print(f'End of process ( {number} )')

    return ()


async def asynchronous():

    tasks = [asyncio.ensure_future(wbs_analyze(number)) for number in range(2)]
    await asyncio.wait(tasks)
    return ()


def main():
    try:

        asyncio.get_event_loop().run_until_complete(asynchronous())

    except KeyboardInterrupt:
        pass

    except (ClientError, GeneratorExit) as err:

        print('error : ', err)
        pass

    finally:
        print('ok')
    return ()


if __name__ == "__main__":
    main()

exit()
