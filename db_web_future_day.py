import os
import re
import time
import glob
import datetime
import random
import requests
import json
import pymysql
import xml.etree.ElementTree as et
import pandas as pd
import numpy as np
import socket
socket.setdefaulttimeout(8)


host = "localhost"
user = "root"
passwd = "root"
db = "quotation"
port = 3306


def _get_conn():
    return pymysql.connect(
        host,
        user,
        passwd,
        db,
        port = port)


def _load_trading_day():
    trading_day = pd.read_csv("trading_day.csv")
    trading_day['dt'] = pd.to_datetime(trading_day['dt']).apply(lambda x: x.date())
    return trading_day


def _create_tb(conn):
    sql = """create table if not exists future_day_web (
    exchange char(3),
    code char(2),
    contract char(6),
    date date,
    open float,
    high float,
    low float,
    close float,
    settle float,
    presettle float,
    vol int,
    opn int,
    turnover float,
    primary key(contract, date))
    """
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()


def download(day):
    def _requests(address, data=None):
        TRYTIME = 5
        trytime = 0
        req = None
        while True:
            try:
                trytime += 1
                if data:
                    req = requests.post(address, data)
                else:
                    req = requests.get(address)
                break
            except:
                time.sleep(1)
                if trytime > TRYTIME:
                    raise Exception("request get trytime > 5 with address: %s" % address)
        if req is None or req.status_code != 200 or req.url.split("/")[-1] == "error_404.html":
            raise Exception("help_requests failed with address ", address)
        return req

    def _download(filename, address, post_data=None, startd=datetime.date(2010,1,1)):
        if not os.path.exists(filename) and day >= startd:
            req = _requests(address, post_data)
            with open(filename, 'w') as outfile:
                outfile.write(req.text)

    # cfe
    cfe_filename = "origin/cfe/cfe_%s.csv" % format(day, "%Y%m%d")
    cfe_address = "http://www.cffex.com.cn/sj/hqsj/rtj/%s/index.xml" % format(day, "%Y%m/%d")
    _download(cfe_filename, cfe_address, startd=datetime.date(2010,4,16))

    # shf
    shf_filename = "origin/shf/shf_%s.csv" % format(day, "%Y%m%d")
    shf_address = "http://www.shfe.com.cn/data/dailydata/kx/kx%s.dat" % format(day, "%Y%m%d")
    _download(shf_filename, shf_address)

    # dce
    dce_filename = "origin/dce/dce_%s.csv" % format(day, "%Y%m%d")
    dce_address = "http://www.dce.com.cn/publicweb/quotesdata/exportDayQuotesChData.html"
    dce_post_dat = {
        'dayQuotes.trade_type': 0,
        'dayQuotes.variety': 'all',
        'exportFlag': 'txt',
        'year': int(format(day, "%Y")),
        'month': int(format(day, "%m")) - 1,
        'day': int(format(day, "%d"))}
    _download(dce_filename, dce_address, dce_post_dat)

    # czc
    czc_filename = "origin/czc/czc_%s.csv" %format(day, "%Y%m%d")
    czc_address = "http://www.czce.com.cn/cn/DFSStaticFiles/Future/%s/%s/FutureDataDaily.txt" % (
            format(day, "%Y"), format(day, "%Y%m%d"))
    _download(czc_filename, czc_address, startd=datetime.date(2018,1,1))


def standard(filename):
    """
    标准化：交易所，品种，合约，ohlc，结算，昨结算，成交量，持仓量，成交金额
    """

    def _standard_record(record):
        for i in range(len(record)):
            for j in range(4, len(record[i])):
                if record[i][j] == 'null' or record[i][j] == '':
                    record[i][j] = None
                if isinstance(record[i][j], str):
                    record[i][j] = float(record[i][j].replace(',', ''))
            for k in range(2, 6):
                if record[i][k] == 0:
                    record[i][k] = None
        return record

    def _cfe_parse(text, day):
        root = et.fromstring(text.lower())
        record = [
            ["cfe",
             c.find("productid").text.strip(),
             c.find("instrumentid").text.strip(),
             c.find("tradingday").text.strip(),
             c.find("openprice").text,
             c.find("highestprice").text,
             c.find("lowestprice").text,
             c.find("closeprice").text,
             c.find("settlementprice").text,
             c.find("presettlementprice").text,
             c.find("volume").text,
             c.find("openinterest").text,
             c.find("turnover").text
             ] for c in root if c.tag == "dailydata"
        ]
        record = _standard_record(record)
        return record

    def _shf_parse(text, day):
        data = json.loads(text.lower())
        pat = re.compile("^[0-9]{4}$")
        record = []
        for v in data['o_curinstrument']:
            if not pat.match(v['deliverymonth']):
                continue
            contract = v['productid'].strip().split("_")[0] + v['deliverymonth']
            record.append([
                'shf',
                v['productid'].strip().split("_")[0],
                contract,
                day,
                v['openprice'],
                v['highestprice'],
                v['lowestprice'],
                v['closeprice'],
                v['settlementprice'],
                v['presettlementprice'],
                v['volume'],
                v['openinterest'],
                None
            ])
        record = _standard_record(record)
        return record

    def _dce_parse(text, day):
        codes = {
            '豆一': 'a',
            '豆二': 'b',
            '豆粕': 'm',
            '豆油': 'y',
            '玉米': 'c',
            '玉米淀粉': 'cs',
            '鸡蛋': 'jd',
            '棕榈油': 'p',
            '聚乙烯': 'l',
            '聚氯乙烯': 'v',
            '胶合板': 'bb',
            '纤维板': 'fb',
            '铁矿石': 'i',
            '焦炭': 'j',
            '焦煤': 'jm',
            '聚丙烯': 'pp',
            '乙二醇': 'eg'
        }
        data = text.lower()
        record = []
        for v in [t.strip() for t in data.split("\n") if len(t.strip()) > 0]:
            v = [t.strip() for t in v.split("\t") if len(t.strip()) > 0]
            if v[0] != '商品名称' and v[0].find('小计') == -1 and v[0] != '总计':
                contract = codes[v[0]] + v[1]
                record.append([
                    'dce',
                    codes[v[0]],
                    contract,
                    day,
                    v[2],
                    v[3],
                    v[4],
                    v[5],
                    v[7],
                    v[6],
                    v[10],
                    v[11],
                    v[13]
                ])
        record = _standard_record(record)
        return record

    def _czc_parse(text, day):
        contract_pat = re.compile("^[a-z]{1,2}[0-9]{3}$")
        data = [v.strip() for v in text.lower().split("\n") if len(v.strip()) > 0]
        record = []
        for i in range(1, len(data)):
            v = [t.strip() for t in data[i].split("|")]
            contract = v[0].lower()
            if contract_pat.match(contract):
                contract = contract[:-3] + '1' + contract[-3:]
                record.append([
                    'czc',
                    contract[:-4],
                    contract,
                    day,
                    v[2],
                    v[3],
                    v[4],
                    v[5],
                    v[6],
                    v[1],
                    v[9],
                    v[10],
                    float(v[12].replace(",", '')) * 10000
                ])
        record = _standard_record(record)
        return record

    def _czc_history_parse(text, day):
        contract_pat = re.compile("^[a-z]{1,2}[0-9]{3}$")
        data = [v.strip() for v in text.lower().split("\n") if len(v.strip()) > 0]
        record = []
        for i in range(1, len(data)):
            v = [t.strip() for t in data[i].split("|")]
            contract = v[1].lower()
            if contract_pat.match(contract):
                contract = contract[:-3] + '1' + contract[-3:]
                record.append([
                    'czc',
                    contract[:-4],
                    contract,
                    v[0],
                    v[3],
                    v[4],
                    v[5],
                    v[6],
                    v[7],
                    v[2],
                    v[10],
                    v[11],
                    float(v[13].replace(",", '')) * 10000
                ])
        record = _standard_record(record)
        return record


    _f = os.path.split(filename)[-1]
    with open(filename, 'r') as infile:
        text = infile.read()

    day = None
    if _f.find("_") == -1:
        func = _czc_history_parse
    else:
        exchange, day = _f[:-4].split("_")
        func = eval("_%s_parse" % exchange)

    record = func(text, day)
    return record


def storage(record):
    conn = _get_conn()
    cur = conn.cursor()
    for v in record:
        v[0] = "'%s'"%v[0]
        v[1] = "'%s'"%v[1]
        v[2] = "'%s'"%v[2]
        v[3] = "'%s'"%v[3]
        sql = "insert into future_day_web value (%s);" %(
            ','.join([str(item) for item in v]))
        sql = sql.replace("None", "null")
        try:
            cur.execute(sql)
        except pymysql.err.IntegrityError:
            return
        except:
            print(sql)
            raise("error")
    conn.commit()
    cur.close()
    conn.close()


def check():
    conn = _get_conn()

    def _check_day():
        # 日期完整性检验
        sql = "select exchange, date, count(*) from future_day_web group by exchange, date order by date"
        data = pd.read_sql(sql, conn)
        trading_day = _load_trading_day()
        for v in trading_day['dt']:
            if v <= data['date'].max():
                if v < datetime.date(2010,4,16):
                    assert len(data[data['date'] == v]) == 3
                else:
                    assert len(data[data['date'] == v]) == 4

    _check_day()
    print("日期完整性检验通过")

    try:
        from WindPy import w
        w.start()
    except ModuleNotFoundError:
        return

    def _wsd(contract, stroption, startd, endd):
        data = w.wsd(contract, stroption, startd, endd)
        data = {'Fields': data.Fields, 'Times': data.Times, 'Data': data.Data}
        names = ["contract", "date"] + [v.lower() for v in data['Fields']]
        record = []
        for i in range(len(data['Times'])):
            record.append([contract, data['Times'][i]] +
                          [data['Data'][j][i] for j in range(len(data['Data']))])
        return pd.DataFrame(record, columns=names)

    def _check_contract(contract):
        print("\tcheck", contract, end=' ')
        sql = """select exchange,contract,date,open,high,low,close,settle,
        presettle as pre_settle, vol as volume, opn as oi 
        from future_day_web where contract='%s'""" % contract
        data = pd.read_sql(sql, conn)

        startd = data['date'].iloc[0]
        endd = data['date'].iloc[-1]
        ncontract = contract+'.' + data['exchange'][0]

        ndata = _wsd(ncontract, "open,high,low,close,settle,pre_settle,volume,oi", startd, endd)
        if data['exchange'][0]=='czc' and ndata['close'].isnull().all():
            ncontract = ncontract[:-8] + ncontract[-7:]
            ndata = _wsd(ncontract, "open,high,low,close,settle,pre_settle,volume,oi", startd, endd)

        if contract[:-4] == 'wr':
            # wr 2018年9月交易所网站没有数据
            ndata = ndata[ndata['volume'].notnull()]
            ndata.index = range(len(ndata))

        assert len(data) == len(ndata)

        for v in ['date', 'open', 'high', 'low', 'close', 'settle', 'volume', 'oi']:
            # pre_settle处理有区别
            data = data.fillna(0.0)
            ndata = ndata.fillna(0.0)
            assert (data[v] == ndata[v]).all() == True

        return True

    maxdate = pd.read_sql("select max(date) as d from future_day_web", conn)['d'][0]
    contracts = list(pd.read_sql("select contract from future_day_web where date='%s'" % maxdate, conn)['contract'])
    contracts = random.sample(contracts, 10)

    for v in contracts:
        print(_check_contract(v))
    print("抽样检查通过")


def main():
    # 确定startd和endd
    startd = datetime.date(2010, 1, 1)
    if int(time.strftime("%H", time.localtime(time.time()))) >= 17:
        endd = datetime.date.today()
    else:
        endd = datetime.date.today() - datetime.timedelta(days=1)

    # 下载原始数据
    trading_day = _load_trading_day()
    trading_day = trading_day[np.logical_and(startd <= trading_day['dt'], trading_day['dt'] <= endd)]
    for v in trading_day['dt'].sort_values(ascending=False):
        download(v)

    # 更新mysql数据库
    conn = _get_conn()
    tables = list(pd.read_sql("show tables", conn)['Tables_in_quotation'])
    if 'future_day_web' not in tables:
        # 全部更新
        _create_tb(conn)
        for f in glob.glob("origin/*/*.csv"):
            record = standard(f)
            storage(record)
    else:
        # 增量更新
        maxdate = pd.read_sql("select max(date) as d from future_day_web", conn)['d'][0]
        for f in glob.glob("origin/*/*.csv"):
            _f = os.path.split(f)[-1]
            if _f.find("_") == -1:
                continue
            else:
                exchange, date = _f[:-4].split("_")
                if date > format(maxdate, "%Y%m%d"):
                    record = standard(f)
                    storage(record)

    check()



if __name__ == "__main__":
    main()