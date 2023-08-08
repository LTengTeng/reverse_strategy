'''
本模块主要丈现功能为从同花顺提取对应现券、国债期货与利率互换的代码:现券为国债、政策性金融债《剔除续发、增发、特别、抗疫、美元债、柜台债):商业银行债仅保留五人行的二级资本债与水续债:
国债期货保留当期合约代码;
利率互换暂时从利率数据库中提取。
'''
import numpy as np
import pandas as pd
import pickle
from datetime import date
from dateutil.relativedelta import relativedelta
import json
import requests
from _iFinD_accessToken import thsHeaders
from _iFinD_Api_Config import *


def getsectorID(Block=BlockID['现券']):
    Today = str(date.today())
    newSectorID = {}

    with open('数据/板块成分.pickle','rb') as file:
        SectorID = pickle.load(file)
    file.close()

    for i in Block.keys():
        if i == '国债':
            value = Block[i]
            thsData = THS_DataPool('block', Today + ';' + value, 'thscode:Y,security_name:Y')
            _newSectorID_ = pd.DataFrame(thsData['tables'][0]['table'])

            _newSectorID_ = \
                _newSectorID_[~_newSectorID_['SECURITY_NAME'].strcontains("续|增|储蓄|专项|定向|特别|柜台|抗疫|美元")]
            _newSectorID = \
            _newSectorID_[_newSectorID_['THSCODE'].str.contains('.IB')]

            _preSectorID = SectorID['国债']
            _SectorID = pd.concat([_preSectorID, _newSectorID], axis=0)
            newSectorID['国债'] = _SectorID[~_SectorID.THSCODE.duplicated()].reset_index(drop=True)

        elif i == '政策性金融债':
            value = Block[i]
            thsData = THS_DataPool('block', Today +';' + value, 'thscode: Y, security_name: Y')
            _newSectorID_ = pd.DataFrame(thsData['tables'][0]['table'])
            _newSectorID_ = \
            _newSectorID_[~_newSectorID_['SECURITY_NAME'].
                          str.contains('续| 增 | 定向 | 储蓄 | 专项 | 特别 | 计划 | 柜台 | 抗疫 | 美元 | 金融 | 证券')]
            _newSectorID_ = \
            _newSectorID_[_newSectorID_['THSCODE'].str.contains('.IB')]

            for name in ['国开','农发','进出']:
                newSectorID= _newSectorID_[_newSectorID_['SECURITYNAME'].str.contains(name)].copy()
                _preSectorID = SectorID[name]
                _SectorID = pd.concat([_preSectorID,newSectorID_], axis=0)
                newSectorID[name] = _SectorID[~_SectorID.THSCODE.duplicated()].reset_index(drop=True)
        elif i =='商业银行二永债':
            _temp_ = pd.DataFrame()
            for key, value in Block[i].items():
                thsData = THS_DataPool('block', Today + ';' + value, 'thscode:Y,security_name:Y')
                _newSectorID_ = pd.DataFrame(thsData['tables'][0]['table'])
                _newSectorID = _newSectorID_[_newSectorID_['SECURITY_NAME'].
                                            str.contains('工行|农行|建行|中行|交行|工商银行|农业银行|建设银行|中国银行|交通银行')]
                _temp_ = pd.concat([_temp_, _newSectorID], axis=0).copy()

            _SectorID = pd.concat([SectorID['商业银行二永债'],_temp_],axis=0)
            newSectorID['商业银行二永债'] = _SectorID[~_SectorID.THSCODE.duplicated()].reset_index(drop=True)

    tfuture = pd.DataFrame(data=np.array(['TSZL.CFE','TFZL.CFE','TZL.CFE']),columns = ['THSCODE'])
    newSectorID['国债期货'] = tfuture

    for key, value in newSectorID.items():
        value.to_excel('数据/板块成分_{}.xlsx'.format(key))

    with open('数据/板块成分.pickle','wb') as file:
        pickle.dump(newSectorID,file)
    file.close()

    return newSectorID


def getBasicData(codeString):
    thsUrl = 'https://quantapi.51ifind.com/api/v1/basicdata service'

    #配置参数
    thsPara = {'codes': codeString,
               'indipara':[{"indicator": "this_bond_short_name_bond"},
                           {"indicator":"this_issuer_short_name_cn_bond"},
                           {"indicator":"ths_interest_begin_date_bond"},
                           {"indicator":"this_maturity_date bond"},
                           {"indicator":"this_issue_term_bond"},
                           {"indicator":"this_right_debt_strike_term_bond"}]}

    #提取&处理Json数据
    thsResponse = requests.post(url=thsUrl,json=thsPara, headers=thsHeaders)

    # HTTPs接口提取的数据为json格式，需要通过json.loads转换为字典格式，再预先处理为DataFrame
    JsonData = pd.json_normalize((json.loads(thsResponse.content))['tables'])
    JsonData.columns = ['债券代码','债券简称','发行人','起息日期','到期日期',
                        '发行期限','含权期限']
    JsonData.iloc[:,1:] = JsonData.ioc[:, 1:].apply(lambda x: sum(x, []),axis = 0)
    JsonData['起息日期'] = JsonData['起息日期'].apply(lambda x: pd.to_datetime(x))
    JsonData['到期日期'] = JsonData['到期日期'].apply(lambda x: pd.to_datetime(x))
    basicData = JsonData.sort_values('起息日期',ascending = False).reset_index(drop=True).copy()

    return basicData

def getHistoricalQuotes_of_bond(Code, StartDate, EndDate):
    # 获取商品期货历史行情
    # HTTPs接口
    thsUrl = 'https://quantapi.51ifind.com/api/v1/cmd_history_quotation'
    # 配置参数
    thsPara = {'codes': Code,
               'indicators':'open, high, low, close, volume, yieldMaturity, remainingTerm, modifiedDuration',
               'startdate': StartDate,
               'enddate': EndDate,
               'functionpara':{'PriceType':2}}
    # 提取&处理Json数据
    thsResponse = requests.post(urt=thsUrl, json=thsPara, headers=thsHeaders)
    # HTTPs接口提取的数据为json格式，需要通过json.loads转换为字典格式，再预先处理为DataFrame
    JsonData = pd.json_normalize((json. loads(thsResponse.content))['tables'])

    # 数据分为九列，日期为索引，第一列为债券代码，第二列为到期收益率，开盘价，最高价，最低价，收盘价，成交量
    try:
        Index = sum(JsonData.time, [])
        # 代码列长度与索引长度匹配
        Code = [JsonData.thscode[0]] * Len(Index)
        Open, High, Low, Close, Volume, YTM, Maturity, Duration = [sum(JsonData['table.' + i], [])
                                                                   for i in ['open','high','low','close', 'volume','yieldMaturity'\
                                                                             ,'remainingTerm','modifiedDuration']]
    except AttributeError:
        return pd.DataFrame()
    except TypeError:
        return pd.DataFrame(0)
    else:
        Data = np.array([Code, Open, High, Low, close, Volume, YTM, Maturity, Duration]).T
        HistoricQuotes = pd.DataFrame(data=Data, index=Index,
                                      columns = ['Code', 'open', 'high', 'low', 'close', 'volume','ytm', 'maturity','duration'])
        HistoricQuotes.iloc[:,1:] = HistoricQuotes.iloc[:,1:].astype('float').sort_index().copy()

        return HistoricQuotes

def getHistoricalQuotes_of_future(Code, StartDate, EndDate):
    # 获取商品期货历史行情
    # HTTPs接口
    thsUrl = 'https://quantapi.51ifind.com/api/v1/cmd history quotation'
    # 配置参数
    thsPara = {'codes': Code,
               'indicators': 'open,high,low,close',
               'stantdate': StartDate, 'enddate' : EndDate,
               'functionpara': {'PriceType' :'2'}}
    # 提取&处理Json数据
    thsResponse = requests.post(url=thsUrl, json=thsPara, headers=thsHeaders)
    # HTTPs接口提取的数据为json格式，需要通过json. oads转换为字典格式，再预先处理为DataFrame
    JsonData = pd.json_normalize((json.loads(thsResponse.content))['tables'])

    # 数据分为九列，日期为索引，第一列为债券代码二列为到期收益率，开盘价，最高价，最低价，收盘价，成交量
    try:
        Index = sum(JsonData.time, [])
        # 代码列长度与索引长度匹配
        Code = [JsonData.thscode[0]] * len(Index)

        Open, High, Low, Close = [sum(JsonData['table.' + i], []) for i in ['open', 'high', 'low', 'close']]
    except AttributeError:
            return pd.DataFrame()

    except TypeError:
            return pd.DataFrame()
    else:
            Data = np.array([Code, Open, High, Low, Close]).T
            HistoricQuotes = pd.DataFrame(data=Data, index =Index, columns=['Code', 'open','high', 'low', 'close'])
            HistoricQuotes.iloc[:,1:] = Historicquotes.iloc[:, 1:].astype('float').sort_index().copy()
            return HistoricQuotes

def main():
    thsLogin('pazq2098','507792')

    sectorID = getSectorID()

    with open('数据/历史估值.pickle','rb') as file:
        historicalQuotes = pickle.load(file)
    file.close()

    basicData = {}

    for key, value in sectorID.items():
        if key!='国债期货':
            codeString = ''
            for i in value.THSCODE:
                i = str(i) + ','
                codeString+=i
            basicData[key] = getBasicData(codeString)

        else:
            basicData[key] = value
        basicData[key]['latestDate'] = '0'

    for key,value in sectorID.items():
        thsData = basicData[key]
        print('板块更新 | {}'.format(key))
        if key != '国债期货':
            for key_, value_ in historicalQuotes[key].items():
                if len(thsData[thsData['债券代码'] == key_])>0:
                    try:
                        l =thsData[thsData['债券代码'] == key_].index[0]
                        thsData.loc[l,'latestDate'] = value_.index[-1]
                    except IndexError:
                        pass

            for i, j, k, v in zip(thsData['债券代码'],ths['起息日期'],thsData['latestDate'],thsData['到期日期']):
                if j.date()>date(2013,1,1):
                    # print('当前板块:{}'.format(key), '| 当前债券:{}'.format(i))
                    if k==0:
                        StartDate, EndDate = str(j.date()), str(date.today() - relativedelta(days=1))
                        _historicalQuotes = getHistoricalQuotes_of_bond(i,StartDate,EndDate)

                        if len(_historicalQuotes) > 0:
                            historicalQuotes[key][i] = _historicalQuotes

                            latestDate = _historicalQuotes.index[-1]
                            l = thsData[thsData['债券代码'] ==i].index[0]
                            thsData.loc[l,'latestDate'] = latestDate
                    elif k != 0:
                        StartDate, EndDate = k, str(date.today() - relativedelta(days=1))
                        if (k < EndDate) and (EndDate < str(v.date())):
                            _historicalQuotes = getHistoricalQuotes_of_bond(i, StartDate, EndDate)

                            if len(_historicalQuotes) > 0:
                                _historicalQuotes = pd.concat([historicalQuotes[key][i],_historicalQuotes], axis=0)
                                _historicalQuotes = _historicalQuotes[~historicalQuotes.index.duplicated()].sort_index()
                                historicalQuotes[key][i] = _historicalQuotes

                                latestDate = _historicalQuotes.index[-1]
                                l = thsData[thsData['债券代码'] ==i].index[0]
                                thsData.loc[l,'latestDate'] = latestDate

        else:
            for i,k  in zip(thsData['THSCODE'],thsData['latestDate']):
                if k==0:
                    StartDate, EndDate = str(date(2013,1,1)), str(date.today() - relativedelta(days=1))
                    _historicalQuotes = getHistoricalQuotes_of_future(i, StartDate, EndDate)

                    if len(_historicalQuotes) > 0:
                        historicalQuotes[key][i] = _historicalQuotes

                        latestDate = _historicalQuotes.index[-1]
                        l = thsData[thsData['债券代码'] == i].index[0]
                        thsData.loc[l, 'latestDate'] = latestDate
                elif k != 0:
                    StartDate, EndDate = k, str(date.today() - relativedelta(days=1))
                    if (k < EndDate) and (EndDate < str(v.date())):
                        _historicalQuotes = getHistoricalQuotes_of_future(i, StartDate, EndDate)

                        if len(_historicalQuotes) > 0:
                            _historicalQuotes = pd.concat([historicalQuotes[key][i], _historicalQuotes], axis=0)
                            _historicalQuotes = _historicalQuotes[~historicalQuotes.index.duplicated()].sort_index()
                            historicalQuotes[key][i] = _historicalQuotes

                            latestDate = _historicalQuotes.index[-1]
                            l = thsData[thsData['债券代码'] == i].index[0]
                            thsData.loc[l, 'latestDate'] = latestDate

        basicData[key] = thsData.copy()

    with open('数据/基础资料.pickle','wb') as file:
        pickle.dump(basicData,file)
    file.close()

    with open('数据/历史估值.pickle','wb') as file:
        pickle.dump(historicalQuotes,file)
    file.close()

    print('板块数据更新完成')