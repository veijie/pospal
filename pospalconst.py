import hashlib
import requests
from requests.structures import CaseInsensitiveDict
import json
from sqlalchemy import create_engine
import pandas as pd
from pandas import json_normalize
from sqlalchemy.sql.expression import false, true
from datetime import datetime

BRANCHS = [
    {'account':'foodstyle','branchno':'0000','name':'呼雨呼晴总部','url':'https://area18-win.pospal.cn:443/','appid':'5D57003C0B65F1D5075E9D7564AAAFDB','appkey':'64325938716487425'},
    {'account':'fdspck01','branchno':'Z001','name':'总部运营中心仓库','url':'https://area18-win.pospal.cn:443/','appid':'793E3A2B07B695E7F37F1CFE17BFF9C1','appkey':'290851828648988066'},
    {'account':'fdsp001','branchno':'A001','name':'呼雨呼晴-宝龙店','url':'https://area18-win.pospal.cn:443/','appid':'A9F8E0CCD37EF339A24858728C7B000A','appkey':'879167059692208598'},
    {'account':'fdsp002','branchno':'A002','name':'呼雨呼晴-万象九宜城','url':'https://area18-win.pospal.cn:443/','appid':'1529DEEAEF570392F71C0AB7A78C79DF','appkey':'1069407360735653579'},
    {'account':'fdsp003','branchno':'A003','name':'呼雨呼晴-门店3','url':'https://area18-win.pospal.cn:443/','appid':'208A291EFDD6FB578CD4BF16EE4C3B62','appkey':'770242098259924699'},
    {'account':'fdsp004','branchno':'A004','name':'呼雨呼晴-门店4','url':'https://area18-win.pospal.cn:443/','appid':'09ED1271FC593FAE7029C9C90C64FB0F','appkey':'1005100847108015891'},
    {'account':'fdsp005','branchno':'A005','name':'呼雨呼晴-门店5','url':'https://area18-win.pospal.cn:443/','appid':'E99D3633E92879835C2F4CA6D74227F2','appkey':'882148495997978034'},
];

class BranchNO():
    HQ  = 0
    CK01 = 1 
    A001 = 2
    A002 = 3
    A003 = 4
    A004 = 5
    A005 = 6


def md5value2(appkey,str):
    input_name = hashlib.md5()
    input_name.update((appkey+str).encode("utf-8"))
    return input_name.hexdigest().upper()

# 根据postBackParameter的参数获取其对应json字符串
def postBackParaStr(type,value):
    "根据postBackParameter的参数获取其对应json字符串"
    if value == '':
        return '';
    str = '"postBackParameter": {"parameterType": "' + type + '","parameterValue": "' + value + '"}';
    return str;

# 获取查询会员资料首页的web请求body,signature
def qryCustomer_body(brno,type,value):
    "获取查询会员资料首页的web请求body,signature 参数：brno=门店编号，extstr=扩展参数"
    if brno+1 > len(BRANCHS):
        return {'url':'','postbody':'','signature':''};
    postbody = '{"appId":"'+BRANCHS[brno]['appid']+ '"';
    extstr = postBackParaStr(type,value);
    if extstr != '':
        postbody = postbody + ',' + extstr;
    postbody = postbody + '}';
    signature = md5value2(BRANCHS[brno]['appkey'],postbody);
    url = BRANCHS[brno]['url']+'pospal-api2/openapi/v1/customerOpenApi/queryCustomerPages';
    rlist = {'url':url,'postbody':postbody,'signature':signature};
    return rlist;

def qryCustomer_bash(brno,extstr):
    "获取查询会员资料的web请求bash终端命令,signature 参数：brno=门店编号，extstr=扩展参数"
    if brno+1 > len(BRANCHS):
        return {'url':'','postbody':'','signature':''};
    postbody = '{"appId":"'+BRANCHS[brno]['appid']+ '"';
    if extstr != '':
        postbody = postbody + ',' + extstr;
    postbody = postbody + '}';
    signature = md5value2(BRANCHS[brno]['appkey'],postbody);
    url = BRANCHS[brno]['url']+'pospal-api2/openapi/v1/customerOpenApi/queryCustomerPages';
    bashcmd = 'curl -X POST '+ url + \
        ' -H "User-Agent: openApi" -H "Content-Type: application/json" -H "accept-encoding: gzip,deflate" -H "time-stamp: ' + \
        str(int(datetime.now().timestamp()*1000)) + \
        '" -H "data-signature: '+ signature + '" -d "' + postbody.replace('"','\\"') + '"';
    return bashcmd;


# 清理顾客数据
def customer_clear(connstr):
    "清理顾客数据"
    str = '';
    if connstr == '':
        connstr = "postgresql://odoo:odoo@localhost/postgres";
    eng = create_engine(connstr,execution_options={"isolation_level": "REPEATABLE READ"});
    try:
        r = eng.execute("delete from retrieve_data_index where tablename='customers;'");
        r = eng.execute('delete from customers_');
    except:
        str = '数据连接异常:',connstr;
        #print(str);
    else:
        str = '';
        #print("顾客数据清理完成！");
    finally:
        return str;

# 增量同步顾客会员数据
def customer_sync(connstr):
    "同步会员数据到目标数据库（connstr连接串）。isall=1 全量更新，否则增量"

    headers = CaseInsensitiveDict()
    headers["User-Agent"] = "openApi"
    headers["Content-Type"] = "application/json"
    headers["accept-encoding"] = "gzip,deflate"
    headers["time-stamp"] = str(int(datetime.now().timestamp()*1000)) # "1628755947796" #

    #engine = create_engine('mysql://root:mysql4et@172.0.0.10/epos?charset=utf8')
    if connstr == '':
        connstr = "postgresql://odoo:odoo@localhost/postgres";
    eng = create_engine(connstr,execution_options={"isolation_level": "REPEATABLE READ"});

    try:
        r = eng.execute("select paratype, paravalue, recordcount from retrieve_data_index a inner join (select max(retr_id) max_id from retrieve_data_index where tablename='customers') b on retr_id = max_id").fetchall();
    except:
        print('数据连接异常:',connstr);
        raise;
    else:
        if len(r) == 0 :
            req = qryCustomer_body(BranchNO.HQ,'','')
        else:
            req = qryCustomer_body(BranchNO.HQ,r[0][0],r[0][1])
        data = req['postbody']
        signature = req['signature']

        print(data,signature)
        headers["data-signature"] = signature
        url = req['url']
        resp = requests.post(url, headers=headers, data=data.encode())

        pi = 0 
        while resp.status_code == 200:
            if pi > 10000:  # 预防程序出现异常死循环
                break;
            data2 = json.loads(resp.text)
            # print(data2['data']['postBackParameter']['parameterType'])
            # print(data2['data']['postBackParameter']['parameterValue'])
            # 页面记录数 data2['data']['pageSize'] 
            # 数据处理
            df = json_normalize(data2['data']['result']);
            if df.shape[0] == 0:
                print('未发现新增的会员数据')
                break;
            # weixinOpenIds 列是多行列，先删除才能保存（待处理）
            if 'weixinOpenIds' in df.columns:
                df1 = df.drop(labels='weixinOpenIds',axis=1);
            else:
                df1 = df;
            pi = pi + 1;
            print(pi, datetime.now(), df.shape[0], data2['data']['postBackParameter']['parameterValue']);
            try:
                # 保存数据提取索引
                msql = "insert into retrieve_data_index (tablename, paratype, paravalue, recordcount) values ('customers','" + \
                    data2['data']['postBackParameter']['parameterType'] + "','" + \
                    data2['data']['postBackParameter']['parameterValue'] + "','" + \
                    str(df.shape[0]) + \
                    "')"
                eng.execute(msql);
                # 保存数据
                df1.to_sql('customers_', con=eng, if_exists='append');
            except:
                print("数据处理出现异常");
                raise;
            else:
                # 返回记录数小于页面应有记录数时，数据提取完成
                if data2['data']['pageSize'] > df.shape[0]:
                    print(datetime.now(),"数据同步完成");
                    break;
                req = qryCustomer_body(BranchNO.HQ,data2['data']['postBackParameter']['parameterType'],data2['data']['postBackParameter']['parameterValue'])
                data = req['postbody']
                signature = req['signature']
                headers["time-stamp"] = str(int(datetime.now().timestamp()*1000)) # "1628755947796" #
                headers["data-signature"] = signature
                resp = requests.post(url, headers=headers, data=data.encode())