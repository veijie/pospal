import hashlib
import requests
from requests.structures import CaseInsensitiveDict
import json
from sqlalchemy import create_engine
import pandas as pd
from pandas import json_normalize
from sqlalchemy.sql.coercions import expect
from sqlalchemy.sql.expression import false, label, table, true
#import datetime
from datetime import datetime, timedelta
from pospalconst import *


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
    
def postBackParaStr2(para):
    "根据postBackParameter的参数获取其对应json字符串"
    if para == {} or 'parameterType' not in para.keys() or 'parameterValue' not in para.keys():
        return '';
    str = '"postBackParameter": {"parameterType": "' + para['parameterType'] + '","parameterValue": "' + para['parameterValue'] + '"}';
    return str;

# 获取查询会员资料首页的web请求body,signature
def qryCustomer_body(type,value, brno = BranchNO.HQ):
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

def qryCustomer_bash(extstr, brno = BranchNO.HQ):
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
def customer_clear(connstr = "postgresql://odoo:odoo@localhost/postgres"):
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
def customer_sync(connstr = "postgresql://odoo:odoo@localhost/postgres"):
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
            req = qryCustomer_body('','')
        else:
            req = qryCustomer_body(r[0][0],r[0][1],BranchNO.HQ)
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
            if data2['status'] == 'error':
                break;
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
                req = qryCustomer_body(data2['data']['postBackParameter']['parameterType'],data2['data']['postBackParameter']['parameterValue'],BranchNO.HQ)
                data = req['postbody']
                signature = req['signature']
                headers["time-stamp"] = str(int(datetime.now().timestamp()*1000)) # "1628755947796" #
                headers["data-signature"] = signature
                resp = requests.post(url, headers=headers, data=data.encode())

# 从接口提取数据
def query_data(funurl, rqextparm = '', brno = BranchNO.HQ, printurl = 0):
    "brno 门店代码；funurl 接口url；rqextparm 扩展的请求体参数"
    headers = CaseInsensitiveDict()
    headers["User-Agent"] = "openApi"
    headers["Content-Type"] = "application/json"
    headers["accept-encoding"] = "gzip,deflate"
    headers["time-stamp"] = str(int(datetime.now().timestamp()*1000)) # "1628755947796" #

    if brno+1 > len(BRANCHS):
        return {};
    postbody = '{"appId":"'+BRANCHS[brno]['appid']+ '"';
    #extstr = rqextparm;
    if rqextparm != '':
        postbody = postbody + ',' + rqextparm;
    postbody = postbody + '}';
    signature = md5value2(BRANCHS[brno]['appkey'],postbody);
    url = BRANCHS[brno]['url']+funurl;
    headers["data-signature"]=signature;
    print(signature)
    print(postbody)
    data = requests.post(url, headers=headers, data=postbody.encode())
    if data.status_code != 200:
        print('调用url失败，请检查有关参数',data.status_code);
        print('url:', url);
        print('header:',headers);
        print('request body:', postbody);
        #print(data.text);
    else:
        if printurl == 1:
            print('url:', url);
            print('header:',headers);
            print('request body:', postbody);
    return data;

# 根据自定义的接口函数编号调用接口
def query_data2(funno, rqextparm = '', brno = BranchNO.HQ, printurl = 0):
    "brno 门店代码；funno 接口函数编号；rqextparm 扩展的请求体参数"
    return query_data(POSPALFUNS[funno]['funurl'], rqextparm, brno, printurl);

# 拉取数据并保存到数据库
def save_data(funno, dbstr='', tblname = '', rqextparm = '', brno = BranchNO.HQ, excludecol = ''):
    "从接口提取对应funno接口的数据，保存到connstr:tablename"
    "excludecol: 不需要保存的列"

    if POSPALFUNS[funno]['type'] != 'query':
        print('仅支持查询类的接口(type=query)');
        return;

    connstr = dbstr;
    if connstr == '':
        connstr = "postgresql://odoo:odoo@localhost/postgres";
    eng = create_engine(connstr,execution_options={"isolation_level": "REPEATABLE READ"});
    tablename = tblname;
    if tablename == '':
        tablename = POSPALFUNS[funno]['tablename'];

    # 会员查询要剔除weixinOpenIds列，否则保存出错
    excol = excludecol;
    if (funno == PPFunNO.QRY_CUSTOMER) & (excol == ''):
        excol = 'weixinOpenIds';
    try:
        tstr = "select paratype, paravalue, recordcount from retrieve_data_index a inner join (select max(retr_id) max_id from retrieve_data_index where tablename='"+ tablename + "') b on retr_id = max_id";
        r = eng.execute(tstr).fetchall();
    except:
        print('数据连接异常:',connstr);
        raise;
    else:
        if len(r) == 0 :
            resp = query_data2(funno,rqextparm, brno);
        else:
            tstr = postBackParaStr(r[0][0],r[0][1]);
            resp = query_data2(funno,rqextparm=tstr, brno=brno);
        
        pi = 0 
        while resp.status_code == 200:
            if pi > 10000:  # 预防程序出现异常死循环
                break;
            data2 = json.loads(resp.text)
            # print(data2['data']['postBackParameter']['parameterType'])
            # print(data2['data']['postBackParameter']['parameterValue'])
            # 页面记录数 data2['data']['pageSize'] 
            # 数据处理
            if data2['status'] == 'error':
                print(r.text);
                break;

            df = json_normalize(data2['data']['result']);
            if df.shape[0] == 0:
                print('未发现新增的'+ POSPALFUNS[funno]['dname'] + '数据');
                break;
            # 排除列
            if (excol != '') & (excol in df.columns):
                df1 = df.drop(labels=excol,axis=1);
            else:
                df1 = df;
            
            # 分类有嵌套数据的列            
            nestcols = [];
            for col in df1.columns:
                if isinstance(df[col][0],list):
                    nestcols.append(col);
                    df1=df1.drop(labels=col,axis=1);
            
            pi = pi + 1;
            print(pi, datetime.now(), df.shape[0], data2['data']['postBackParameter']['parameterValue']);
            try:
                # 保存数据提取索引
                msql = "insert into retrieve_data_index (tablename, paratype, paravalue, recordcount) values ('"+ tablename + "','" + \
                    data2['data']['postBackParameter']['parameterType'] + "','" + \
                    data2['data']['postBackParameter']['parameterValue'] + "','" + \
                    str(df.shape[0]) + \
                    "')"
                eng.execute(msql);
                # 保存数据
                tstr = tablename + '_'
                df1.to_sql(tstr, con=eng, if_exists='append');
                
            except:
                print("数据处理出现异常");
                raise;
            else:
                # 返回记录数小于页面应有记录数时，数据提取完成
                if data2['data']['pageSize'] > df.shape[0]:
                    print(datetime.now(),POSPALFUNS[funno]['dname'],"数据同步完成");
                    break;
                parmstr = postBackParaStr(data2['data']['postBackParameter']['parameterType'],data2['data']['postBackParameter']['parameterValue']);
                resp = query_data2(funno,rqextparm=parmstr, brno=brno);



# 提取pd数据的树形层级关系，返回树形json数据
# {'level': 0, 'name': '', 'key': '', 'subkeys': []}
# name = 所在层级的列名（顶级名称=root）
# 笨笨的写法:< 有空再优化:--) 
def getpdstruct(pddata,keycol,row=0):
    if len(pddata) == 0:
        return {};
    retjs = dict();
    retjs['level']=0;
    retjs['key']=keycol;
    retjs['subkeys']=list();
    i = 0;
    for col in pddata.columns:
        if isinstance(pddata[col][row], list):
            tmpdf = pd.DataFrame(pddata.loc[:0][col][0]);
            tjs=dict();
            tjs['level']=1;
            tjs['name']=col;
            tjs['key']='';
            tjs['subkeys']=list();
            # 尝试第2级
            for col2 in tmpdf.columns:
                if isinstance(tmpdf[col2][0], list):
                    tmpdf2 = pd.DataFrame(tmpdf.loc[:0][col2][0]);
                    print(col2,tmpdf2)
                    tjs2 = dict();
                    tjs2['level']=2;
                    tjs2['name']=col2;
                    tjs2['key']='';
                    tjs2['subkeys']=list();
                    #尝试第3级
                    for col3 in tmpdf2.columns:
                        if isinstance(tmpdf2[col3][0], list):
                            tmpdf3 = pd.DataFrame(tmpdf2.loc[:0][col3][0]);
                            print(col3,tmpdf3)
                            tjs3 = dict();
                            tjs3['level']=3;
                            tjs3['name']=col3;
                            tjs3['key']='';
                            tjs3['subkeys']=list();
                            #尝试第4级
                            for col4 in tmpdf3.columns:
                                if isinstance(tmpdf3[col3][0], list):
                                    tmpdf4 = pd.DataFrame(tmpdf3.loc[:0][col4][0]);
                                    tjs4 = dict();
                                    tjs4['level']=4;
                                    tjs4['name']=col4;
                                    tjs4['key']='';
                                    tjs4['subkeys']=list();
                                    tjs3['subkeys'].append(tjs4);
                            tjs['subkeys'].append(tjs3);
                    tjs['subkeys'].append(tjs2);
            retjs['subkeys'].append(tjs);
    #pddata = retdf;
    return retjs;


# 保存pd数据到数据库
# struct=如果存在嵌套，需要指定对应的嵌套结构，否则数据保存会失败
def pd2db(df, eng, tablename, struct={}, excludecol=''):
    retjs = {};
    if df.shape[0] == 0:
        return retjs;
    # 排除列
    excol = excludecol;
    if (excol != '') & (excol in df.columns):
        df0 = df.drop(labels=excol,axis=1);
    else:
        df0 = df;
    # 分类有嵌套数据的列            
    #第2级
    # sub1 = pd.DataFrame(df.iloc[1]['items'][0]['ticketitemattributes'])
    # sub2 = pd.DataFrame(df.iloc[1]['items'][1]['ticketitemattributes'])
    # 填充第1级数据
    # sub1.insert(0,'productUid',value=df.iloc[1]['items'][0]['productUid'])
    # sub2.insert(0,'productUid',value=df.iloc[1]['items'][1]['productUid'])
    # 填充第0级数据
    # sub1.insert(0,'uid',value=df.iloc[1]['uid'])
    # sub2.insert(0,'uid',value=df.iloc[1]['uid'])
    key0 = 'uid'
    key11='productUid'
    nestcols = '';
    if 'key' in struct:
        #若有第1级嵌套
        key0 = struct['key'];
        for struct1 in struct['subkeys']:
            nestcol1 = struct1['name'];
            tablename1 = tablename + '_' + nestcol1;
            if nestcols == '':
                nestcols = nestcol1;
            else:
                if not nestcol1 in nestcols:
                    nestcols += ',' + nestcol1;
            for i in range(0,len(df0)):
                df1=pd.DataFrame(df0[nestcol1][i]);
                #df0.drop(labels=nestcol1,axis=1);
                if len(df1) == 0:
                    continue;
                #print('=>',tablename1,i,df0[key0][i]);
                #若有第2级嵌套
                nestcols1 = '';
                key11 = struct1['key'];
                tabindex2 = 0;
                for struct2 in struct1['subkeys']:
                    nestcol2 = struct2['name'];
                    tablename2 = tablename1 + '_' + nestcol2;
                    if nestcols1 == '':
                        nestcols1 = nestcol2;
                    else:
                        if not nestcol2 in nestcols1:
                            nestcols1 += ',' + nestcol2;
                    for j in range(0,len(df1)):
                        df2=pd.DataFrame(df1[nestcol2][j])
                        #df1.drop(labels=nestcol2,axis=1);
                        if len(df2) == 0:
                            continue;
                        #print('=>  ',tablename2, i,j,df1[key11][j]);
                        #若有第3级嵌套
                        nestcols2 = '';
                        key111 = struct2['key'];
                        tabindex2 = 0;
                        for struct3 in struct2['subkeys']:
                            nestcol3 = struct3['name'];
                            if nestcol3 not in df2.columns:
                                continue;
                            exitfor = 0;
                            tablename3 = tablename2 + '_' + nestcol3;
                            if nestcols2 == '':
                                nestcols2 = nestcol3;
                            else:
                                if not nestcol3 in nestcols2:
                                    nestcols2 += ',' + nestcol3;
                            for k in range(0,len(df2)):
                                #print(i,j,k,tablename3);
                                if type(df2[nestcol3][k]) == dict:  # 不用嵌套本级，直接采用list数据
                                    df3=pd.DataFrame(df2[nestcol3][k],index=[0]);  ##这里出错 waiting
                                    #exitfor = 1;
                                else:
                                    df3=pd.DataFrame(df2[nestcol3][k]);
                                #df1.drop(labels=nestcol2,axis=1);
                                if len(df3) == 0:
                                    continue;
                                #print('=>  ',tablename3, i,j,df2[key111][k]);
                                df3.insert(0,key111+'__',value=df2[key111][k]);  # 为避免列名重复，列名加_
                                df3.insert(0,key11+'_',value=df1[key11][j]);  # 为避免列名重复，列名加_
                                df3.insert(0,key0,value=df0[key0][i]);
                                #print(df3)
                                #dfdbs[tabindex1][tabindex2] = dfdbs[tabindex1][tabindex2].append(df2);
                                # 保存到数据库
                                if not tablename3 in retjs:                                    
                                    retjs[tablename3] = pd2db0(df3,eng,tablename3);
                                else:
                                    retjs[tablename3] += pd2db0(df3,eng,tablename3);
                                if exitfor == 1:
                                    break;
                        df2.insert(0,key11+'_',value=df1[key11][j]);  # 为避免列名重复，列名加_
                        df2.insert(0,key0,value=df0[key0][i]);
                        #print(df2)
                        #dfdbs[tabindex1][tabindex2] = dfdbs[tabindex1][tabindex2].append(df2);
                        # 保存到数据库
                        if not tablename2 in retjs:                                    
                            retjs[tablename2] = pd2db0(df2,eng,tablename2,nestcols2);
                        else:
                            retjs[tablename2] += pd2db0(df2,eng,tablename2,nestcols2);
                #保存第1级的表，此时才能获取需要忽略的字段nestcols1
                df1.insert(0,key0,value=df0[key0][i])
                #print(df1);
                if not tablename1 in retjs:                                    
                    retjs[tablename1] = pd2db0(df1,eng,tablename1,nestcols1);
                else:
                    retjs[tablename1] += pd2db0(df1,eng,tablename1,nestcols1);
    # 保存到数据库
    if not tablename in retjs:                                    
        retjs[tablename] = pd2db0(df0,eng,tablename,nestcols);
    else:
        retjs[tablename] += pd2db0(df0,eng,tablename,nestcols);
    return retjs;

def pd2db0(df, eng, tablename, excludecol='', ifexists='append'):
    #print('保存表：',tablename, '排除字段', excludecol);
    if excludecol != '':
        for str in excludecol.split(','):
            df=df.drop(labels=str, axis = 1);
    try:
        df.to_sql(tablename, con=eng, if_exists=ifexists);
    except:
        print(tablename,'保存失败');
        print(df);
        raise;
        return -1;
    else:
        return len(df);

#同步销售数据
def sync_sales(connstr, storeno, saledate):
    #connstr 数据库引擎连接串,storeno 门店号（参考postpalconst）, saledate 销售日期
    # 同步流程
    # 1. 提取待同步门店的上次日期以及最大记录id
    # 2. 若未同步过，开始日期取开业日期；已同步则从上次的同步参数开始提取
    # 3. 每次提取后立即保存到数据库，并写入提交状态
    # 4. 保存原始的json数据
    if connstr == '':
        connstr = "postgresql://odoo:odoo@localhost/postgres";
    eng = create_engine(connstr,execution_options={"isolation_level": "REPEATABLE READ"});
    intrno = 24 # 销售单据接口的接口代码
    savetblname = POSPALFUNS[intrno]['tablename']
    date1 = '2021-06-01'
    date2 = saledate
    try:
        r = eng.execute(f"select paratype, paravalue, rpara1, recordcount from retrieve_data_index a inner join (select max(retr_id) max_id from retrieve_data_index where tablename='{savetblname}') b on retr_id = max_id").fetchall();
    except:
        print('数据连接异常:',connstr);
        raise;
    paratype=''
    paravalue = ''
    retjs = []
    if len(r) == 0 :
        if BRANCHS[storeno]['opendate'] != '':
            date1 = BRANCHS[storeno]['opendate']
    else:
        paratype = r[0][0]
        paravalue = r[0][1]
        date1 = r[0][2]
    # 日期从date1到date2查询数据
    intrcount = 0;
    p1 = f'"startTime":"{date1} 00:00:00","endTime":"{date1} 23:59:59"';
    print(date1);
    while date1 <= date2:
        rqpara = p1;
        if paravalue != '':
            rqpara += ',' + postBackParaStr(paratype, paravalue);
        resp = query_data2(intrno, rqpara, storeno);
        intrcount += 1;
        # waiting here 
        data2 = json.loads(resp.text);
        if data2['status'] == 'error':
            print('接口调用失败：' + ','.join(data2['messages']));
            break;
        df = json_normalize(data2['data']['result']);
        paravalue = data2['data']['postBackParameter']['parameterValue'];
        paratype = data2['data']['postBackParameter']['parameterType'];
        if len(df) == 0 or paravalue == '':
            date1 = incdate(date1);
        else:
	        #pd2db(df, eng, savetblname, DATA_STRUCT.QRY_SALETICKETS);
            js = pd2db(df, eng, savetblname, DATA_STRUCT.QRY_SALETICKETS);
            retjs.append(js);
            pvalue = f"'{savetblname}','{date1}', '{data2['data']['postBackParameter']['parameterType']}', '{data2['data']['postBackParameter']['parameterValue']}', {str(df.shape[0])}"
            msql = f"insert into retrieve_data_index (tablename, rpara1, paratype, paravalue, recordcount) values ({pvalue})";
            eng.execute(msql);
            if len(df) < 100:
                date1 = incdate(date1);
                paravalue = '';
                print(date1);
                p1 = f'"startTime":"{date1} 00:00:00","endTime":"{date1} 23:59:59"';
    return retjs;

def incdate(datestr):
    return (datetime.strptime(datestr, '%Y-%m-%d') + timedelta(days=1)).__format__('%Y-%m-%d');

