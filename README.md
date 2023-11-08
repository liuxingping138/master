import logging
import re

import ahocorasick
import dask.dataframe as dd
import pandas as pd

logging.captureWarnings(True)
import time

from dask.distributed import Client


def make_AC(AC, word_set):
    for word in word_set:
        AC.add_word(word ,word)
    return AC

# def read_csv(lujing):
#     f1 =open(lujing ,encoding='utf-8')
#     data =pd.read_csv(f1)
#     f1.close()
#     return data


def fill_nan(df_js):
    # 按列填充空值
    col_list = df_js.columns.tolist()
    for col in col_list:
        df_js[col].fillna(value='', inplace=True)
    return df_js

    '''
    ahocosick：自动机的意思
    可实现自动批量匹配字符串的作用，即可一次返回该条字符串中命中的所有关键词
    '''






path_key = 'D:\\广东临时需求\\辽宁\\副本辽宁全行业客户数据匹配清单(1).xlsx'
paths = 'D:\\广东临时需求\\'
def read_key():
    df = pd.read_excel('D:\\广东临时需求\\辽宁\\辽宁存量产品-四融打标1027.xlsx')
    df.to_csv(paths+'产品关键词.txt',sep='\t',index=False,header=None)
    return df

def get_key(paths):
    read_key()
    df2 = pd.read_table(paths+'产品关键词.txt', names=[ '关键词','产品'])
    kw_list = df2['关键词'].unique().tolist()
    AC_KEY = ahocorasick.Automaton()
    AC_KEY = make_AC(AC_KEY, set(kw_list))
    AC_KEY.make_automaton()
    return (AC_KEY,df2)



def cliens():
    client = Client(n_workers=6, processes=True,
                    threads_per_worker=1,
                    memory_limit='20GB')  # '192.168.0.106:8786'
    return client

def fun2(r):
    pattern = r'\(.*?\)'
    return re.sub(pattern,'',r)

def merger_key():
    pass
    df1 = pd.read_excel(path_key)
    df1.to_csv(paths+ '辽宁存量匹配结果.csv')
    df1 = dd.read_csv(paths + "辽宁存量匹配结果.csv")
    df1 = df1.repartition(npartitions=10)
    df1 = df1.fillna('')

    meta_arg = pd.Series(str, name='meta_arg')
    AC_KEY, dfs = get_key(paths)
    df1['关键词'] = df1.apply(lambda r: func1(r,AC_KEY), axis=1, meta=meta_arg)

    time1 = time.time()
    print(time.time() - time1)
    time1 = time.time()
    df_pp = df1.compute()
    print(time.time() - time1)
    con2 = df_pp['关键词'] != ''
    df_pp = df_pp[con2]
    print('wwwwwwwwww', len(df_pp))
    df_pp = df_pp.reset_index()
    df_result = df_pp.drop('关键词', axis=1).join(
        df_pp['关键词'].str.split('/', expand=True).stack().reset_index(level=1, drop=True).rename('关键词'))
    df_result.drop_duplicates(inplace=True)
    print('将匹配关键词/分开行数', len(df_result))
    df_pp.drop_duplicates(inplace=True)
    df_result = df_result.merge(dfs, left_on='关键词', right_on='关键词')
    df11 = pd.read_excel( "副本辽宁全行业客户数据匹配清单(1).xlsx", dtype=str)
    df_results = df11.merge(df_result[['企业id','关键词', '省级机构总部']],left_on = '企业id',right_on = '企业id',how='left')

    df_result = df_result.pivot_table(index=['集团编码'], values=['关键词', '产品'],
                               aggfunc={'关键词': func4, '产品': func4})
    df_pp.to_excel(paths + "辽宁存量数据匹配产品类型.xlsx",index=False)

def func1(r,AC_KEY):
    name_list = []
    for item in AC_KEY.iter(str(r['订购集团产品明细（匹配后）'])):
        name_list.append(item[1])
    return '|'.join(name_list)

def func4(r):
    s = r.values.tolist()
    s = [i for i in s if i != '']
    return '/'.join(s)


if __name__ == '__main__':

    # client.cluster
    # get_key(path_key,paths)
    # read_gaode()

    cliens()
    merger_key()
