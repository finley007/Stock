#! /usr/bin/env python
# -*- coding:utf8 -*-

import pandas as pd
import numpy as np

from scipy.stats import pearsonr

def test_ewm():
    df = pd.DataFrame({'A': [0, 1, 2, np.nan, 4]})
    print(df)
    print(df.ewm(span=2).mean())
    
def test_ewm_with_params():
    df = pd.DataFrame({'A': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                      'B': [0, 0, 0, 1, 0, 0, 0, 2, 0, 0]}, columns=['A', 'B'])
    print(df)
    print(df.ewm(span=1).mean())
    print(df.ewm(span=2).mean())
    print(df.ewm(span=3).mean())
    print(df.ewm(com=0).mean())
    print(df.ewm(com=1).mean())
    print(df.ewm(com=2).mean())
    print(df.ewm(alpha=1, adjust=False).mean())
    print(df.ewm(alpha=1, adjust=True).mean())
    print(df.ewm(alpha=0.3, adjust=False).mean())
    print(df.ewm(alpha=0.3, adjust=True).mean())
    print(df.ewm(alpha=0.6, adjust=False).mean())
    print(df.ewm(alpha=0.6, adjust=True).mean())
    print(df.ewm(alpha=0.9, adjust=False).mean())
    print(df.ewm(alpha=0.9, adjust=True).mean())
    
    
    print(df['B'].ewm(com=1).mean())
    
def test_stdev():
    df = pd.DataFrame({'A': [0, 1, 5, 3, 4]})
    df['sum'] = df['A'].rolling(2).sum()
    df['diff'] = df['A'] - df['A'].shift(1)
    df['diff.stdev'] = df['diff'].rolling(2).std()
    print(df)
    
def test_multi_dimensional_array():
    list=[[] for _ in range(2)]
    print(list)
    for array in list:
        array.append("a")
    print(list)
    
def test_traversal_array():
    list=[[] for _ in range(2)]
    print(list)
    for i in range(0, len(list)):
        if (i == 0):
            list[i].append("a")
        else:
            list[i].append("b")
    print(list)
    
def test_list_2_string():
    print('_'.join(['a','b']))
    print('_'.join(['a']))
    print('_'.join([]))
    
    
def test_circular_backtracking():
    fruits = ['banana', 'apple',  'mango']
    has_backtrack = False
    for index in range(len(fruits)):
        print ('当前水果 : %s' % fruits[index])
        if (not has_backtrack and fruits[index] == 'apple'):
            index = index - 1
            has_backtrack = True
            
def test_parse_str():
    str = "1|2|3"
    if (str.find("|") != -1):
        params = list(map(lambda str: int(str), str.split("|")))
        print(params)
        
def test_set():
    my_set = set()
    my_set.add("a")
    my_set.add("a")
    my_set.add("b")
    print(my_set)
    print("a" in my_set)
    
    
def test_df_template_operation():
    
    
    
if __name__ == '__main__':
    # test_ewm()
    # test_ewm_with_params()
    # test_stdev()
    # test_multi_dimensional_array()
    # test_traversal_array()
    # test_list_2_string()
    # test_circular_backtracking()
    # test_parse_str()
    test_set()