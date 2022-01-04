#! /usr/bin/env python
# -*- coding:utf8 -*-

import mplfinance as mpf
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


"""打印分析图
 
    :param data: 待分析数据源，必须包含’Open’, ‘High’, ‘Low’ 和 ‘Close’ 数据（注意:首字母是大写的），
    而且行索引必须是pandas.DatetimeIndex，行索引的名称必须是’Date‘(同理注意首字母大写)
    :type data: DataFrame 
    :param type: 指定风格：‘bar’, ‘ohlc’, ‘candle’
    :param volume: 是否展示成交量
    :param show_nontrading: 是否展示非交易天数
    :param figratio: 图像横纵比
    :param add_plot: 绘制额外信息
    :return: NoneType
 
    Usage::
 
      >>> from visual_tools import draw_analysis_curve
      >>> draw_analysis_curve(data)
    """
def draw_analysis_curve(data, type='candle',
                        volume=True,
                        show_nontrading=False,
                        figratio=(20, 10),
                        figscale=1,
                        show_signal=False,
                        signal_keys=[]):
    mc = mpf.make_marketcolors(up='r', down='g')
    s = mpf.make_mpf_style(marketcolors=mc)
    if (show_signal):
        add_plot = mpf.make_addplot(data[signal_keys])
        mpf.plot(data=data, type=type, volume=volume, figratio=figratio, figscale=figscale,
                 show_nontrading=show_nontrading, style=s, addplot=add_plot)
    else:
        mpf.plot(data=data, type=type, volume=volume, figratio=figratio, figscale=figscale,
                 show_nontrading=show_nontrading, style=s)

"""画直方图
 
    :param data: 待分析数据源
    :type data: Series 
    :param bin_num: 直方图立柱的数量
    :param facecolor: 直方图立柱的的颜色
    :param alpha: 是否展示非交易天数
    :return: NoneType
 
    Usage::
 
      >>> from visual_tools import draw_histogram
      >>> draw_histogram(data)
    """
def draw_histogram(data, bin_num, facecolor='blue', alpha=0.5):
  # mean = np.mean(data) 
  # std = np.std(data)
  plt.hist(data, bin_num, facecolor=facecolor, alpha=alpha)
  plt.show()
 
"""画散点图
 
    :param xlabel: 横坐标标识
    :param ylabel: 纵坐标标识
    :param xscope: 横轴阈值范围
    :type xscope: dict
    :             min 最小值
    :             max 最大值
    :param yscope: 纵轴阈值范围
    :type yscope: dict
    :             min 最小值
    :             max 最大值
    :param data: 数据源
    :type data: list
    :           dict
    :           x: 横坐标值 
    :           y: 纵坐标值
    :           color: 颜色 
    :           label: 图例 
    :return: NoneType
 
    Usage::
 
      >>> from visual_tools import draw_scatter
      >>> draw_scatter('横坐标','纵坐标',data = data)
    """  
def draw_scatter(xlabel='X', ylabel='Y', xscope={}, yscope={}, data=[]):  
  plt.rcParams['font.sans-serif']=['SimHei']
  plt.rcParams['axes.unicode_minus'] = False
  #matplotlib画图中中文显示会有问题，需要这两行设置默认字体
  plt.xlabel(xlabel)
  plt.ylabel(ylabel)
  if (xscope.get('min') and xscope.get('max')):
    plt.xlim(xmax=xscope['max'],xmin=xscope['min'])
  if (yscope.get('min') and yscope.get('max')):
    plt.ylim(ymax=yscope['max'],ymin=yscope['min'])
  # colors1 = '#00CED1' #点的颜色
  area = np.pi * 2**2  # 点面积 
  if (data):
    for data_set in data:
    # 画散点图
      plt.scatter(data_set['x'], data_set['y'], s=area, c=data_set['color'], alpha=0.4, label=data_set['label'])
  plt.legend()
  plt.show()
  
  
  """画折线图
 
    :param data: 待分析数据源
    :type data: Dataframe 
    :param title: 标题
    :param xlabel: 横坐标标识
    :param ylabel: 纵坐标标识
    :param plot_info: 数据源解析字典
    :param show_grid: 是否展示网格
    :return: NoneType
 
    Usage::
 
      >>> from visual_tools import draw_line
      >>> draw_line(data)
    """
def draw_line(data, title='', xlabel='', ylabel='', plot_info={'x':'x','y':[{'key':'y','label':''}]}, show_grid=False):
  plt.style.use('ggplot')
  plt.figure(figsize=(10,5))
  plt.title(title)
  plt.xlabel(xlabel)
  plt.ylabel(ylabel)
  for y in plot_info.get('y'):
    plt.plot(data[plot_info.get('x')],data[y.get('key')],label=y.get('label'))
  plt.legend()
  plt.grid(show_grid)
  plt.show()
  