#! /usr/bin/env python
# -*- coding:utf8 -*-

class Pagination():
    
    def __init__(self, list, page_size = 10):
        self._list = list
        self._cur_index = 0
        self._page_size = page_size
        
    def get_max_index(self):
        if len(self._list) % self._page_size == 0:
            return int((len(self._list) / self._page_size))
        else:
            return int((len(self._list) / self._page_size)) + 1
        
    def has_next(self):
        if self._cur_index < self.get_max_index():
            return True
        else:
            return False
    
    def has_prev(self):
        if self._cur_index > 1:
            return True
        else:
            return False
        
    def first(self):
        self._cur_index = 1
        if len(self._list) >= self._page_size:
            return self._list[0: self._page_size]
        else:
            return self._list
        
    def last(self):
        self._cur_index = self.get_max_index()
        return self._list[(self._cur_index - 1) * self._page_size: len(self._list)]
    
    def next(self):
        if self._cur_index == self.get_max_index(): #已到最后一页
            return self._list[(self._cur_index - 1) * self._page_size: len(self._list)]
        else:
            self._cur_index = self._cur_index + 1
            if len(self._list) > self._cur_index * self._page_size:
                return self._list[(self._cur_index - 1) * self._page_size : self._cur_index * self._page_size]
            else:
                return self._list[(self._cur_index - 1) * self._page_size : len(self._list)]
        
    def prev(self):
        if self._cur_index == 1:
            if len(self._list) < self._cur_index * self._page_size:
                return self._list[0 : len(self._list)]
            else:
                return self._list[0 : self._cur_index * self._page_size]
        else:
            self._cur_index = self._cur_index - 1
            return self._list[(self._cur_index - 1) * self._page_size : self._cur_index * self._page_size]
        

if __name__ == '__main__':
    list = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18]
    pagination = Pagination(list, page_size=5)
    while pagination.has_next():
        print(pagination.next())
    print(pagination.first())
    print(pagination.last())
    while pagination.has_prev():
        print(pagination.prev())
    
                
        
    