/*
 * Copyright (c) 2015 KingSoft.com, Inc. All Rights Reserved
 *
 * @file test_hash.cc
 * @author liangjianqun(liangjianqun@kingsoft.com)
 * @date 2015/11/13 19:29:19
 * @brief
 *
 */





#include "hash_table.h"

using namespace phenix;

struct Index {
    int k;
    int off;
    int len;

    int64_t key() {
        return k;
    }
};

int main(int args, const char** argv){
    ObjectPool<HashNode<Index> > pool;

    HashTable<Index> hash(1023, &pool);
    Index old;
    for (int i = 0; i < 1000; ++i) {
        Index index;
        index.k = i;
        hash.Insert(index, &old);
    }

    return 0;
}















/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
