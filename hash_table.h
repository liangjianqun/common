/*
 * Copyright (c) 2015 KingSoft.com, Inc. All Rights Reserved
 *
 * @file common/hash_table.h
 * @author liangjianqun(liangjianqun@kingsoft.com)
 * @date 2015/11/09 18:18:15
 * @brief
 *
 */

#ifndef  _COMMON_HASH_TABLE_H_
#define  _COMMON_HASH_TABLE_H_

#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <map>
#include <vector>
#include <pthread.h>

namespace phenix {

class MutexLock {
public:
    MutexLock(pthread_mutex_t* lock) :lock_(lock){
        if (lock_ != NULL) {
            pthread_mutex_lock(lock_);
        }
    }
    virtual ~MutexLock() {
        if (lock_ != NULL) {
            pthread_mutex_unlock(lock_);
        }
    }

private:
    pthread_mutex_t* lock_;
};

template<typename T>
class ObjectPool {
public:
    ObjectPool() :gc_(NULL), gc_size_(0), pool_(NULL), pool_size_(0) {
        pthread_mutex_init(&mutex_, NULL);
    }
    virtual ~ObjectPool() {
        pthread_mutex_lock(&mutex_);
        std::map<void*, int>::iterator it = history_.begin();
        MutexLock lock(&mutex_);
        for (; it != history_.end(); ++it) {
            delete[] reinterpret_cast<T*>(it->first);
        }
        history_.clear();
        gc_ = NULL;
        gc_size_ = 0;
        pool_ = NULL;
        pool_size_ = 0;
        pthread_mutex_unlock(&mutex_);
        pthread_mutex_destroy(&mutex_);
    }

    T* Alloc() {
        MutexLock lock(&mutex_);
        if (gc_ != NULL) {
            T* p = gc_;
            gc_ = gc_->next;
            --gc_size_;
            std::map<void*, int>::iterator it = FindLessEqual(p);
            assert(it != history_.end());
            ++it->second;
            return p;
        }
        if (pool_size_ == 0) {
            pool_size_ = 1048576;
            pool_ = new T[pool_size_];
            history_[pool_] = 1;
        } else {
            std::map<void*, int>::iterator it = FindLessEqual(pool_);
            assert(it != history_.end());
            ++it->second;
        }
        --pool_size_;
        return pool_ + pool_size_;
    }

    void Free(T* p) {
        MutexLock lock(&mutex_);
        std::map<void*, int>::iterator it = FindLessEqual(p);
        assert(it != history_.end());
        --it->second;
        if (it->second <= 0 && history_.size() > 10) {
            delete[] reinterpret_cast<T*>(it->first);
            history_.erase(it);
            return;
        }
        p->next = gc_;
        gc_ = p;
        ++gc_size_;
    }

private:
    std::map<void*, int>::iterator FindLessEqual(T* p) {
        std::map<void*, int>::iterator it = history_.lower_bound(p);
        if (it != history_.end()) {
            if (it->first > p) {
                --it;
            }
        } else {
            if (history_.size() != 0) {
                --it;
            }
        }
        return it;
    }

    T* gc_;
    int64_t gc_size_;
    T* pool_;
    int pool_size_;
    std::map<void*, int> history_;
    pthread_mutex_t mutex_;
};

template<typename T>
struct HashNode {
    T value;
    HashNode* next;

    int64_t Key() {
        return value.key();
    }
};

template<typename T>
class HashTable {
public:
    explicit HashTable(uint32_t bucket_size, ObjectPool<HashNode<T> >* pool = NULL) :
        list_(NULL), size_(0), bucket_size_(bucket_size), pool_(pool) {
        if (bucket_size_ == 0) {
            bucket_size_ = 16381;
        }
        list_ = new HashNode<T>*[bucket_size_];
        memset(list_, 0, sizeof(list_[0]) * bucket_size_);
    }

    bool Insert(T new_value, T* old_value) {
        HashNode<T>** p = FindLessEqual(new_value.key());
        if (*p != NULL && (*p)->Key() == new_value.key()) {
            *old_value = (*p)->value;
            (*p)->value = new_value;
            return false;
        }

        HashNode<T>* new_node = Alloc();
        new_node->value = new_value;
        new_node->next = *p;
        *p = new_node;
        ++size_;
        return true;
    }

    bool Delete(int64_t key, T* value) {
        HashNode<T>** p = FindLessEqual(key);
        if (*p != NULL && (*p)->Key() == key) {
            *value = (*p)->value;
            HashNode<T>* to_delete = *p;
            *p = (*p)->next;
            Free(to_delete);
            --size_;
            return true;
        }
        return false;
    }

    bool Get(int64_t key, T* value) {
        HashNode<T>** p = FindLessEqual(key);
        if (*p != NULL && (*p)->Key() == key) {
            *value = (*p)->value;
            return true;
        }
        return false;
    }

    virtual ~HashTable() {
        for (uint32_t i = 0; i < bucket_size_; ++i) {
            HashNode<T>* p = list_[i];
            while (p != NULL) {
                HashNode<T>* next = p->next;
                Free(p);
                p = next;
            }
        }
        delete[] list_;
    }

    uint32_t Size() const {
        return size_;
    }

private:
    HashNode<T>** FindLessEqual(int64_t key) {
        int hash = (key < 0? -key: key) % bucket_size_;
        HashNode<T>** p = &list_[hash];
        while (*p != NULL) {
            if ((*p)->Key() <= key) {
                break;
            }
            p = &(*p)->next;
        }
        return p;
    }

    inline HashNode<T>* Alloc() {
        if (pool_ != NULL) {
            return pool_->Alloc();
        } else {
            return new HashNode<T>();
        }
    }

    inline void Free(HashNode<T>* p) {
        if (pool_ != NULL) {
            pool_->Free(p);
        } else {
            delete p;
        }
    }

    HashNode<T>** list_;
    uint32_t size_;
    uint32_t bucket_size_;
    ObjectPool<HashNode<T> >* pool_;
};

}

#endif  //_COMMON_HASH_TABLE_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
