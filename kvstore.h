#pragma once
#ifndef _KVSTORE_H_
#define _KVSTORE_H_
#include "kvstore_api.h"
#include <ctime>
using namespace std;


struct node
{
    node *up ;
    node *down ;
    node *left ;
    node *right ;
    uint64_t key = 0;
    string val ;
    node(uint64_t key,string val) :key(key),val(val), up(nullptr), down(nullptr), left(nullptr), right(nullptr) {}
};
struct node_head
{
    node_head *up ;
    node_head *down ;
    node *right;
    node_head():up(nullptr),down(nullptr),right(nullptr){}
};
class SkipList
{
private:
public:

    node_head* bottom_head;
    node_head* top_head;
    node* cur_node;
    int tot_level = 0;
    int size=0;
    int max=(2*1024*1024)/(sizeof(uint64_t)+sizeof(string));

    SkipList(/* args */);
    ~SkipList();
    bool ifup(){
        srand(time(NULL));
        return (rand()%2)&1;
    };
    void put(uint64_t key, const std::string &s);

    std::string get(uint64_t key);
    node* search(uint64_t key);

    bool del(node* target);
    void leveldown();
    void reset();
};

struct  buf_node
{
    uint64_t key=0;
    string val="";
    bool isDel=false;
};

struct buffer{
    uint64_t begin=0;
    uint64_t end=0;
    buf_node* buf_head=nullptr;

};


class KVStore : public KVStoreAPI
{
	// You can add your implementation here
private:
SkipList memtable;
public:
    
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;
};

#endif
