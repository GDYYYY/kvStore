#pragma once
#ifndef _KVSTORE_H_
#define _KVSTORE_H_
#include "kvstore_api.h"
#include <ctime>
using namespace std;
//SkipList部分
const int max_size = 2*1024*1024;//(2 * 1024 * 1024) ; 
struct node
{
    node *up;
    node *down;
    node *left;
    node *right;
    uint64_t key = 0;
    string val;
    node(uint64_t key, string val) : key(key), val(val), up(nullptr), down(nullptr), left(nullptr), right(nullptr) {}
};//跳转表的节点
// struct isDel{
//     uint64_t key;
//     isDel* next=nullptr;
// };
struct node_head
{
    node_head *up;
    node_head *down;
    node *right;
    node_head() : up(nullptr), down(nullptr), right(nullptr) {}
};//跳转表头
class SkipList
{
private:
public:
    node_head *bottom_head;
    node_head *top_head;
    node *cur_node=nullptr;
    int tot_level = 0;
    uint64_t size = 0;
    int max = max_size;//(2 * 1024 * 1024) ;

    SkipList( );
    ~SkipList();
    bool ifup()
    {
        srand(22);//(time(NULL));
        return (rand() % 2) & 1;
    };
    void put(uint64_t key, const std::string &s);

    std::string get(uint64_t key);
    node *search(uint64_t key);

    bool del(node *target);
    void leveldown();
    void reset();//清空（待debug）
};

struct buf_node
{
    uint64_t key = 0;
    //string val = "";
    uint64_t offset=0;
    buf_node* next=nullptr;
};

struct buffer
{
    uint64_t min = 0;
    uint64_t max = 0;
    int level=0;
    int num=0;
    buffer *next = nullptr;
    buf_node *head=nullptr;//head有值
};

struct block_node
{
    uint64_t key=0;
    string val="";
    //uint64_t offset=0;//需要吗？
    block_node* next=nullptr;
    block_node* last=nullptr;
};

struct block
{
    uint64_t min=0;
    uint64_t max=0;
    uint64_t size=0;
    int level=0;
    int num=0;
    block_node *head=nullptr;//head有值
};
struct nm
{
    int num=0;
    int min=0;
};

class KVStore : public KVStoreAPI
{
    // You can add your implementation here
private:
    SkipList memtable;
    buffer* offset_head=new buffer;//offset_head无值
    //block* cur_block=nullptr; //用于归并
    //block* cur_block1=nullptr;
    //block* new_block=nullptr; //用于从文件中加载
    nm *num;
    

public:

    uint64_t cur_offset=0;
    uint64_t cur_num=0;
    uint64_t cur_len=0;
    uint64_t cur_level=0;
    bool isEnd=false;//记录查找到的位置
    KVStore(const std::string &dir);

    ~KVStore();

    //应该没问题
    void load_offset();//加载索引

    //debug
    bool search_from_offset(uint64_t key);//从offset中寻找是否存在

    //debug
    string get_val(uint64_t level,uint64_t num,uint64_t offset,uint64_t len);//获取值

    //应该没问题
    void mem_to_ss(SkipList memtable);
     //memtable转换为sstable,第一个数据是size（用于跳转至索引区）

     //debug 暂时问题出在load_block，merge_ss
    void check_merge(int level);//检查该层是否需要下移

    //debug
    void merge_ss(block* x);//将上层的block与下面一层合并 记得update_buffer

    //应该没问题(鬼)
    block* load_block(int level,int num,block* x);//将文件转换为block x

    //应该没问题
    void block_to_ss(block* x);//将block写为文件
 
    //可能没问题
    void merge_block(block* x1,block* x2,block* x3,block* x4);//x1比x2新
    //合并两个block,得到的新block一个存在x3，一个返回(实际是存在cur_block中）继续合并
    
    //应该没问题
    void update_buffer(block* x);//(debug)
    //更新buffer区域的buf_node，可以考虑将归并时的offset更新和max，min放在这里
    
    void del_buffer(int level,int num);

    void reset_block(block* x);

    void checkNum(uint64_t level);

    void put(uint64_t key, const std::string &s) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;//未写
};

#endif
