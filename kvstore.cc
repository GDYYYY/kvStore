#include "kvstore.h"
#include <fstream>
#include <filesystem>
#include <string>
#include <iostream>
using namespace std;
namespace fs = std::filesystem;

int pow(int a, int b)
{
	if (b == 0)
		return 1;
	while (b--)
	{
		a = a * a;
	}
	return a;
}

KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir)
{
	//从文件中读取所有索引并调用函数建成链表
	load_offset();
}

KVStore::~KVStore()
{
}

void KVStore::block_to_ss(block *x)
{
	int level = x->level;
	int num = x->num;
	string path = "./data/level" + to_string(level);
	if (!fs::exists(path))
		fs::create_directories(path);
	path = "./data/level" + to_string(level) + "/" + to_string(num) + ".dat";
	fstream out(path, ios::binary | ios::out);
	out.write((char *)&x->size, sizeof(uint64_t));

	//写入key-val
	block_node *cur_node = x->head;
	while (cur_node)
	{
		out.write((char *)&cur_node->key, sizeof(uint64_t));
		out.write(cur_node->val.c_str(), cur_node->val.length());
		if (cur_node->next)
			cur_node = cur_node->next;
		else
			break;
	}

	//写入key-offset
	buffer *cur_buf = offset_head->next;
	while (cur_buf->level != level || cur_buf->num != num)
	{
		cur_buf = cur_buf->next;
	}
	buf_node *cur = cur_buf->head;

	while (cur->next)
	{
		out.write((char *)&cur->key, sizeof(uint64_t));
		out.write((char *)&cur->offset, sizeof(uint64_t));
		cur = cur->next;
	}
	out.write((char *)&cur->key, sizeof(uint64_t));
	out.write((char *)&cur->offset, sizeof(uint64_t));
	out.close();
}

block *KVStore::load_block(int level, int num)
{
	fstream in;
	uint64_t k1, k2;
	uint64_t off1, off2;
	char *s; //value
	uint64_t size;
	block *newone = new block;
	new_block = newone;
	newone->level = level;
	newone->num = num;
	block_node *cur = new block_node;
	newone->head = cur;

	string path = "./data/level" + to_string(level) + "/" + to_string(num) + ".dat";
	in.open(path, ios::binary | ios::in);
	if (!in)
		return nullptr;

	in.read((char *)&size, sizeof(uint64_t));
	newone->size = size;
	in.seekg(size, ios::beg);
	in.read((char *)&k1, sizeof(uint64_t));
	in.read((char *)&off1, sizeof(uint64_t));
	size += 2 * sizeof(uint64_t);
	in.seekg(size, ios::beg); //跳转指针

	while (!in.eof()) //判断是否到达末尾
	{				  //in.seekg(size,ios::beg);//跳转指针
		in.read((char *)&k2, sizeof(uint64_t));
		in.read((char *)&off2, sizeof(uint64_t));
		cur->key = k1;
		int len = off2 - off1 - sizeof(uint64_t);
		s = new char[len + 1];
		s[len] = '\0';
		in.seekg(off1 + sizeof(uint64_t), ios::beg);
		in.read(s, len);
		cur->val = s;
		cur->next = new block_node;
		cur->next->last = cur;
		cur = cur->next;
		k1 = k2;
		off1 = off2;
		size += 2 * sizeof(uint64_t);
		in.seekg(size, ios::beg); //跳转指针
	}
	cur->key = k1;
	int len = size - off1 - sizeof(uint64_t);
	s = new char[len + 1];
	s[len] = '\0';
	in.seekg(off1 + sizeof(uint64_t), ios::beg);
	in.read(s, len);
	cur->val = s;
	in.close();
	return new_block;
}

void KVStore::load_offset()
{
	int level = 0;
	int num = 0;
	fstream in;
	uint64_t k;
	uint64_t off;
	uint64_t size;
	buffer *newone = offset_head;
	while (true)
	{
		string path = "./data/level" + to_string(level) + "/" + to_string(num) + ".dat";

		in.open(path, ios::binary | ios::in);
		if (!in)
			return;
		in.read((char *)&size, sizeof(uint64_t));
		while (newone->next)
			newone = newone->next; //到尾端
		newone->next = new buffer;
		newone = newone->next;
		newone->level = level;
		newone->num = num;
		buf_node *bnew = new buf_node;
		newone->head = bnew;
		while (!in.eof()) //判断是否到达末尾
		{
			in.seekg(size, ios::beg); //跳转指针
			in.read((char *)&k, sizeof(uint64_t));
			in.read((char *)&off, sizeof(uint64_t));
			bnew->key = k;
			bnew->offset = off;
			bnew->next = new buf_node;
			bnew = bnew->next;
			size += 2 * sizeof(uint64_t);
		}
		delete bnew;
		newone->min = newone->head->key;
		newone->max = k;
		in.close();
	}
	if (num < pow(2, level + 1))
		num++;
	else
	{
		level++;
		num = 0;
	}

} //debug

//memtable转换为sstable
//文件中第一个数据是size（用于跳转至索引区）//然后是level和num（第几级第几个）
void KVStore::mem_to_ss(SkipList memtable)
{
	//加载进内存中offset
	buffer *cur_buf = new buffer;
	buffer *bef = offset_head;
	while (bef->next && bef->level == 0)
	{
		bef = bef->next;
	}
	cur_buf->next = bef->next;
	bef->next = cur_buf;
	cur_buf->num = ((bef == offset_head) ? 0 : bef->num + 1);

	node *cur_node = memtable.bottom_head->right;
	cur_buf->min = cur_node->key; //记录最小值
	uint64_t off = 0;
	buf_node *cur_bnode = new buf_node;
	cur_buf->head = cur_bnode;
	while (cur_node)
	{
		cur_bnode->key = cur_node->key;
		cur_bnode->offset = off;
		cur_bnode->next = new buf_node;
		cur_bnode = cur_bnode->next;
		off += (sizeof(cur_node->key) + cur_node->val.length());
		if (cur_node->right)
			cur_node = cur_node->right;
		else
		{
			cur_buf->max = cur_node->key;
			break;
		}
	}
	//写入sstable(debug)
	string path = "./data/level" + to_string(0);
	if (!fs::exists(path))
		fs::create_directories(path);
    path = "./data/level" + to_string(0) + "/" + to_string(cur_buf->num) + ".dat";
	fstream out(path, ios::binary | ios::out);
	buf_node *cur = cur_buf->head;
	cur_node = memtable.bottom_head->right;
	out.write((char *)&off, sizeof(uint64_t));
	while (cur_node)
	{
		out.write((char *)&cur_node->key, sizeof(uint64_t));
		out.write(cur_node->val.c_str(), cur_node->val.length());
		if (cur_node->right)
			cur_node = cur_node->right;
		else
			break;
	}

	while (cur->next)
	{
		out.write((char *)&cur->key, sizeof(uint64_t));
		out.write((char *)&cur->offset, sizeof(uint64_t));
	}
	out.close();
}

void KVStore::check_merge(int level)
{
	int max_num = 0;
	string path1 = "./data/level" + to_string(level) + "/" + to_string(max_num) + ".dat";
	while (fs::exists(path1))
	{
		max_num++;
		path1 = "./data/level" + to_string(level) + "/" + to_string(max_num) + ".dat";
	}
	max_num--;

	if (max_num < pow(2, level + 1))
		return; //判断是否溢出
	if (level == 0)
	{
		block *x1 = load_block(0, 1);
		block *x2 = load_block(0, 0);
		block *x3 = merge_block(x1, x2);
		merge_ss(x3);
		merge_ss(x2);
		//删除level0中的文件
		int num = 0;
		while (num <= max_num)
		{
			string path = "./data/level" + to_string(level) + "/" + to_string(num) + ".dat";
			//if(!fs::remove(path)) break;
			fs::remove(path);
			num++;
		}
	}
	else
	{
		while (max_num >= pow(2, level + 1))
		{
			block *x1 = load_block(0, max_num); //取最后那个
			merge_ss(x1);

			//删除对应文件
			string path = "./data/level" + to_string(level) + "/" + to_string(max_num) + ".dat";
			fs::remove(path);
			max_num--;
		}
	}
}
//待调试
block *KVStore::merge_block(block *x1, block *x2)
{
	uint64_t size = 0;
	block_node *cur_1 = x1->head;
	block_node *cur_2 = x2->head;

	if (cur_block)
		delete cur_block;
	block *new_block = new block; //reset cur_block
	cur_block = new_block;

	//将cur_1插在合适的cur_2后
	while (size < max_size) //size不包括cur_2
	{
		if (!cur_1)
			break;
		while (cur_1->key > cur_2->key && size < max_size)
		{
			size += (sizeof(cur_2->key) + cur_2->val.length());
			if (!cur_2->next)
				break;
			cur_2 = cur_2->next;
			//cur_2->offset = size;
		}
		if (cur_2->key == cur_1->key)
		{
			size = size - cur_2->val.length() + cur_1->val.length();
			cur_2->val = cur_1->val;
		}
		else
			while (cur_1 && cur_1->key < cur_2->key)
			{
				block_node *newone = new block_node;
				newone->next = cur_2;
				newone->key = cur_1->key;
				newone->val = cur_1->val;
				//newone->offset = size;
				if (cur_2 == x2->head)
					x2->head = newone;
				else
				{
					newone->last = cur_2->last;
					newone->last->next = newone;
				}
				cur_2->last = newone;
				size += (sizeof(newone->key) + newone->val.length());
				if (cur_1->next)
					cur_1 = cur_1->next;
				else
					cur_1 = nullptr;
				// cur_2->offset=size;
			}
		if (!cur_1)
			break;
		if (cur_2->key == cur_1->key)
		{
			size = size - cur_2->val.length() + cur_1->val.length();
			cur_2->val = cur_1->val;
		}

		//cur_1>=cur_2
		if (cur_2->next)
		{
			//cur_2->offset = size;
			cur_2 = cur_2->next;
		}
		else
		{
			//cur_2->offset = size;
			cur_2->next = cur_1->next;
			size += (cur_2->val.length() + sizeof(cur_2->key));
			cur_2 = cur_2->next;
			//cur_2->offset = size;
			while (size < max_size)
			{
				size += (cur_2->val.length() + sizeof(cur_2->key));
				cur_2 = cur_2->next;
				//cur_2->offset = size;
			}
		}
		//if(size>=max_size) break;
	}
	//x2->min = x2->head->key;
	//x2->max = cur_2->last->key;
	cur_2->last->next = nullptr;
	//完成前半部分归并，存在x2

	size = 0;
	block_node *cur = new block_node;
	cur_block->level = x2->level;
	cur_block->head = cur;
	if (cur_1 || cur_2)
	{
		if (cur_1 && cur_2)
		{
			if (cur_1->key < cur_2->key)
			{
				cur->key = cur_1->key;
				//cur->offset = size;
				cur->val = cur_1->val;
				if (cur_1->next)
					cur_1 = cur_1->next;
				else
					cur_1 = nullptr;
			}
			else if (cur_1->key == cur_2->key)
			{
				cur->key = cur_1->key;
				//cur->offset = size;
				cur->val = cur_1->val;
				if (cur_1->next)
					cur_1 = cur_1->next;
				else
					cur_1 = nullptr;
				if (cur_2->next)
					cur_2 = cur_2->next;
				else
					cur_2 = nullptr;
			}
			else
			{
				cur->key = cur_2->key;
				//cur->offset = size;
				cur->val = cur_2->val;
				if (cur_2->next)
					cur_2 = cur_2->next;
				else
					cur_2 = nullptr;
			}
		}

		while (cur_1 && cur_2)
		{
			block_node *newone = new block_node;
			cur->next = newone;
			newone->last = cur;
			//size += (sizeof(cur->key) + cur->val.length());
			cur = cur->next;
			if (cur_1->key < cur_2->key)
			{
				cur->key = cur_1->key;
				//cur->offset = size;
				cur->val = cur_1->val;
				if (cur_1->next)
					cur_1 = cur_1->next;
				else
					cur_1 = nullptr;
			}
			else if (cur_1->key == cur_2->key)
			{
				cur->key = cur_1->key;
				//cur->offset = size;
				cur->val = cur_1->val;
				if (cur_1->next)
					cur_1 = cur_1->next;
				else
					cur_1 = nullptr;
				if (cur_2->next)
					cur_2 = cur_2->next;
				else
					cur_2 = nullptr;
			}
			else
			{
				cur->key = cur_2->key;
				//cur->offset = size;
				cur->val = cur_2->val;
				if (cur_2->next)
					cur_2 = cur_2->next;
				else
					cur_2 = nullptr;
			}
		}
		if (!(cur_1 && cur_2) & (cur_1 || cur_2)) //只剩一边
		{
			if (!cur_1)
			{
				cur_2->last = cur;
				cur = cur_2;
			}
			if (!cur_2)
			{
				cur_1->last = cur;
				cur = cur_1;
			}
			//if(size==0) cur_block->min=cur->key;
			while (cur)
			{
				//cur->offset = size;
				//size += (sizeof(cur->key) + cur->val.length());
				if (cur->next)
					cur = cur->next;
				else
				{
					//cur->offset = size;
					break;
				}
			}
		}
	}
	//cur_block->min=cur_block->head->key;
	//cur_block->max=cur->key;//可能空指针,所以在更新buffer的时候再处理
	// update_buffer(x2);
	// update_buffer(cur_block);
	return cur_block;
}

void KVStore::update_buffer(block *x)
{
	buffer *cur = offset_head;
	buffer *newbuf = new buffer;
	block_node *cur_node = x->head;
	x->min = cur_node->key;
	newbuf->level = x->level;
	newbuf->num = x->num;
	while (cur->next)
	{
		if ((cur->next->level == x->level && cur->next->num == x->num) || (cur->next->level == (x->level + 1) && cur->level == x->level))
		{
			if (cur->next->next)
				newbuf->next = cur->next->next;
			delete cur->next;
			cur->next = newbuf;
			break;
		} //找到对应buffer或该层最后一个
		cur = cur->next;
	}
	if (!cur->next)
		cur->next = newbuf; //或者找到最后一个buffer

	buf_node *cur_bnode = new buf_node;
	uint64_t off = 0;
	newbuf->head = cur_bnode;
	while (cur_node->next)
	{
		cur_bnode->offset = off;
		cur_bnode->key = cur_node->key;
		//cur_node->offset=off;
		cur_node = cur_node->next;
		off += (sizeof(uint64_t) + cur_node->val.length());
		cur_bnode->next = new buf_node;
		cur_bnode = cur_bnode->next;
	}
	cur_bnode->offset = off;
	cur_bnode->key = cur_node->key;
	//cur_node->offset=off;
	x->max = cur_node->key;
	x->size = off + (sizeof(uint64_t) + cur_node->val.length());
} //待debug

void KVStore::merge_ss(block *x)
{ //调用该函数后记得删除x
	int min = x->min;
	int max = x->max;
	int level = x->level + 1;
	int i = 0;
	num = new nm[pow(2, level + 1) + 3]; //用于排序
	buffer *cur = offset_head;
	while (cur->next && cur->level != level)
	{
		cur = cur->next;
	}
	if (cur->level != level)
	{
		cur->next = new buffer;
		cur = cur->next;
		cur->level = level;
		cur->num = 0;
		//  cur->min=x->min;cur->max=x->max;
		update_buffer(x);
		block_to_ss(x);
		return;
	} //新建层

	while (cur->level == level)
	{
		if (cur->max > min)
		{
			num[i].num = cur->num;
			num[i].min = cur->min;
			i++;
		}
		else if (cur->min < max)
		{
			num[i].num = cur->num;
			num[i].min = cur->min;
			i++;
		}
		cur = cur->next;
	} //取出所有有关的buffer
	for (int m = 0; m < i - 1; m++)
	{
		for (int n = 0; n < i - m - 1; n++)
		{
			if (num[n].min > num[n + 1].min)
			{
				int tmpnum = num[n + 1].num;
				int tmpmin = num[n + 1].min;
				num[n + 1].num = num[n].num;
				num[n + 1].min = num[n].min;
				num[n].min = tmpmin;
				num[n].num = tmpnum;
			}
		}
	}
	//将buffer冒泡排序，否则归并会冲突
	for (int j = 0; j < i; j++)
	{
		block *x2 = load_block(level, num[j].num);
		x = merge_block(x, x2);
		update_buffer(x2);
		string path = "./data/level" + to_string(level) + "/" + to_string(num[j].num) + ".dat";
		fs::remove(path); //删除对应文件
		block_to_ss(x2);
	}
	update_buffer(x);
	block_to_ss(x);
	check_merge(level);
	delete num;
}

bool KVStore::search_from_offset(uint64_t key)
{
	if(!offset_head->next) return false;
	buffer *cur_buf = offset_head->next;
	buffer* target=new buffer;
	buf_node *spot = new buf_node;
	bool flag = false;
	while (!flag && cur_buf)
	{
		while (cur_buf->min > key || cur_buf->max < key)
		{
			if (cur_buf->next)
				cur_buf = cur_buf->next;
			else
			{
				return false;
			}
		}
		buf_node *cur = cur_buf->head;
		while (cur->key != key)
		{
			cur = cur->next;
			if (cur->key > key)
				break;
		}
		if (cur->key == key)
		{
			spot = cur;
			target=cur_buf;
			flag = true;
			break;
		}
		else
		{
			if (cur_buf->next)
				cur_buf = cur_buf->next;
			else
			{
				return false;
			}
		}
	}
	if (!flag)
		return false;
		if(cur_buf->level!=0) cur_level=cur_buf->level;
	while (cur_buf->level == 0 && cur_buf->next&&cur_buf->next->level==0)
	{
		cur_level = 0;
		cur_buf = cur_buf->next;
		while (cur_buf->min > key || cur_buf->max < key && cur_buf->level == 0)
		{
			if (cur_buf->next)
				cur_buf = cur_buf->next;
			else
			{
				cur_buf = nullptr;
				break;
			}
		}
		if (!cur_buf||cur_buf->level!=0)
			break;
		buf_node *cur = cur_buf->head;
		while (cur->key != key)
		{
			cur = cur->next;
			if (cur->key > key)
				break;
		}
		if (cur->key == key)
		{
			spot = cur;
			target=cur_buf;
		}	
	}//在零层时需要取num最大的

	cur_num=target->num;
	cur_offset=spot->offset+sizeof(uint64_t);
	if(spot->next) 
	{cur_len=spot->next->offset-spot->offset-sizeof(uint64_t);
	isEnd=false;}
	else
	{
		isEnd=true;
	}
	return flag;
}

string KVStore::get_val(uint64_t level,uint64_t num,uint64_t offset,uint64_t len){
	fstream in;
	uint64_t size;
	char* ans;
	string path= "./data/level" + to_string(level) + "/" + to_string(num) + ".dat";
	in.open(path, ios::binary | ios::in);
	if (!in)
		return nullptr;

	if(isEnd)  {in.read((char *)&size, sizeof(uint64_t));
	len=size-offset;}
	ans=new char[len+1];
	ans[len]='\0';
	in.read(ans,len);
	return ans;
}
/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	if (memtable.size < memtable.max)
		memtable.put(key, s);
	else
	{
		mem_to_ss(memtable);
		check_merge(0);
		memtable.reset();
		memtable.put(key, s);
	}
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	string val;
	val = memtable.get(key);
	if (val!="")
		return val;
	if (!search_from_offset(key))
		return "";
	return get_val(cur_level, cur_num, cur_offset, cur_len);
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
//待改
bool KVStore::del(uint64_t key)
{
	// node *target;
	// if (!memtable.search(key) || memtable.cur_node->key != key)
	// 	return false;
	// else
	// 	target = memtable.cur_node;
	// return memtable.del(target);
	node *target;
	if (search_from_offset(key)) //从offset中搜索(未写)
	{
		put(key, "");
		return true;
	}
	else
	{
		if (!memtable.search(key) || memtable.cur_node->key != key)
			return false;
		else
			target = memtable.cur_node;
		return memtable.del(target);
	}
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
}

node *SkipList::search(uint64_t key)
{
	if (tot_level == 0)
		return nullptr;
	node_head *cur_head = top_head;
	while (cur_head->right->key > key && cur_head->down)
	{
		cur_head = cur_head->down;
	}
	if (cur_head->right->key <= key)
		cur_node = cur_head->right;
	else
	{
		node *add = new node(key, "");
		add->right = cur_head->right;
		cur_head->right = add;
		add->right->left = add;
		cur_node = add;
		return cur_node;
	};
	while (cur_node->down)
	{
		while (cur_node->right && cur_node->right->key <= key)
		{
			cur_node = cur_node->right;
		}
		cur_node = cur_node->down;
	}
	while (cur_node->right && cur_node->right->key <= key)
	{
		cur_node = cur_node->right;
	}

	return cur_node;
}
void SkipList::put(uint64_t key, const std::string &s)
{
	//cout<<s;
	if (tot_level == 0)
	{ //empty
		node_head *l = new node_head();
		node *x1 = new node(key, s);
		l->right = x1;
		bottom_head = l;
		top_head = l;
		tot_level++;
		size += (sizeof(key) + s.length()); //更新大小
		return;
	}
	node *x;
	if (search(key))
		x = cur_node; //寻找该值或不大于该值的最大值
	else
		return;
	if (x->key == key)
	{ //该值存在，则更新val
		cur_node = x;
		size = size - x->val.length() + s.length(); //更新大小
		x->val = s;
		while (x->up)
		{
			x = x->up;
			x->val = s;
		}
		return;
	}
	else //不存在则插入
	{
		node *y = new node(key, s);
		if (x->right)
		{
			y->right = x->right;
			y->right->left = y;
		}
		x->right = y;
		y->left = x;
		cur_node = y;
		size = size + s.length() + sizeof(key); //在底层插入,更新大小
		//int top = 1;
		//node_head *cur_head=new node_head;
		//cur_head=bottom_head;
		node_head *cur_head = bottom_head;
		while (ifup()) //是否增长
		{
			if (cur_head->up)
				cur_head = cur_head->up;
			else
			{
				node_head *newtop = new node_head;
				tot_level++;
				cur_head->up = newtop;
				newtop->down = cur_head;
				cur_head = newtop;
				top_head = cur_head;
			} //超越已有层数则新增headnode

			node *z = new node(key, s);
			y->up = z;
			z->down = y;
			if (!cur_head->right)
			{
				cur_head->right = z;

			} //当前层没有东西
			else
			{
				node *h = cur_head->right;
				while (h->key < key)
				{
					h = h->right;
				}
				if (h->key == key)
					continue;
				if (h == cur_head->right)
				{
					cur_head->right = z;
					z->right = h;
					h->left = z;
				}
				else
				{
					h->left->right = z;
					z->left = h->left;
					h->left = z;
					z->right = h;
				}
			}
		}
	}
}

std::string SkipList::get(uint64_t key)
{
	if (!search(key))
		return "";
	//node *x=cur_node;
	return cur_node->key == key ? cur_node->val : "";
}

bool SkipList::del(node *target1)
{
	size = size - sizeof(target1->key) - target1->val.length(); //更新大小
	node_head *cur_head = bottom_head;
	node *target = target1;
	node *next = target->up ? target->up : nullptr;
	while (target)
	{
		if (target->right)
		{
			if (target == cur_head->right)
				cur_head->right = target->right;
			else
			{
				target->left->right = target->right;
				target->right->left = target->left;
			}
		}
		else
		{
			if (target == cur_head->right)
				cur_head->right = nullptr;
			else
			{
				target->left->right = nullptr;
			}

		} //左右连接
		if (target->up)
		{
			next = target->up;
			next->down = nullptr;
			cur_head = cur_head->up;
		}
		else
			next = nullptr;
		delete target;
		if (next)
			target = next;
		else
			break;
	}
	leveldown();
	return true;
}
void SkipList::leveldown()
{
	node_head *cur_head = top_head;
	node_head *next = cur_head->down ? cur_head->down : nullptr;
	while (!cur_head->right && cur_head != bottom_head)
	{
		top_head = cur_head->down;
		delete cur_head;
		tot_level--;

		if (next)
		{
			cur_head = next;
			next = cur_head->down ? cur_head->down : nullptr;
		}
		else
			break;
	}
	if (!bottom_head->right)
		tot_level--;
}

//待debug
void SkipList::reset()
{
	node_head *cur_head = top_head;
	node *cur_node = top_head->right;
	while (cur_head)
	{
		while (cur_node->right)
		{
			node *node = cur_node;
			cur_node = cur_node->right;
			//else cur_node=nullptr;
			delete node;
		}
		delete cur_node;
		//delete cur_head->right;
		if (!cur_head->down)
			break;
		cur_head = cur_head->down;
		cur_node = cur_head->right;
	}

	while (cur_head->down)
	{
		node_head *head = cur_head;
		cur_head = cur_head->down;
		delete head;
	}
}

SkipList::SkipList(/* args */)
{
	//	node_head* bottom_head=new node_head;
	//	node_head* top_head=new node_head;
}

SkipList::~SkipList()
{
	//	delete bottom_head;
	//	delete top_head;
}
