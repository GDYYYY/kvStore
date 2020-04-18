#include "kvstore.h"
#include <fstream>
#include <filesystem>
#include <string>
#include <iostream>
#include "time.h"
using namespace std;
namespace fs = std::filesystem;

int pow(int a, int b)
{
	if (b == 0)
		return 1;
	while (--b)
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
		{fs::create_directories(path); 
		}
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

block *KVStore::load_block(int level, int num,block* newone)
{
	fstream in;
	uint64_t k1, k2;
	uint64_t off1, off2;
	//char *s; //value
	uint64_t size;
	uint64_t u_size;
	//block *newone = x;
	//x = newone;
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
	u_size=size;
	size+=sizeof(uint64_t);//注意要算上size的大小
	in.seekg(size, ios::beg);
	in.read((char *)&k1, sizeof(uint64_t));
	in.read((char *)&off1, sizeof(uint64_t));
	size += 2 * sizeof(uint64_t);
	in.seekg(size, ios::beg); //跳转指针

    char* s;
	while (in.read((char *)&k2, sizeof(uint64_t))&&!in.eof()) //判断是否到达末尾
	{
		//in.read((char *)&k2, sizeof(uint64_t));
		in.read((char *)&off2, sizeof(uint64_t));
		cur->key = k1;
		int len = off2 - off1 - sizeof(uint64_t);
	
		s = new char[len + 1];
		s[len] = '\0';	
		in.seekg(off1 + 2*sizeof(uint64_t), ios::beg);		
		in.read(s, len);
		cur->val = s;
		delete s;
	
		cur->next = new block_node;
		cur->next->last = cur;
		cur = cur->next;
		k1 = k2;
		off1 = off2;
		size += 2 * sizeof(uint64_t);
		in.seekg(size, ios::beg); //跳转指针
	}

	cur->key = k1;
	int len = u_size - off1 - sizeof(uint64_t);
	s = new char[len + 1];
	s[len] = '\0';
	in.close();
	in.open(path, ios::binary | ios::in);
	//cout<<in.tellg()<<" ";
	in.tellg();
	in.seekg(off1 + 16, ios::beg);
	//cout<<off1<<' '<<in.tellg()<<' ';
	in.read(s, len);
	cur->val = s;
	//cout<<"load block: "<<cur->key<<' '<<len<<' '<<cur->val<<'\n';
	delete s;

	in.close();

	return newone;
}

void KVStore::load_offset()
{
	//cout<<"load offset\n";
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
        //cout<<path<<'\n';
		if(!fs::exists(path)&&level!=0) return;
        else if(!fs::exists(path)&&level==0) {level++;
           num=0; continue;}
		//cout<<"open\n";
		in.open(path, ios::binary | ios::in);
		size=0;
		in.read((char *)&size, sizeof(uint64_t));
		//cout<<"size:"<<size<<'\n';
		size+=sizeof(uint64_t);
		while (newone->next)
			newone = newone->next; //到尾端
		newone->next = new buffer;
		newone = newone->next;
		newone->level = level;
		newone->num = num;
		buf_node *bnew = new buf_node;
		newone->head = bnew;
		//size+=sizeof(uint64_t);
		//cout<<"first one "<<size<<'\n';
		in.seekg(size, ios::beg); //跳转指针
		while (in.read((char *)&k, sizeof(uint64_t))&&!in.eof()) //判断是否到达末尾
		{
			//in.read((char *)&k, sizeof(uint64_t));
			in.read((char *)&off, sizeof(uint64_t));
			bnew->key = k;
			newone->max = k;
			bnew->offset = off;
			bnew->next = new buf_node;
			bnew = bnew->next;
			size += 2 * sizeof(uint64_t);
		}
		delete bnew;
		newone->min = newone->head->key;
		//cout<<newone->level<<' '<<newone->num<<' '<<newone->min<<' '<<newone->max<<'\n';
		in.close();
	if (num < pow(2, level + 1)-1)
		num++;
	else
	{
		level++;
		num = 0;
	}
	}

} //debug

//memtable转换为sstable
//文件中第一个数据是size（用于跳转至索引区）//然后是level和num（第几级第几个）
void KVStore::mem_to_ss(SkipList memtable)
{
	//加载进内存中offset
	buffer *cur_buf = new buffer;
	buffer *bef = offset_head;
	bool flag=true;
	if(offset_head->next&&offset_head->next->level==0) flag=false;
	while (bef->next && bef->next->level == 0)
	{
		bef = bef->next;
	}//bef是level0的最后一个（包括offset_head)
	if(bef->next) cur_buf->next = bef->next;
	bef->next = cur_buf;
	cur_buf->num = (flag ? 0 : bef->num + 1);//?

	node *cur_node = memtable.bottom_head->right;
	cur_buf->min = cur_node->key; //记录最小值
	uint64_t off =0;//未考虑size的占位
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
    //fs::remove(path);
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
		cur=cur->next;
	}
	out.close();
}

void KVStore::check_merge(int level)
{
	//return;
	int max_num = 0;
	string path1 = "./data/level" + to_string(level) + "/" + to_string(max_num) + ".dat";
	while (fs::exists(path1))
	{
		max_num++;
		path1 = "./data/level" + to_string(level) + "/" + to_string(max_num) + ".dat";
	}
	max_num--;
    ///cout<<"num of file: "<<max_num<<'\n';
	if (max_num < pow(2, level + 1)-1)
		{///cout<<"dont need merge\n";
			return;} //判断是否溢出
	if (level == 0)
	{
		//cout<<"merge 0\n";

		block *x1 = new block;
		load_block(0, 1,x1);
		block *x2 = new block;
		load_block(0, 0,x2);
        ///cout<<"finish load\n";
		block *x3 = new block;
		block *x4 = new block;
		merge_block(x1, x2,x3,x4);
		x4->num=1;
		//cout<<"to update "<<x3->level<<' '<<x3->num<<'\n';
		update_buffer(x3);
		//cout<<"to update "<<x4->level<<' '<<x4->num<<'\n';
		update_buffer(x4);
		///cout<<"merge block successful\n";
		merge_ss(x3);
		merge_ss(x4);
		///cout<<"finish merge\n";
		//删除level0中的文件
		int num = 0;
		while (num <= max_num)
		{
			string path = "./data/level" + to_string(level) + "/" + to_string(num) + ".dat";
			//if(!fs::remove(path)) break;
			del_buffer(level,num);
			fs::remove(path);
			//checkNum(level);
			num++;
		}
		///cout<<"finish delete file\n";
	}
	else
	{
		while (max_num >= pow(2, level + 1)-1)
		{
			block *x1 = new block;
			load_block(level, max_num,x1); //取最后那个
			merge_ss(x1);

			//删除对应文件
			string path = "./data/level" + to_string(level) + "/" + to_string(max_num) + ".dat";
			del_buffer(level,max_num);
			fs::remove(path);
			//checkNum(level);
			max_num--;
		}
	}
	checkNum(level);
}
//待调试
void KVStore::merge_block(block *x1, block *x2,block* x3,block* x4)
{
	uint64_t size = 0;
	block_node *cur_1 = x1->head;
	block_node *cur_2 = x2->head;
	block_node * spot;
	bool flag=false;

	// if (x3)
	// 	delete x3;
	block *new_block = x3; 
    block* bigger_block=x4;
	//x3 = new_block;

	block_node *cur = new block_node;
	new_block->level = x2->level;
	new_block->head = cur;
	bigger_block->level=x2->level;
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
			cur->next=new block_node;
			cur=cur->next;
		}//选出 头

		while (cur_1 && cur_2)
		{
			//block_node *newone = new block_node;
			//cur->next = newone;
			//newone->last = cur;
			//size += (sizeof(cur->key) + cur->val.length());
			//cur = cur->next;
			// if(size>=max_size&&!flag){
			// 	spot=cur;
			// 	flag=true;
			// }
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
			cur->next=new block_node;
			size+=(sizeof(cur->key) + cur->val.length());
			if(size>=max_size&&!flag){
				spot=cur;
				flag=true;
			}
			cur=cur->next;
		}
		
		if (!(cur_1 && cur_2) & (cur_1 || cur_2)) //只剩一边
		{
			if (!cur_1)
			{
				//cur_2->last = cur;
				cur->key=cur_2->key;
				cur->next=cur_2->next;
				cur->val=cur_2->val;
				cur = cur->next;
			}
			if (!cur_2)
			{
				cur->key=cur_1->key;
				cur->next=cur_1->next;
				cur->val=cur_1->val;
				cur = cur->next;
			}
			//if(size==0) cur_block->min=cur->key;
			while (cur)
			{
				//cur->offset = size;
				size += (sizeof(cur->key) + cur->val.length());
				if (cur->next)
					cur = cur->next;
				else
				{
					//cur->offset = size;
					cur->next=new block_node;
					break;
				}
				if(size>=max_size&&!flag){
				spot=cur;
				flag=true;
			    }
			}
		}
	}
	
	block_node* a=new_block->head;

	while(a->next)
	{
		//cout<<a->key<<' ';
		a->next->last=a;
		a=a->next;
	}
	a=a->last;
	a->next=nullptr;
	
	//248  | 249  (250)            279 280         spot=250
	if(flag) {
		bigger_block->head=spot;
		block_node* end=spot->last;
		//std::cout<<"merge:"<<spot->last->key<<' '<<spot->last->val<<' '<<spot->key<<' '<<spot->val<<'\n';
		spot->last->next=nullptr;
		spot->last=nullptr;
		//std::cout<<"merge:"<<end->key<<' '<<end->val<<' '<<a->key<<' '<<a->val<<'\n';
	} 
	
	
}

void KVStore::update_buffer(block *x)
{
	if(!x->head) return;
	buffer *cur = offset_head;
	buffer *newbuf = new buffer;
	block_node *cur_node = x->head;
	x->min = cur_node->key;
	newbuf->min=cur_node->key;
	newbuf->level = x->level;
	newbuf->num = x->num;
	while (cur->next)
	{
		
		if (cur->next->level == x->level && cur->next->num == x->num) 
		{
			if (cur->next->next)
				newbuf->next = cur->next->next;
			delete cur->next;
			cur->next = newbuf;
			break;
		} //找到对应buffer
		else if (cur->next->level == (x->level + 1) && cur->level == x->level){
			if (cur->next->next)
				newbuf->next = cur->next->next;
			cur->next = newbuf;
			break;
		}//或者插在对应level的最后一个
		
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
	newbuf->max=cur_node->key;
	//cout<<"update: "<<x->level<<' '<<x->num<<' '<<x->min<<' '<<x->max<<'\n';
	x->size = off + (sizeof(uint64_t) + cur_node->val.length());
} //待debug

void KVStore::del_buffer(int level,int num){
	buffer* cur=offset_head;
	buffer* target;
	if(level==0&&num==0)
	{
		if(cur->next)
		{target=cur->next;
		if(target->next)
		cur->next=target->next;
		else cur->next=nullptr;
		delete target;}
		return;
	}//防止删除head
	while (cur->next)
	{
		if(cur->next->level==level&&cur->next->num==num)
		{
			target=cur->next;
			if(target->next)
			cur->next=target->next;
			else
			{
				 cur->next=nullptr;
			}
			delete target;
			return;			
		}
		if (cur->next->level>level)
		{
			return;
		}
		cur=cur->next;	
	}
}

void KVStore::merge_ss(block *x)
{ //调用该函数后记得删除x
	int min = x->min;
	int max = x->max;
	int level = x->level + 1;
	int i = 0;
	num = new nm[pow(2, level + 1) + 3]; //用于排序
	///cout<<"start merge to level "<<level<<'\n';
	buffer *cur = offset_head;
	//cout<<"offset: ";
	while (cur->next )
	{
		if(cur->level!=level)
		cur = cur->next;
		else break;
		//cout<<cur->level<<':'<<cur->num<<' ';
	}
	//cout<<'\n';
	if (cur->level != level)
	{
		cur->next = new buffer;
		cur = cur->next;
		cur->level = level;
		x->level=level;
		cur->num = 0;
		x->num=0;
		///cout<<"create new level\n";
		//  cur->min=x->min;cur->max=x->max;
		update_buffer(x);
		///cout<<"create "<<x->level<<' '<<x->num<<'\n';
		///cout<<"update buffer finish\n";
		block_to_ss(x);
		///cout<<"block to sstable finish\n";
		return;
	} //新建层
    int max_num=-1;
	while (cur->level == level)
	{
		if (cur->max >= min&&cur->min<=min)
		{
			num[i].num = cur->num;
			num[i].min = cur->min;
			i++;
		}
		else if (cur->min <= max &&cur->max>=max)
		{
			num[i].num = cur->num;
			num[i].min = cur->min;
			i++;
		}
		else if(cur->min>=min&&cur->max<=max)
		{
			num[i].num = cur->num;
			num[i].min = cur->min;
			i++;
		}
		else if(cur->min<=min&&cur->max>=max)
		{
			num[i].num = cur->num;
			num[i].min = cur->min;
			i++;
		}
		max_num=cur->num;
		if(cur->next) cur = cur->next;
		else break;
	} //取出所有有关的buffer并找到末尾值
	//cout<<max_num<<'\n';
	///cout<<"get all buffer\n";
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
	block *x2 = new block;
	block *x3;//=new block;
	block *x4;// = new block;
	block* tmp=new block;
	bool isTwo=true;
	for (int j = 0; j < i; j++)
	{
		///cout<<"load block from level "<<level<<'\n';
		x3=new block;
		x4=new block;
		load_block(level, num[j].num, x2);
		//cout<<"with:"<<level<<' '<<num[j].num<<'\n';	
		merge_block(x,x2,x3,x4);
		x3->num=x2->num;
		if(x4->head) 
        {x=x4;	
		isTwo=true;
		//cout<<"true\n";
		// if(x3) delete x3;
		// update_buffer(x3);
		string path = "./data/level" + to_string(level) + "/" + to_string(num[j].num) + ".dat";
		del_buffer(level,num[j].num);
		fs::remove(path); //删除对应文件
		update_buffer(x3);
		block_to_ss(x3);
		}
		else
		{
			isTwo=false;
			//cout<<"false\n";
			x=x3;//考虑到合并之后的x3可能与其后的block范围有交集
			string path = "./data/level" + to_string(level) + "/" + to_string(num[j].num) + ".dat";
			del_buffer(level,num[j].num);
		    fs::remove(path); //删除对应文件
		}
		x3=tmp;
		x4=tmp;
	}
	x->level=level;
	if(isTwo)	
	{x->num=max_num+1;
	//cout<<"new: "<<x->num<<'\n';
	}
	//有多出来的block才需要增加num
	if(x->head){
		string path = "./data/level" + to_string(x->level) + "/" + to_string(x->num) + ".dat";
		del_buffer(level,x->num);
		fs::remove(path); //删除对应文件
	update_buffer(x);
	block_to_ss(x);}
	checkNum(level);
	check_merge(level);	
	delete num;
}
void KVStore::checkNum(uint64_t level){
	buffer* cur=offset_head->next;
    while(cur->next)
	{
		cur=cur->next;
		if(cur->level==level) break;
		
	}//找到buffer中对应行
	if(cur->level==level){
		int a=cur->num;
		if(cur->num!=0)
		{
			string path="./data/level"+to_string(level)+"/"+to_string(cur->num)+".dat";
			string newp="./data/level"+to_string(level)+"/"+to_string(0)+".dat";
            rename(path.c_str(),newp.c_str());
			cur->num=0;
		}
		while (cur->next&&cur->next->level==level)
		{
			a=cur->num;
			cur=cur->next;
			if(cur->num!=(a+1))
			{
				string path="./data/level"+to_string(level)+"/"+to_string(cur->num)+".dat";
			string newp="./data/level"+to_string(level)+"/"+to_string(a+1)+".dat";
            rename(path.c_str(),newp.c_str());
			cur->num=a+1;
			}
		}
		
	}
	//如果数字不连续则改文件名同时改数字

}
bool KVStore::search_from_offset(uint64_t key)
{
	//std::cout<<"search\n";
	if(!offset_head->next) 
	{//std::cout<<" no offset\n";
		return false;}
	buffer *cur_buf = offset_head->next;
	//std::cout<<cur_buf->level<<' '<<cur_buf->num<<' '<<cur_buf->min<<' '<<cur_buf->max<<'\n';
	buffer* target=new buffer;
	buf_node *spot = new buf_node;
	bool flag = false;//是否找到
	//std::cout<<"begin\n";
	while (!flag && cur_buf)
	{
		//std::cout<<cur_buf->min<<' '<<cur_buf->max<<'\n';
		while (cur_buf->min > key || cur_buf->max < key)
		{
			//std::cout<<"? ";
			if (cur_buf->next)
			{	cur_buf = cur_buf->next;
			  //std::cout<<cur_buf->level<<' '<<cur_buf->num<<' '<<cur_buf->min<<' '<<cur_buf->max<<'\n';
			  }
			else
			{
				//std::cout<<"x\n";
				return false;
			}
			//std::cout<<". \n";
		}
		//std::cout<<"1\n";
		buf_node *cur = cur_buf->head;
		while (cur->key != key&&cur->next)
		{
			cur = cur->next;
			if (cur->key > key)
				break;
		}
		//std::cout<<"2\n";
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
		//std::cout<<"find a buffer\n";
	cur_level=cur_buf->level;
	while (cur_buf->level == 0 && cur_buf->next&&cur_buf->next->level==0)
	{
		//cur_level = 0;
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
			if(cur->next)
			cur = cur->next;
			else
			{
				break;
			}
			
			if (cur->key > key)
				break;
		}
		if (cur->key == key)
		{
			spot = cur;
			target=cur_buf;
		}	
	}//在零层时需要取num最大的
    //std::cout<<"level0 buf\n";
	cur_num=target->num;
	cur_offset=spot->offset+2*sizeof(uint64_t);
	if(spot->next) 
	{cur_len=spot->next->offset-spot->offset-sizeof(uint64_t);
	isEnd=false;}
	else
	{
		isEnd=true;
	}
	return flag;
}

void KVStore::reset_block(block* x){
	if(!x->head) return;
	block_node* cur=x->head;
	while (cur->next)
	{
		cur=cur->next;
		delete cur->last;
	}
	delete cur;
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
	len=size-offset+sizeof(uint64_t);}
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
	//memtable.put(key,s);
    //  int start;
	//  int end;
	// start = clock();
	if (memtable.size < memtable.max)
		{/////cout<<"mem: ";
			memtable.put(key, s);
		/////cout<<" put "<<key<<' '<<s<<'\n';
		}
	else
	{
		//std::cout<<"mem to ss\n";
		mem_to_ss(memtable);
		   ///cout<<"check 0 level merge after mem to ss\n";
		check_merge(0);
		   ///cout<<"reset mem\n";
		memtable.reset();
		/////cout<<"mem: ";
			memtable.put(key, s);
		/////cout<<" put "<<key<<' '<<s<<'\n';
	}
	//end = clock();
	//if(end - start >2)
	//cout << end-start << " ";
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	string val;

	val = memtable.get(key);
	//return memtable.get(key);
	if (val!="")
		{  // cout<<"mem: get "<<key<<' '<<val<<'\n';
			return val;}
	else if(memtable.cur_node&&memtable.cur_node->key==key) return "";
	//考虑内存中删掉了但sstable中还有的情况
	if (!search_from_offset(key))
		{return "";}
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
	if (search_from_offset(key)) //从offset中搜索
	{
		///cout<<"offset: del "<<key<<'\n';
		put(key, "");

		///cout<<"offset: del "<<key<<"success\n";
		return true;
	}
	else
	{
		if (!memtable.search(key) || memtable.cur_node->key != key)
			{///cout<<"mem: del "<<key<<" false\n";
				return false;}
		else
			{   /////cout<<"mem: del "<<key<<'\n';
				target = memtable.cur_node;}
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
	/////cout<<s;
	if (tot_level == 0)
	{   /////cout<<"tot_level=0\n";
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
		/////cout<<"find\n";
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
		/////cout<<"update "<<key<<'\n';
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
		/////cout<<"insert "<<key<<'\n';
		//int top = 1;
		//node_head *cur_head=new node_head;
		//cur_head=bottom_head;
		node_head *cur_head = bottom_head;
		while (ifup()) //是否增长
		{
			/////cout<<"grow\n";
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
		/////cout<<"finish grow\n";
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
	//cout<<"1 ";
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
		//cout<<"2 ";
		if (target->up)
		{
			next = target->up;
			next->down = nullptr;
			cur_head = cur_head->up;
		}
		else
			next = nullptr;
			//cout<<"3 ";
		delete target;
		//cout<<"4 ";
		if (next)
			target = next;
		else
			break;
			//cout<<"5\n";
	}
	//cout<<"6 ";
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
	size=0;
	tot_level=0;
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
  //srand(2);
}

SkipList::~SkipList()
{
	//	delete bottom_head;
	//	delete top_head;
}
