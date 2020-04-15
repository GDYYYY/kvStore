#include "kvstore.h"
#include <string>
using namespace std;

KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir)
{
	//从文件中读取所有索引并调用函数建成链表
	load_offset();
}

KVStore::~KVStore()
{
}

void KVStore::load_offset()
{
	//打开文件（不会）
}

//memtable转换为sstable
//文件中第一个数据是size（用于跳转至索引区）//然后是level和num（第几级第几个）
void KVStore::mem_to_ss(SkipList memtable)
{
	//加载进内存中offset
	buffer *cur_buf = new buffer;
	cur_buf->next = offset_head->next;
	offset_head->next = cur_buf;		   //新建一个buffer
	cur_buf->num = cur_buf->next->num + 1; //num增加
	node *cur_node = memtable.bottom_head->right;
	cur_buf->min = cur_node->key; //记录最小值
	int off = 0;
	buf_node *cur_bnode = cur_buf->head;
	while (cur_node)
	{
		cur_bnode->key = cur_node->key;
		cur_bnode->offset = off;
		off += (sizeof(cur_node->key) + cur_node->val.length);
		if (cur_node->right)
			cur_node = cur_node->right;
		else
		{
			cur_buf->max = cur_node->key;
			cur_node = nullptr;
		}
	}
	//写入sstable(未写)
}

void KVStore::check_merge(int level)
{
	if (numofsstable <= 2 * *level)
		return; //判断是否溢出(未完成)
	if (level == 0)
	{
		block *x1 = load_block(0, 0);
		block *x2 = load_block(0, 1);
		block *x3 = merge_block(x1, x2);
		merge_ss(x3);
		merge_ss(x2);
		//删除level0中的文件（没写）
	}
	else
	{
		while (numofsstable > 2 * *level)
		{
			block *x1 = load_block(0, numofsstable); //取最后那个（未完成）
			merge_ss(x1);
			//删除对应文件（没写）
		}
	}
}
//待调试
block *KVStore::merge_block(block *x1, block *x2)
{
	int size = 0;
	block_node *cur_1 = x1->head;
	block_node *cur_2 = x2->head;
	if (cur_block)
		delete cur_block;
	block *new_block = new block;
	cur_block = new_block;
	//将cur_1插在合适的cur_2后
	while (size < max_size) //size不包括cur_2
	{
		if (!cur_1)
			break;
		while (cur_1->key > cur_2->key && size < max_size)
		{
			size += (sizeof(cur_2->key) + cur_2->val.length);
			if (!cur_2->next)
				break;
			cur_2 = cur_2->next;
			cur_2->offset = size;
		}
		if (cur_2->key == cur_1->key)
		{
			size = size - cur_2->val.length + cur_1->val.length;
			cur_2->val = cur_1->val;
		}
		else if (cur_1->key < cur_2->key)
		{
			block_node *newone = new block_node;
			newone->last = cur_2->last;
			newone->next = cur_2;
			newone->key = cur_1->key;
			newone->val = cur_1->val;
			newone->offset = size;
			if (cur_2 == x2->head)
				x2->head = newone;
			cur_2->last = newone;
			newone->last->next = newone;
			size += (sizeof(newone->key) + newone->val.length);
			// cur_2->offset=size;
		}
		if (cur_1->next)
			cur_1 = cur_1->next;
		else
		{
			cur_1 = nullptr;
			break;
		}
		if (cur_2->next)
		{
			cur_2->offset = size;
			cur_2 = cur_2->next;
		}
		else
		{
			cur_2->offset = size;
			cur_2->next = cur_1->next;
			size += (cur_2->val.length + sizeof(cur_2->key));
			cur_2 = cur_2->next;
			cur_2->offset = size;
			while (size < max_size)
			{
				size += (cur_2->val.length + sizeof(cur_2->key));
				cur_2 = cur_2->next;
				cur_2->offset = size;
			}
		}
		//if(size>=max_size) break;
	}
	x2->min = x2->head->key;
	x2->max = cur_2->last->key;
	cur_2->last->next = nullptr;
	//完成前半部分归并，存在x2
	size = 0;
	block_node *cur = new block_node;
	cur_block->level=x2->level;
	cur_block->head=cur;
	if (cur_1 || cur_2)
	{
		if(cur_1&&cur_2)
		{if (cur_1->key < cur_2->key)
		{
			cur->key = cur_1->key;
			cur->offset = size;
			cur->val = cur_1->val;
			if (cur_1->next)
				cur_1 = cur_1->next;
			else
				cur_1 = nullptr;
		}
		else if (cur_1->key == cur_2->key)
		{
			cur->key = cur_1->key;
			cur->offset = size;
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
			cur->offset = size;
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
			size += (sizeof(cur->key) + cur->val.length);
			cur = cur->next;
			if (cur_1->key < cur_2->key)
			{
				cur->key = cur_1->key;
				cur->offset = size;
				cur->val = cur_1->val;
				if (cur_1->next)
					cur_1 = cur_1->next;
				else
					cur_1 = nullptr;
			}
			else if (cur_1->key == cur_2->key)
			{
				cur->key = cur_1->key;
				cur->offset = size;
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
				cur->offset = size;
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
			if(size==0) cur_block->min=cur->key;
			while (cur)
			{
				cur->offset = size;
				size += (sizeof(cur->key) + cur->val.length);
				if (cur->next)
					cur = cur->next;
				else
				{
					cur->offset = size;
					break;
				}
			}
		}
	}
	cur_block->min=cur_block->head->key;
	//cur_block->max=cur->key;//可能空指针,所以在更新buffer的时候再处理
	// update_buffer(x2);
    // update_buffer(cur_block);
	return cur_block;
}
void KVStore::merge_ss(block* x){//调用该函数后记得删除x
     int min=x->min;
	 int max=x->max;
	 int level=x->level+1;
	 //nm num[20];
	 int i=0;
	 buffer* cur=offset_head;
	 while (cur->level!=level)
	 {
		cur=cur->next;
	 }
	 
	 while (cur->level==level)
	 {	 
		 if(cur->max>min)
		 {num[i].num=cur->num; 
		 num[i].min=cur->min;
		 i++;}
		 else if(cur->min<max)
		 {num[i].num=cur->num;
		 num[i].min=cur->min;
		  i++;}
		 cur=cur->next;		 
	 }
	 for(int m=0;m<i-1;m++)
	 {
		 for(int n=0;n<i-m-1;n++)
		 {
			 if(num[n].min>num[n+1].min)
			 {
				 int tmpnum=num[n+1].num;
				 int tmpmin=num[n+1].min;
				 num[n+1].num=num[n].num;
				 num[n+1].min=num[n].min;
				 num[n].min=tmpmin;
				 num[n].num=tmpnum;

			 }
		 }
	 }
	 //需要排序，否则归并会冲突
	 for(int j=0;j<i;j++){
		 block* x2=load_block(level,num[j]);
		 x=merge_block(x,x2);
		 update_buffer(x2);
		 //删除对应文件（没写）
		 block_to_ss(x2);
	 }
	 update_buffer(x);
	 block_to_ss(x);
	 check_merge(level);
	 
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
	return memtable.get(key);
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
//待改
bool KVStore::del(uint64_t key)
{
	node *target;
	if (!memtable.search(key) || memtable.cur_node->key != key)
		return false;
	else
		target = memtable.cur_node;
	return memtable.del(target);
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
		size += (sizeof(key) + s.length); //更新大小
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
		size = size - x->val.length + s.length; //更新大小
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
		size = size + s.length + sizeof(key); //在底层插入,更新大小
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
	size = size - sizeof(target1->key) - target1->val.length; //更新大小
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
