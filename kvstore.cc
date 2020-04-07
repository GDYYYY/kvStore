#include "kvstore.h"
#include <string>
using namespace std;

KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir)
{
}

KVStore::~KVStore()
{
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	if(memtable.size<memtable.max)
	memtable.put(key, s);
	else{
		//update(memtable,buffer);
		//memtable.reset();
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
bool KVStore::del(uint64_t key)
{
	node *target ;
	if(!memtable.search(key)||memtable.cur_node->key!=key) return false;
	else target=memtable.cur_node;
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
		size++;
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
		size++; //在底层插入
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
        size--;
		node_head *cur_head = bottom_head;
		node* target=target1;
		node* next=target->up?target->up:nullptr;
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
                cur_head->right=nullptr;
            else
            {
                target->left->right=nullptr;
            }

        }//左右连接
        if(target->up)
        {next=target->up;
            next->down=nullptr;
            cur_head=cur_head->up;}
        else next=nullptr;
        delete target;
        if(next) target=next;
        else break;
    }
    leveldown();
    return true;
}
void SkipList::leveldown(){
    node_head * cur_head=top_head;
    node_head* next=cur_head->down?cur_head->down:nullptr;
    while (!cur_head->right&&cur_head!=bottom_head)
    {
        top_head=cur_head->down;
        delete cur_head;
        tot_level--;

        if(next){
            cur_head=next;
            next=cur_head->down?cur_head->down:nullptr;
        }
        else break;
    }
    if(!bottom_head->right) tot_level--;
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
