#include <iostream>
#include<cmath>
#include <vector>
#include <algorithm>
#include<iomanip>
#include "consistent_hash.h"
#include <sstream>
#include <fstream>

using namespace std;

const int OSD = 1;
const int HOST = -2;
const int COW = -3;
const int ROOT = 0;

typedef struct node {
    int id;
    int weight;
    int  type;
    int item_num;
    node *item;
    ConsistentHash *item_hash;
} node;

typedef struct root_node {
    vector<node *> item;
    ConsistentHash *item_hash;
} root_node;


typedef struct pg_to_osd {
    int id_1, id_2, id_3;
    pg_to_osd(int id_1, int id_2, int id_3) : id_1(id_1), id_2(id_2), id_3(id_3) {}
} pg_to_osd;

string& trim(string &s)
{
    if (s.empty())
    {
        return s;
    }
    s.erase(0,s.find_first_not_of(" "));
    s.erase(s.find_last_not_of(" ") + 1);
    return s;
}

vector<string> testSplit(string srcStr, const string& delim)
{
	int nPos = 0;
	vector<string> vec;
    srcStr = trim(srcStr);
	nPos = srcStr.find(delim.c_str());
	while(-1 != nPos)
	{
		string temp = srcStr.substr(0, nPos);
		vec.push_back(temp);
		srcStr = srcStr.substr(nPos+1);
		nPos = srcStr.find(delim.c_str());
	}
	vec.push_back(srcStr);
	return vec;
}

node* gen_map(string file_name, root_node * hash_node, int fault_domain)
{
    ifstream in(file_name);
    string line, id_line;
    if(!in) {
        cout <<"no such file" << endl;
        return nullptr;
    }
    int osd_id = 1;
    int bucket_id = -1;
    getline (in, line);
    int cow_num = atoi(line.c_str());

    node *root = new node();
    root->id = 0;
    root->weight = 0;
    root->type = ROOT;
    root->item_num = cow_num;
    root->item = new node[cow_num];
    vector<int> root_item_weight;
    root_item_weight.clear();

    for(int i = 0; i < cow_num; i++) {
        getline (in, line);
        int host_num_per_cow = atoi(line.c_str());

        node *cow_node = &(root->item[i]);
        cow_node->weight = 0;
        cow_node->item_num = host_num_per_cow;
        cow_node->item = new node[host_num_per_cow];
        cow_node->type = COW;
        cow_node->id = bucket_id--;
        vector<int> cow_item_weight;
        cow_item_weight.clear();

        for(int j = 0; j < host_num_per_cow; j++) {
            getline (in, line);
            getline (in, id_line);
            vector<string> osd(testSplit(line, " "));
            vector<string> id(testSplit(id_line, " "));
            int osd_num_per_host = osd.size();

            node *host_node = &(cow_node->item[j]);
            host_node->item_num = osd_num_per_host;
            host_node->weight = 0;
            host_node->item = new node[osd_num_per_host];
            host_node->type = HOST;
            host_node->id = bucket_id--;
            vector<int> host_item_weight;
            host_item_weight.clear();
            
            for(int k = 0; k < osd_num_per_host; k++) {
                node *osd_node = &(host_node->item[k]);
                osd_node->id = atoi(id[k].c_str());
                osd_node->item_num = 0;
                osd_node->item = nullptr;
                osd_node->type = OSD;
                osd_node->weight = atoi(osd[k].c_str());
                host_node->weight += osd_node->weight;
                host_item_weight.push_back(osd_node->weight);
            }
            cow_node->weight += host_node->weight;
            cow_item_weight.push_back(host_node->weight);
            if(fault_domain >= HOST) {
                host_node->item_hash = new ConsistentHash(osd_num_per_host, 5000, host_item_weight);
            } else {
                host_node->item_hash = nullptr;
            }
        }
        root->weight += cow_node->weight;
        if(fault_domain >= COW) {
            cow_node->item_hash = new ConsistentHash(host_num_per_cow, 5000, cow_item_weight);
        } else {
            cow_node->item_hash = nullptr;
        }
    }

    cout << "aaaa" << endl;
    for(int i = 0; i < root->item_num; i++) {
        node *cow_node = &(root->item[i]);
        if(fault_domain == COW) {
            hash_node->item.push_back(cow_node);
            root_item_weight.push_back(cow_node->weight);
            continue;
        }
        for(int j = 0 ; j < cow_node->item_num; j++) {
            node *host_node = &(cow_node->item[j]);
            if(fault_domain == HOST) {
                hash_node->item.push_back(host_node);
                root_item_weight.push_back(host_node->weight);
                continue;
            }

            for(int k = 0; k < host_node->item_num; k++) {
                node *osd_node = &(host_node->item[k]);
                if(fault_domain == OSD) {     
                    hash_node->item.push_back(osd_node);
                    root_item_weight.push_back(osd_node->weight);
                    continue;
                }
            }
        }
    }
    cout << "bbbb" << endl;
    hash_node->item_hash = new ConsistentHash(hash_node->item.size(), 5000, root_item_weight);

    return root;
}


int bucket_straw2_choose(node *bucket, int x)
{
    unsigned int i, high = 0;
    unsigned int u;
    double draw, high_draw = 0;
    for(int i = 0; i < bucket->item_num; i++) {
        u = crush_hash32_rjenkins1_2(x, bucket->item[i].id); 
        u &= 0xffff;
        draw = log(u / 65536.0) / bucket->item[i].weight;
        if (i == 0 || draw > high_draw) {
            high = i;
            high_draw = draw;
        }
    }
    return high;
}


int fristntoleaf(node *bucket, int pg_id)  //return       
{   
    if(bucket->type == OSD) {
        return bucket->id;
    }
    int item_idx = bucket_straw2_choose(bucket, pg_id);
    return fristntoleaf(&(bucket->item[item_idx]), pg_id);
}



vector<int> do_crush(root_node* map, int pg_id, int rep_num)
{
    rep_num =  rep_num <= map->item.size() ? rep_num : map->item.size();
    vector<int> ret_osd;

    vector<size_t> bucket_vec = map->item_hash->GetServerIndex(pg_id, rep_num);

    for(int i = 0; i < rep_num; i++) {
        node *bucket_node = map->item[i];
        ret_osd.push_back(fristntoleaf(bucket_node, pg_id));
    }
    return ret_osd;
}

void delete_map(node *root)
{
    for(int i = 0; i < root->item_num; i++) {
        node *cow_node = &(root->item[i]);
        for(int j = 0 ; j < cow_node->item_num; j++) {
            node *host_node = &(cow_node->item[j]);
            delete host_node->item_hash;
            delete []host_node->item;

        }
        delete cow_node->item_hash;
        delete []cow_node->item;
    }
    delete []root->item;
    delete root->item_hash;
    delete root;
}

void print_map(node *root) 
{
    cout << "root:" << root->id << "(" << root->weight << ")" << endl;
    for(int i = 0; i < root->item_num; i++) {
        node *cow_node = &(root->item[i]);
        cout << "\tcow:" << cow_node->id << "(" << cow_node->weight << ")" << endl;
        for(int j = 0 ; j < cow_node->item_num; j++) {
            node *host_node = &(cow_node->item[j]);
            cout << "\t\thost:" << host_node->id << endl << "\t\t\t";
            for(int k = 0; k < host_node->item_num; k++) {
                node *osd_node = &(host_node->item[k]);
                cout << osd_node->id << "(" << osd_node->weight << ")" << " ";
            }
            cout << endl;
        }
    }
}

void print_vec(vector<int> vec)
{
    cout << "[";
    for(int i = 0; i < vec.size(); i++) {
        cout << vec[i] << " ";
    }
    cout << "]" << endl;
}

int main(int argc, char* argv[])
{
    if (argc != 2) {
        cout << "usage: " << argv[0] << " pg_num" << endl;
        return 0;
    }

    int pg_num = atoi(argv[1]);

    vector<int> result(50, 0);   // 节点存放数据数目统计
    vector<pg_to_osd> data_index(pg_num, pg_to_osd(-1, -1, -1)); //数据存放节点位置， 下标是数据值i，它存放在data_index[i]上

    root_node *hash_root = new root_node();
    node *root = gen_map("./map.txt", hash_root, HOST);
    print_map(root);
    /*
    int max_osd_id = -1;
    for(int i = 0; i < pg_num; i++) {
        vector<int> osds = do_crush(root, i, 3);
        sort(osds.begin(), osds.end());
        data_index[i] = pg_to_osd(osds[0], osds[1], osds[2]);
        result[osds[0]]++;
        result[osds[1]]++;
        result[osds[2]]++;
        
        if(osds[2] > max_osd_id) {
            max_osd_id = osds[2];
        }
        //print_vec(do_crush(root, i, 3));
    }
    for(int i = 1; i < max_osd_id; i++) {
        cout << "osd_" << i << "_pg_num:" << result[i] << endl;
    } 
    */
    for(int i = 0; i < hash_root->item.size(); i++) {
        cout << hash_root->item[i]->id << " " << hash_root->item[i]->weight << endl;
    }
    delete_map(root);
    delete hash_root;
    return 0;
}
