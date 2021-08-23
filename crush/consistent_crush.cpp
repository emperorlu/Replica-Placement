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

node* gen_map(string file_name)
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
            host_node->item_hash = new ConsistentHash(osd_num_per_host, 5000, host_item_weight);
        }
        root->weight += cow_node->weight;
        root_item_weight.push_back(cow_node->weight);
        cow_node->item_hash = new ConsistentHash(host_num_per_cow, 5000, cow_item_weight);
    }
    root->item_hash = new ConsistentHash(cow_num, 5000, root_item_weight);
    return root;
}

node * fristn(node *bucket, int pg_id, int r)  //return 
{
    if(bucket->type == HOST) {
        return bucket;
    }
    stringstream ss;
    ss << pg_id << r;
    string key = ss.str();
    size_t item_idx = bucket->item_hash->GetServerIndex(key);
    return fristn(&(bucket->item[item_idx]), pg_id, r);
}

vector<int> do_crush(node* map, int pg_id, int rep_num)
{
    vector<int> ret_osd;
    vector<int> ret_host;
    for(int r = 1; ret_osd.size() < rep_num  && r <= 10; r++) {
        node *host_node = fristn(map, pg_id, r);
        int host_id = host_node->id;
        vector<int>::iterator iter=std::find(ret_host.begin(),ret_host.end(),host_id);
        if(iter == ret_host.end()) {
            ret_host.push_back(host_id);
            stringstream ss;
            ss << pg_id << r;
            string key = ss.str();
            size_t idx = host_node->item_hash->GetServerIndex(key);
            ret_osd.push_back(host_node->item[idx].id);
        }
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

int main()
{
    node *root = gen_map("./map.txt");
    print_map(root);
    
    for(int i = 0; i < 20; i++) {
        print_vec(do_crush(root, i, 3));
    }
    
    delete_map(root);
    return 0;
}
