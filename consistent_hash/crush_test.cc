#include <iostream>
#include<cmath>
#include <vector>
#include <algorithm>
#include<iomanip>
#include <fstream>
#include <set>

using namespace std;

#define crush_hash_seed 1315423911

#define crush_hashmix(a, b, c) do {			\
		a = a-b;  a = a-c;  a = a^(c>>13);	\
		b = b-c;  b = b-a;  b = b^(a<<8);	\
		c = c-a;  c = c-b;  c = c^(b>>13);	\
		a = a-b;  a = a-c;  a = a^(c>>12);	\
		b = b-c;  b = b-a;  b = b^(a<<16);	\
		c = c-a;  c = c-b;  c = c^(b>>5);	\
		a = a-b;  a = a-c;  a = a^(c>>3);	\
		b = b-c;  b = b-a;  b = b^(a<<10);	\
		c = c-a;  c = c-b;  c = c^(b>>15);	\
	} while (0);

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
} node;

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


static unsigned int crush_hash32_rjenkins1_3(unsigned int a, unsigned int b, unsigned int c)
{
	unsigned int hash = crush_hash_seed ^ a ^ b ^ c;
	unsigned int x = 231232;
	unsigned int y = 1232;
	crush_hashmix(a, b, hash);
	crush_hashmix(c, x, hash);
	crush_hashmix(y, a, hash);
	crush_hashmix(b, x, hash);
	crush_hashmix(y, c, hash);
	return hash;
}

int bucket_straw2_choose(node *bucket, int x, int r)
{
    unsigned int i, high = 0;
	unsigned int u;
    double draw, high_draw = 0;
    for(int i = 0; i < bucket->item_num; i++) {
        u = crush_hash32_rjenkins1_3(x, bucket->item[i].id, r);
        u &= 0xffff;
        draw = log(u / 65536.0) / bucket->item[i].weight;
        //cout << "u:" << u << "  draw:" << draw << " " << log(u / 65536.0) << endl;
        if (i == 0 || draw > high_draw) {
			high = i;
			high_draw = draw;
		}
	}
    return high;
}

node * fristn(node *bucket, int pg_id, int r)  //return 
{
    if(bucket->type == HOST) {
        return bucket;
    }
    int item_idx = bucket_straw2_choose(bucket, pg_id, r);
    return fristn(&(bucket->item[item_idx]), pg_id, r);
}

vector<int> do_crush(node* map, int pg_id, int rep_num)
{
    vector<int> ret_osd;
    vector<int> ret_host;
    for(int r = 1; ret_osd.size() < rep_num && r < 10; r++) {
        node *host_node = fristn(map, pg_id, r);
        int host_id = host_node->id;
        //cout << "host_id:" << host_id << endl;
        vector<int>::iterator iter=std::find(ret_host.begin(),ret_host.end(),host_id);
        if(iter == ret_host.end()) {
            ret_host.push_back(host_id);
            int idx = bucket_straw2_choose(host_node, pg_id, r);
            ret_osd.push_back(host_node->item[idx].id);
        }
    }
    return ret_osd;
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

    for(int i = 0; i < cow_num; i++) {
        getline (in, line);
        int host_num_per_cow = atoi(line.c_str());

        node *cow_node = &(root->item[i]);
        cow_node->weight = 0;
        cow_node->item_num = host_num_per_cow;
        cow_node->item = new node[host_num_per_cow];
        cow_node->type = COW;
        cow_node->id = bucket_id--;

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
            
            for(int k = 0; k < osd_num_per_host; k++) {
                node *osd_node = &(host_node->item[k]);
                osd_node->id = atoi(id[k].c_str());
                osd_node->item_num = 0;
                osd_node->item = nullptr;
                osd_node->type = OSD;
                osd_node->weight = atoi(osd[k].c_str());
                host_node->weight += osd_node->weight;
            }
            cow_node->weight += host_node->weight;
        }
        root->weight += cow_node->weight;
    }

    return root;
}

void delete_map(node *root)
{
    for(int i = 0; i < root->item_num; i++) {
        node *cow_node = &(root->item[i]);
        for(int j = 0 ; j < cow_node->item_num; j++) {
            node *host_node = &(cow_node->item[j]);
            delete []host_node->item;

        }
        delete []cow_node->item;
    }
    delete []root->item;
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

int cmp_pg(int pg, const pg_to_osd &pg_1, const pg_to_osd &pg_2)
{
    int num = 0;
    set<int> osd_set;
    osd_set.insert(pg_1.id_1);
    osd_set.insert(pg_1.id_2);
    osd_set.insert(pg_1.id_3);
    if(osd_set.insert(pg_2.id_1).second) {
        num++;
    }
    if(osd_set.insert(pg_2.id_2).second) {
        num++;
    }
    if(osd_set.insert(pg_2.id_3).second) {
        num++;
    }
    cout << "[" << pg_1.id_1 << "," << pg_1.id_2 << "," << pg_1.id_3 << "]";
    cout << "[" << pg_1.id_1 << "," << pg_1.id_2 << "," << pg_1.id_3 << "]";
    return num;
}

int main(int argc, char* argv[])
{
    if (argc != 2) {
        cout << "usage: " << argv[0] << " pg_num" << endl;
        return 0;
    }
    int pg_num = atoi(argv[1]);
    node *root =  nullptr;
    vector<pg_to_osd> data_index(pg_num, pg_to_osd(-1, -1, -1));
    vector<int> result(50, 0);   // 节点存放数据数目统计
    root = gen_map("./map.txt");
    print_map(root);

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
    for(int i = 1; i <= max_osd_id; i++) {
        cout << "osd_" << i << "_pg_num:" << result[i] << endl;
    }
    
        //添加一个OSD
    vector<int> result_add(50, 0);   // 节点存放数据数目统计
    vector<pg_to_osd> data_index_add(pg_num, pg_to_osd(-1, -1, -1)); //数据存放节点位置，
    root = gen_map("./map_add.txt");
    print_map(root);

    /*
    for(int i = 0; i < hash_root->item.size(); i++) {
        cout << hash_root->item[i]->id << " " << hash_root->item[i]->weight << endl;
    }*/


    max_osd_id = -1;
    for(int i = 0; i < pg_num; i++) {
        vector<int> osds = do_crush(root, i, 3);
        data_index_add[i] = pg_to_osd(osds[0], osds[1], osds[2]);
        sort(osds.begin(), osds.end());
        result_add[osds[0]]++;
        result_add[osds[1]]++;
        result_add[osds[2]]++;
        if(osds[2] > max_osd_id) {
            max_osd_id = osds[2];
        }
        //print_vec(do_crush(root, i, 3));
    }
    for(int i = 1; i <= max_osd_id; i++) {
        cout << "osd_" << i << "_pg_num:" << result_add[i] << endl;
    }

    delete_map(root);
    int move_num = 0;
    for(int i = 0; i < pg_num; i++) {
        move_num += cmp_pg(i, data_index[i], data_index_add[i]);
    }
    cout << "moved pg: " << move_num << endl;                         


    return 0;
}
