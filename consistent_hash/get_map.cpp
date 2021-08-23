#include <iostream>
#include<cmath>
#include <vector>
#include <algorithm>
#include<iomanip>
#include "consistent_hash.h"
#include <sstream>
#include <fstream>
#include <string>

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

vector<string> testSplit(string srcStr, const string& delim)
{
	int nPos = 0;
	vector<string> vec;
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
    string line;
    if(!in) {
        cout <<"no such file" << endl;
        return nullptr;
    }
    int osd_id = 1;
    int bucket_id = -1;
    getline (in, line);
    int cow_num = atoi(line.c_str());
    for(int i = 0; i < cow_num; i++) {
        getline (in, line);
        int host_num = atoi(line.c_str());
        for(int j = 0; j < host_num; j++) {
            getline (in, line);
            vector<string> osd(testSplit(line, " "));
            for(int k = 0; k < osd.size(); k++) {
                cout << atoi(osd[k].c_str()) << " ";
            }
            cout << endl;
        }
    }
    return nullptr;
}

int main()
{
    gen_map("./map.txt");
    return 0;
}

