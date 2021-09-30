#include "consistent_hash.h"
#include <vector>
#include <iostream>
#include <sstream>
#include <stdlib.h>
#include <time.h>

using namespace std;

int main()
{
    int pg_num = 10000;
    int node_num = 100;
    int vnode_num = 5000;
    vector<int> result(node_num, 0);   // 节点存放数据数目统计
    vector<int> weight;
    weight.push_back(1);
    weight.push_back(1);
    weight.push_back(1);
    auto consistent_hash = new ConsistentHash(node_num, vnode_num, weight);
    srand(time(NULL));
    for(int i = 0; i < pg_num; i++) {
        int t = rand();
        size_t index = consistent_hash->GetServerIndex(t);
        cout << crush_hash32_rjenkins1(t) << endl;
        result[index]++;
    }
    for(int i=0;i<node_num; ++i)
    {
        cout << "index " << i << ":" << result[i] << endl;
    }

    return 0;
}

