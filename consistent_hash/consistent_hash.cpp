#include "consistent_hash.h"
#include <sstream>
#include <functional>
#include <vector>
#include <algorithm>
#include <set>

ConsistentHash::ConsistentHash(int node_num, int vnode_num, const vector<int> &weights)
    : node_num_(node_num), vnode_num_(vnode_num), weights(weights) {
    set<size_t> t;
    for (int i = 0; i < node_num_; ++i) {
        for (int j = 0; j < vnode_num_*weights[i]; ++j) {
            //size_t partition = crush_hash32_rjenkins1_2(i, j); 
            size_t partition = crush_hash32_rjenkins1_2(i, j) & 0xFFFFFFFF;
            server_nodes_.insert(pair<size_t, size_t>(partition, i));
            //if(!(t.insert(partition).second)) {
            //    cout << "vnode :" << partition  << " " << i << " " << j << endl; 
            //}
        }
    }
    t.clear();
}

ConsistentHash::~ConsistentHash() {
    server_nodes_.clear();
}

size_t ConsistentHash::GetServerIndex(const int &key) {
    size_t partition = crush_hash32_rjenkins1(key) & 0xFFFFFFFF;
    //size_t partition = hash<int>{}(key);
    auto it = server_nodes_.lower_bound(partition);
    if (it == server_nodes_.end()) {
        // 未找到，则返回第一个，这样就构成了一个hash环
        return server_nodes_.begin()->second;
    } else {
        return it->second;
    }
}

vector<size_t> ConsistentHash::GetServerIndex(const int &key, int rep_num) {
    vector<size_t> ret;
    size_t partition = crush_hash32_rjenkins1(key) & 0xFFFFFFFF;
    //size_t partition = hash<int>{}(key);
    auto it = server_nodes_.lower_bound(partition);
    if (it == server_nodes_.end()) {
        // 未找到，则返回第一个，这样就构成了一个hash环
        ret.push_back(server_nodes_.begin()->second);
        it = server_nodes_.begin();
    } else {
        ret.push_back(it->second);
    }
    it++;
    while(ret.size() < rep_num) {
        if (it == server_nodes_.end()) {
            // 未找到，则返回第一个，这样就构成了一个hash环
            ret.push_back(server_nodes_.begin()->second);
            it = server_nodes_.begin();
        } else {
            ret.push_back(it->second);
        }
        it++;
    }
    return ret;
}


