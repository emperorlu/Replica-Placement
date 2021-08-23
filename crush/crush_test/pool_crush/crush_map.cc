#include "crush_map.h"

struct node{
  int id;
  int pg_num;
  node(int id, int num) : id(id), pg_num(num) {}
  bool operator < (const node& a) const {
    return pg_num < a.pg_num; //大顶堆
  }
};

void crush_map::gen_map(int osd_num, int osd_per_host) {
  bucket b(bucket_id--);
  b.weight = 0;
  b.pg_num = 0;
  b.primary_pg = 0;
  root_weight = 0;
  for (int i = 0; i < osd_num; i++) {
    osd_weight.push_back(1);
    osd_pg_num.push_back(0);
    osd_primary_pg_num.push_back(0);
    osd_bucket.push_back(b.id);
    if (i%osd_per_host == 0 && i > 0) {
      root_weight += b.weight;
      buckets.push_back(b);
      b.id = bucket_id--;
      b.weight = 0;
      b.item.clear();
    }
    b.item.push_back(osd_id++);
    b.weight++;
  }
  root_weight += b.weight;
  buckets.push_back(b);
}

void crush_map::set_crushmap_pg_target(unsigned pool_size, unsigned pool_pg_num) {
  int pool_pgs = (pool_size-1) * pool_pg_num;
  int pool_primary_num = pool_pg_num;
  float pgs_per_weight = 1.0 * pool_pgs / root_weight;
  float primary_per_weight = 1.0 * pool_primary_num / root_weight;
  int duplicate_num, primary_num;
  for(auto & b : buckets) {
    b.pg_num = ceil(b.weight * pgs_per_weight);
    b.primary_pg = ceil(b.weight * primary_per_weight);

    for(int i = 0; i < b.item.size(); i++) {
      int item = b.item[i];
      duplicate_num = ceil(osd_weight[item] * pgs_per_weight);
      primary_num = ceil(osd_weight[item] * primary_per_weight);
      osd_pg_num[item] = duplicate_num;   
      osd_primary_pg_num[item] = primary_num;
    }
  }
}
void crush_map::adjust_crushmap_pg_target(unsigned pool_size, unsigned pool_pg_num,
                                          map<int, set<int>>& primary_change,                      
                                          map<int, set<int>>& duplicate_change) {
  int pool_pgs = (pool_size-1) * pool_pg_num;
  int pool_primary_num = pool_pg_num;
  float pgs_per_weight = 1.0 * pool_pgs / root_weight;
  float primary_per_weight = 1.0 * pool_primary_num / root_weight;
  int duplicate_num, primary_num;
  for(auto & b : buckets) {
    duplicate_num = ceil(b.weight * pgs_per_weight);
    primary_num = ceil(b.weight * primary_per_weight);
    b.pg_num = duplicate_num;
    b.primary_pg = primary_num;

    for(int i = 0; i < b.item.size(); i++) {
      int item = b.item[i];
      duplicate_num = ceil(osd_weight[item] * pgs_per_weight);
      primary_num = ceil(osd_weight[item] * primary_per_weight);
      /*
      if(osd_pg_num[item] != duplicate_num) {
        duplicate_change[duplicate_num - osd_pg_num[item]].insert(item);
      }
      if(osd_primary_pg_num[item] != primary_num) {
        primary_change[primary_num - osd_primary_pg_num[item]].insert(item);
      }
      */
      if(osd_to_pgs[item].size() != duplicate_num) {
        duplicate_change[duplicate_num - osd_to_pgs[item].size()].insert(item);
        //cout << __func__ << " osd:" << item << " target:" << duplicate_num << " cur:" << osd_to_pgs[item].size() << endl;
      }
      if(osd_to_primary_pgs[item].size() != primary_num) {
        primary_change[primary_num - osd_to_primary_pgs[item].size()].insert(item);
      }
      osd_pg_num[item] = duplicate_num;   
      osd_primary_pg_num[item] = primary_num;
    }
  }
}

void crush_map::init_pg_mappings(unsigned pool_size, unsigned pool_pg_num) {
  priority_queue<node> q;
  priority_queue<node> primary_q;
  for(auto& b : buckets) {
    q.push(node(b.id, b.pg_num));
    primary_q.push(node(b.id, b.primary_pg));
  }
  for(unsigned pg = 0; pg < pool_pg_num; pg++) { //choose bucket
    vector<int> out_bucket;
    vector<node> t;
    for(int i = 0; i < pool_size && !q.empty();) {
      if(i == 0) { //primary
        t.push_back(primary_q.top()); primary_q.pop();
        out_bucket.push_back(t[i].id);
        buckets_to_primary_pgs[t[i].id].insert(pg);
        t[i].pg_num--;
      } else {
        t.push_back(q.top()); q.pop();
        if(find(out_bucket.begin(), out_bucket.end(), t.back().id) == out_bucket.end()) {
          out_bucket.push_back(t.back().id);
          buckets_to_pgs[t.back().id].insert(pg);
          t.back().pg_num--;
        } else {
          continue;
        }
      }
      i++;
    }
    for(int i = 0; i < t.size(); i++) {
      if(t[i].pg_num > 0) {
        if(i == 0) {
          primary_q.push(t[i]);
        } else {
          q.push(t[i]);
        }
      }
    }
  }
  
  for(auto& b : buckets) {
    while(!q.empty()) q.pop();
    while(!primary_q.empty()) primary_q.pop();
    for(int i = 0; i < b.item.size(); i++) {
      q.push(node(b.item[i], osd_pg_num[b.item[i]]));
      primary_q.push(node(b.item[i], osd_primary_pg_num[b.item[i]]));
    }
    for(auto pg : buckets_to_primary_pgs[b.id]) {
      node n = primary_q.top(); primary_q.pop();      
      if(pgs_to_osd[pg].empty()) {
        pgs_to_osd[pg].push_back(n.id);
      } else {
        pgs_to_osd[pg][0] = n.id;
      }
      osd_to_primary_pgs[n.id].insert(pg);
      n.pg_num--;                      
      if(n.pg_num > 0) {               
        primary_q.push(n);                     
      }
    }

    for(auto pg : buckets_to_pgs[b.id]) {
      node n = q.top(); q.pop();
      if(pgs_to_osd[pg].empty()) {
        pgs_to_osd[pg].push_back(-1);
      }
      pgs_to_osd[pg].push_back(n.id);
      osd_to_pgs[n.id].insert(pg);
      n.pg_num--;
      if(n.pg_num > 0) {
        q.push(n);
      }
    }
  }
}

void crush_map::dump_map() {
  for (auto b : buckets) {
    cout << b.id << "(" << b.weight << ", " << b.pg_num << ", "<< b.primary_pg << ")"<< endl;
    cout << "\t";
    for(auto& i : b.item) {
      cout << i << "(" << osd_weight[i] << ", " << osd_pg_num[i] << ", " << osd_primary_pg_num[i] << ")" << " ";
    }
    cout << endl;
  }
}

void crush_map::dump_result(int pg_nums) {
  for(int i = 0; i < pg_nums; i++) {
    cout << i << ": ";
    for(int j = 0; j < pgs_to_osd[i].size(); j++) {
      cout << pgs_to_osd[i][j] << " ";
    }
    cout << endl;
  }
}

void crush_map::tmp(map<int, set<int>>& change_map, bool is_primary) {
  if(change_map.empty()) return;
  int start = change_map.begin()->first;

  while(true) {
    auto over_it = change_map.lower_bound(start);
    auto under_it = change_map.rbegin();
    bool remapped = false;
    
    if(change_map.empty() || over_it->first >= 0) {
      break;
    }

    while(!remapped &&under_it != change_map.rend() && under_it->first > 0) {
      auto over_osd_it = over_it->second.begin();
      auto under_osd_it = under_it->second.begin();
      bool all_collide = true;
      while(all_collide && under_osd_it != under_it->second.end()) {
        int over_osd = *over_osd_it;                                                                  
        int under_osd = *under_osd_it;
        int over_num = over_it->first;
        int under_num = under_it->first;

        if(is_primary) {
          vector<int> to_remove;
          for(int pg : osd_to_primary_pgs[over_osd]) {
            bool collide = false;
            for(int osd : pgs_to_osd[pg]) {
              if(osd_bucket[osd] == osd_bucket[under_osd] && osd_bucket[osd] != osd_bucket[over_osd]) {
                collide = true;
                break;
              }
            }
            if(!collide) {
              to_remove.push_back(pg);
              osd_to_primary_pgs[under_osd].insert(pg);
              buckets_to_primary_pgs[osd_bucket[over_osd]].erase(pg);
              buckets_to_primary_pgs[osd_bucket[under_osd]].insert(pg);
              pgs_to_osd[pg][0] = under_osd;
              over_num++;    
              under_num--;
              all_collide = false;
              if(!over_num || !under_num) {
                break;
              }
            }
          }
          for(auto pg : to_remove) {
            osd_to_primary_pgs[over_osd].erase(pg);
          }
        } else {
          vector<int> to_remove;
          for(int pg : osd_to_pgs[over_osd]) {
            bool collide = false;
            for(int osd : pgs_to_osd[pg]) {
              if(osd_bucket[osd] == osd_bucket[under_osd] && osd_bucket[osd] != osd_bucket[over_osd]) {
                collide = true;
                break;
              }
            }
            if(!collide) {
              to_remove.push_back(pg);
              osd_to_pgs[under_osd].insert(pg);
              buckets_to_pgs[osd_bucket[over_osd]].erase(pg);
              buckets_to_pgs[osd_bucket[under_osd]].insert(pg);
              for(int i = 0; i < pgs_to_osd[pg].size(); i++) {
                if(pgs_to_osd[pg][i] == over_osd) {
                  pgs_to_osd[pg][i] = under_osd;
                  break;
                }
              }
              over_num++;
              under_num--;
              all_collide = false;
              if(!over_num || !under_num) {
                break;
              }
            }
          }
          for(auto pg : to_remove) {
            osd_to_pgs[over_osd].erase(pg);
          }
        }
        if(!all_collide) {
          over_it->second.erase(over_osd_it);
          under_it->second.erase(under_osd_it);
          if(over_num || under_num) {
            if(over_num) {
              change_map[over_num].insert(over_osd);
            }
            if(under_num) {
              change_map[under_num].insert(under_osd);
            }
          }
          if(over_it->second.empty()) {
            change_map.erase(over_it->first);
          }
          if(under_it->second.empty()) {
            change_map.erase(under_it->first);
          }
          remapped = true;
        } else {
          under_osd_it++;
        }
      }
      if(remapped == false) {
        under_it++;
      }
    }
    if(remapped == false) {
      start++;
    }
  }

  if(!change_map.empty() && change_map.begin()->first < 0) { //collide
    int swap = 0;
    int max_osd = osd_weight.size();
    while(true) {
      auto over_it = change_map.begin();
      auto under_it = change_map.rbegin();
      bool remapped = false;
      if(change_map.empty() || over_it->first >= 0) {
        break;
      }
      while(!remapped &&under_it != change_map.rend() && under_it->first > 0) {
        auto over_osd_it = over_it->second.begin();
        auto under_osd_it = under_it->second.begin();
        bool mapped = false;
        while(!mapped && under_osd_it != under_it->second.end()) {
          int over_osd = *over_osd_it;
          int under_osd = *under_osd_it;
          int over_num = over_it->first;
          int under_num = under_it->first;
          if(is_primary) {
            vector<int> to_remove;
            for(int pg : osd_to_primary_pgs[over_osd]) {
              int tmp_pg = -1;
              do {
                do {
                  swap = (swap+1)%max_osd;
                } while(osd_primary_pg_num[swap]-osd_to_primary_pgs[swap].size() > 0 || swap == over_osd);
                for(int swap_pg : osd_to_primary_pgs[swap]) {
                  if(!check_collide(pg, over_osd, swap) && !check_collide(swap_pg, swap, under_osd)) {
                    tmp_pg = swap_pg;
                    break;
                  }
                }
              } while (tmp_pg == -1);
              pgs_to_osd[pg][0] = swap;
              pgs_to_osd[tmp_pg][0] = under_osd;
              to_remove.push_back(pg);
              osd_to_primary_pgs[swap].erase(tmp_pg);
              osd_to_primary_pgs[swap].insert(pg);
              buckets_to_primary_pgs[osd_bucket[over_osd]].erase(pg);
              buckets_to_primary_pgs[osd_bucket[swap]].erase(tmp_pg);
              buckets_to_primary_pgs[osd_bucket[swap]].insert(pg);
              buckets_to_primary_pgs[osd_bucket[under_osd]].insert(tmp_pg);
              //cout << pg << ":" << over_osd << "->" << swap << endl;
              //cout << tmp_pg << ":" << swap << "->" << under_osd << endl; 
              over_num++;    
              under_num--;
              mapped = true;
              if(!over_num || !under_num) {                           
                break;
              }
            }
            for(auto pg : to_remove) {
              osd_to_primary_pgs[over_osd].erase(pg);
            }
          } else {
            vector<int> to_remove;
            for(int pg : osd_to_pgs[over_osd]) {
              int tmp_pg = -1;
              do {
                do {
                  swap = (swap+1)%max_osd;
                } while(osd_pg_num[swap]-osd_to_pgs[swap].size() > 0 || swap == over_osd);
                for(int swap_pg : osd_to_pgs[swap]) {
                  if(!check_collide(pg, over_osd, swap) && !check_collide(swap_pg, swap, under_osd)) {
                    tmp_pg = swap_pg;
                    break;
                  }
                }
              } while (tmp_pg == -1);
              pgs_to_osd[pg][0] = swap;
              pgs_to_osd[tmp_pg][0] = under_osd;
              to_remove.push_back(pg);
              osd_to_pgs[swap].erase(tmp_pg);
              osd_to_pgs[swap].insert(pg);
              buckets_to_pgs[osd_bucket[over_osd]].erase(pg);
              buckets_to_pgs[osd_bucket[swap]].erase(tmp_pg);
              buckets_to_pgs[osd_bucket[swap]].insert(pg);
              buckets_to_pgs[osd_bucket[under_osd]].insert(tmp_pg);
              //cout << pg << ":" << over_osd << "->" << swap << endl;
              //cout << tmp_pg << ":" << swap << "->" << under_osd << endl;
              over_num++;    
              under_num--;
              mapped = true;
              if(!over_num || !under_num) {                           
                break;
              }
            }
            for(auto pg : to_remove) {
              osd_to_pgs[over_osd].erase(pg);
            }
          }
          if(mapped) {
            over_it->second.erase(over_osd_it);
            under_it->second.erase(under_osd_it);
            if(over_num || under_num) {
              if(over_num) {
                change_map[over_num].insert(over_osd);
              }
              if(under_num) {
                change_map[under_num].insert(under_osd);
              }
            }
            if(over_it->second.empty()) {
              change_map.erase(over_it->first);
            }
            if(under_it->second.empty()) {
              change_map.erase(under_it->first);
            }
            remapped = true;
          } else {
            under_osd_it++;
          }
        }
        if(remapped == false) {
          under_it++;
        }
      }
    }
  }
}

int crush_map::underfull_remap(map<int, set<int>>& change_map,
                                bool is_primary) {
  int remap_count = 0;
  int balance_osd = 0;
  int max_osd = osd_weight.size();
  while(!change_map.empty() && change_map.rbegin()->first > 1) {
    auto under_it = change_map.rbegin();
    auto under_osd_it = under_it->second.begin();
    int under_osd = *under_osd_it;
    int under_num = under_it->first;

    under_it->second.erase(under_osd_it);
    if(under_it->second.empty()) {
      change_map.erase(under_it->first);
    }
    
    int balance_pg = -1;
    if(is_primary) {
      do {
        do {
          balance_osd = (balance_osd+1)%max_osd;
        } while(osd_to_primary_pgs[balance_osd].size() == 0 ||
                osd_primary_pg_num[balance_osd] > osd_to_primary_pgs[balance_osd].size());
        for(int pg : osd_to_primary_pgs[balance_osd]) {
          if(!check_collide(pg, balance_osd, under_osd)) {
            balance_pg = pg;
            break;
          }
        }
      } while(balance_pg == -1);
      osd_to_primary_pgs[balance_osd].erase(balance_pg);
      osd_to_primary_pgs[under_osd].insert(balance_pg);
      buckets_to_primary_pgs[osd_bucket[balance_osd]].erase(balance_pg);
      buckets_to_primary_pgs[osd_bucket[under_osd]].insert(balance_pg);
      pgs_to_osd[balance_pg][0] = under_osd;
      //cout << balance_pg << ":" << balance_osd << "->" << under_osd << endl;
      under_num--;
      remap_count++;
    } else {
      do {
        do {
          balance_osd = (balance_osd+1)%max_osd;
        } while(osd_to_pgs[balance_osd].size() == 0 ||
                osd_pg_num[balance_osd] > osd_to_pgs[balance_osd].size());
        for(int pg : osd_to_pgs[balance_osd]) {
          if(!check_collide(pg, balance_osd, under_osd)) {
            balance_pg = pg;
            break;
          }
        }
      } while(balance_pg == -1);
      osd_to_pgs[balance_osd].erase(balance_pg);
      osd_to_pgs[under_osd].insert(balance_pg);
      buckets_to_pgs[osd_bucket[balance_osd]].erase(balance_pg);
      buckets_to_pgs[osd_bucket[under_osd]].insert(balance_pg);
      for(int i = 0; i < pgs_to_osd[balance_pg].size(); i++) {
        if(pgs_to_osd[balance_pg][i] == balance_osd) {
          pgs_to_osd[balance_pg][i] = under_osd;
          break;
        }
      }
      //cout << balance_pg << ":" << balance_osd << "->" << under_osd << endl;
      under_num--;
      remap_count++;
    }

    if(under_num) {
      change_map[under_num].insert(under_osd);
    }
  }
  return remap_count;
}

int crush_map::preprocess_map_change(map_change& change) {
  for(auto crush_remove_item : change.osd_crush_remove) {
    int osd_bucket = crush_remove_item.first;
    int osd = crush_remove_item.second;
    for(auto item : change.osd_weight_change)  {
      if(item.second.first == osd) {
        return -1;
      }
    }
    change.osd_weight_change.push_back({osd_bucket, {osd, 0}});

    //clear the osd info in out_osd_pg_maps && out_osd_weight
    out_osd_pg_maps.erase(osd);
    out_osd_weight.erase(osd);
  }

  for(auto crush_add_item : change.osd_crush_add) {
    int osd_bucket = crush_add_item.first;
    int osd = crush_add_item.second.first;
    int weight = crush_add_item.second.second;
    for(auto item : change.osd_weight_change)  {
      if(item.second.first == osd) {
        return -1;
      }
    }
    change.osd_weight_change.push_back({osd_bucket, {osd, weight}});
  }

  for(auto osdmap_remove_item : change.osd_osdmap_remove) {
    int osd_bucket = osdmap_remove_item.first;
    int osd = osdmap_remove_item.second;
    for(auto item : change.osd_weight_change)  {
      if(item.second.first == osd) {
        return -1;
      }
    }
    change.osd_weight_change.push_back({osd_bucket, {osd, 0}});
    out_osd_weight[osd] = osd_weight[osd];

    //store the pg info of out_osd into out_osd_pg_maps
    for(int pg : osd_to_primary_pgs[osd]) {
      out_osd_pg_maps[osd].insert({pg, 0});
    }
    for(int pg : osd_to_pgs[osd]) {
      int i = 1;
      for(; i < change.pool_size; i++) {
        if(pgs_to_osd[pg][i] == osd) {
          break;
        }
      }
      assert(i < change.pool_size);
      out_osd_pg_maps[osd].insert({pg, i});
    }
  }

  for(auto osdmap_add_item : change.osd_osdmap_add) {
    int osd_bucket = osdmap_add_item.first;
    int osd = osdmap_add_item.second;
    for(auto item : change.osd_weight_change)  {
      if(item.second.first == osd) {
        return -1;
      }
    }
    change.osd_weight_change.push_back({osd_bucket, {osd, out_osd_weight[osd]}});

    //remap the pgs back to in osd
    for(auto pg_info : out_osd_pg_maps[osd]) {
      int pg = pg_info.first;
      int idx = pg_info.second;
      int from = pgs_to_osd[pg][idx];
      pgs_to_osd[pg][idx] = osd;
      if(idx == 0) { //primary
        osd_to_primary_pgs[osd].insert(pg);
        buckets_to_primary_pgs[this->osd_bucket[osd]].insert(pg);
        osd_to_primary_pgs[from].erase(pg);
        buckets_to_primary_pgs[this->osd_bucket[from]].erase(pg);
      } else { //duplicate
        osd_to_pgs[osd].insert(pg);
        buckets_to_pgs[this->osd_bucket[osd]].insert(pg);
        osd_to_pgs[from].erase(pg);
        buckets_to_pgs[this->osd_bucket[from]].erase(pg);
      }
    }

    //clear the osd info in out_osd_pg_maps && out_osd_weight
    out_osd_pg_maps.erase(osd);
    out_osd_weight.erase(osd);
  }

  return 0;
}

void crush_map::apply_map_change(map_change& change) {
  map<int, set<int>> duplicate_change; //change_num->item
  map<int, set<int>> primary_change; //change_num->item;

  preprocess_map_change(change);
  //reweight
  for(auto p : change.osd_weight_change) {
    int osd_bucket = p.first;
    int osd = p.second.first;
    int weight = p.second.second;
    if(osd >= osd_weight.size()) {
      osd_weight.push_back(0);
      osd_pg_num.push_back(0);
      osd_primary_pg_num.push_back(0);
      this->osd_bucket.push_back(osd_bucket);
      buckets[-1-osd_bucket].item.push_back(osd_id++);

    }

    buckets[-1-osd_bucket].weight += (weight - osd_weight[osd]);
    root_weight += (weight-osd_weight[osd]);
    osd_weight[osd] = weight;
  }
  
  adjust_crushmap_pg_target(change.pool_size, change.pool_pg_num,
                            primary_change, duplicate_change);

  //adjust pg mappings
  //overfull_remap_to_underfull(primary_change, true);
  //overfull_remap_to_underfull(duplicate_change, false);
  
  /*
  cout << "primary_change" << ":" << endl;
  for(auto item : primary_change) {
    cout << item.first << "->" << endl;
    for(auto osd : item.second) {
      cout << osd << " ";
    }
    cout << endl;
  }
  cout << "duplicate_change" << ":" << endl;
  for(auto item : duplicate_change) {
    cout << item.first << "->" << endl;
    for(auto osd : item.second) {
      cout << osd << " ";
    }
    cout << endl;
  }
  */
  tmp(primary_change, true);
  tmp(duplicate_change, false);
  underfull_remap(primary_change, true);
  underfull_remap(duplicate_change, false);
  
}
