#include "pool_bucket.h"

struct node{
  int id;
  int pg_num;
  node(int id, int num) : id(id), pg_num(num) {}
  bool operator < (const node& a) const {
    return pg_num < a.pg_num; //大顶堆
  }
};

void pool_bucket::gen_map(vector<int>& weight) {
  root_weight = 0;
  int bucket_id = 0;
  for(int w : weight) {
    root_weight += w;
    bucket b(bucket_id++);
    b.weight = w;
    buckets_map[b.id] = b;
  }
}

void pool_bucket::init_crushmap_pg_target() {
  correct_bucket_weight();
  int pool_primary_num = pool_pg_num;
  int pool_duplicate_num = (pool_size-1) * pool_pg_num;
  float primary_per_weight = 1.0 * pool_primary_num / (root_weight - total_correct_weight);
  float duplicate_per_weight = 1.0 * pool_duplicate_num / (root_weight - total_correct_weight);                 
  for (auto& b : buckets_map) {
    b.second.primary_pg_num = ceil(b.second.correct_weight * primary_per_weight);
    b.second.duplicate_pg_num = ceil(b.second.correct_weight * duplicate_per_weight);
  }
}

void pool_bucket::adjust_crushmap_pg_target(map<int, set<int>>& primary_change,
                                            map<int, set<int>>& duplicate_change) {
  correct_bucket_weight();
  int pool_primary_num = pool_pg_num;
  int pool_duplicate_num = (pool_size-1) * pool_pg_num;
  float primary_per_weight = 1.0 * pool_primary_num / (root_weight - total_correct_weight);
  float duplicate_per_weight = 1.0 * pool_duplicate_num / (root_weight - total_correct_weight);
  for (auto& b : buckets_map) {
    b.second.primary_pg_num = ceil(b.second.correct_weight * primary_per_weight);
    b.second.duplicate_pg_num = ceil(b.second.correct_weight * duplicate_per_weight);

    if (b.second.primary_pg_num != b.second.primary_pg.size()) {
      primary_change[b.second.primary_pg_num - b.second.primary_pg.size()].insert(b.second.id);
    }
    if (b.second.duplicate_pg_num != b.second.duplicate_pg.size()) {
      duplicate_change[b.second.duplicate_pg_num - b.second.duplicate_pg.size()].insert(b.second.id);
    }
  }
}

void pool_bucket::init_pg_mappings() {
  priority_queue<node> duplicate_q;
  priority_queue<node> primary_q;
  for (auto& b : buckets_map) {
    duplicate_q.push(node(b.first, b.second.duplicate_pg_num));
    primary_q.push(node(b.first, b.second.primary_pg_num));
  }
  for (unsigned pg = 0; pg < pool_pg_num; pg++) { //choose bucket

    vector<int> out_bucket;
    vector<node> t;
    for (int i = 0; i < pool_size && !duplicate_q.empty();) {
      pgs_to_bucket[pg].reserve(pool_size);
      if (i == 0) { //primary
        t.push_back(primary_q.top()); primary_q.pop();
        out_bucket.push_back(t[i].id);
        buckets_map[t[i].id].primary_pg.insert(pg);

        if (pgs_to_bucket[pg].empty()) {
          pgs_to_bucket[pg].push_back(t[i].id);
        } else {
          pgs_to_bucket[pg][0] = t[i].id;
        }
        t[i].pg_num--;
      } else {
        t.push_back(duplicate_q.top()); duplicate_q.pop();
        if (find(out_bucket.begin(), out_bucket.end(), t.back().id) == out_bucket.end()) {
          out_bucket.push_back(t.back().id);
          buckets_map[t.back().id].duplicate_pg.insert(pg);

          if (pgs_to_bucket[pg].empty()) {
            pgs_to_bucket[pg].push_back(invalid_id);
          }
          pgs_to_bucket[pg].push_back(t.back().id);
          t.back().pg_num--;
        } else {
          continue;
        }
      }
      i++;
    }
    for (int i = 0; i < t.size(); i++) {
      if (t[i].pg_num > 0) {
        if (i == 0) {
          primary_q.push(t[i]);
        } else {
          duplicate_q.push(t[i]);
        }
      }
    }
    if (pgs_to_bucket[pg].size() < pool_size && pgs_to_bucket[pg].size() > 0) {  //primary must mapped success
      assert(!duplicate_q.empty());
      for (auto&b : buckets_map) {
        if(b.second.id != duplicate_q.top().id && !check_collide(pg, duplicate_q.top().id, b.second.id)) {
          for (auto tmp_pg : b.second.duplicate_pg) {
            if(!check_collide(tmp_pg, b.second.id, duplicate_q.top().id)) {
              //cout << __func__ << " pg:" << pg << " can remap to :" << b.second.id << " by pg:" << tmp_pg << endl;
              node n = duplicate_q.top(); duplicate_q.pop();
              pgs_to_bucket[pg].push_back(b.second.id);
              buckets_map[b.second.id].duplicate_pg.erase(tmp_pg);
              buckets_map[b.second.id].duplicate_pg.insert(pg);
              buckets_map[n.id].duplicate_pg.insert(tmp_pg);
              for (int i = 1; i < pool_size; i++) {
                if(pgs_to_bucket[tmp_pg][i] == b.second.id) {
                  pgs_to_bucket[tmp_pg][i] = n.id;
                  break;
                }
              }

              n.pg_num--;
              if(n.pg_num) {
                duplicate_q.push(n);
              }
              break;
            }
          }
        }
        if (pgs_to_bucket[pg].size() == pool_size && duplicate_q.empty()) {
          break;
        }
      }
    }
  }
}

void pool_bucket::dump_map() {
  for (auto b = buckets_map.begin(); b != buckets_map.end(); b++) {
    cout << b->first << "(" << b->second.weight << ", " << b->second.primary_pg_num << ", "<< b->second.duplicate_pg_num << ")"<< endl;
  }
}

bool pool_bucket::check_map() {
  return true;
}

bool pool_bucket::check_mapping_result() {
  return true;
}

void pool_bucket::dump_result() {
  for (int i = 0; i < pool_pg_num; i++) {
    cout << i << ": ";
    for (int j = 0; j < pgs_to_bucket[i].size(); j++) {
      cout << pgs_to_bucket[i][j] << " ";
    }
    cout << endl;
  }
}

int pool_bucket::overfull_remap_step1(map<int, set<int>>& change_map, bool is_primary) {
  int remap_count = 0;
  if (change_map.empty()) return remap_count;
  int start = change_map.begin()->first;

  while (true) {
    auto over_it = change_map.lower_bound(start);
    auto under_it = change_map.rbegin();
    bool remapped = false;
    
    if (change_map.empty() || over_it->first >= 0) {
      break;
    }

    while (!remapped &&under_it != change_map.rend() && under_it->first > 0) {
      auto over_bucket_it = over_it->second.begin();
      auto under_bucket_it = under_it->second.begin();
      bool all_collide = true;
      while (all_collide && under_bucket_it != under_it->second.end()) {
        int over_bucket = *over_bucket_it;                                                                  
        int under_bucket = *under_bucket_it;
        int over_num = over_it->first;
        int under_num = under_it->first;
        bucket& over_bucket_info = buckets_map[over_bucket];
        bucket& under_bucket_info = buckets_map[under_bucket];

        if (is_primary) {
          vector<int> to_remove;
          for (int pg : over_bucket_info.primary_pg) {
            bool collide = false;
            for (int osd_bucket : pgs_to_bucket[pg]) {
              if (osd_bucket == under_bucket_info.id) {
                collide = true;  //same bucket
                break;
              }
            }
            if (!collide) {
              to_remove.push_back(pg);
              under_bucket_info.primary_pg.insert(pg);
              //insert must after erase as the over_bucket and under_bucket maybe in same bucket
              pgs_to_bucket[pg][0] = under_bucket;
              over_num++;    
              under_num--;
              remap_count++;
              all_collide = false;
//              if (!over_num || !under_num) {
                break;
//              }
            }
          }
          for (auto pg : to_remove) {
            over_bucket_info.primary_pg.erase(pg);
          }
        } else {
          vector<int> to_remove;
          for (int pg : over_bucket_info.duplicate_pg) {
            bool collide = false;
            for (int osd_bucket : pgs_to_bucket[pg]) {
              if (buckets_map[osd_bucket].id == under_bucket_info.id) {
                collide = true;
                break;
              }
            }
            if(!collide) {
              to_remove.push_back(pg);
              under_bucket_info.duplicate_pg.insert(pg);
              //insert must after erase as the over_bucket and under_bucket maybe in same bucket
              for (int i = 0; i < pgs_to_bucket[pg].size(); i++) {
                if (pgs_to_bucket[pg][i] == over_bucket) {
                  pgs_to_bucket[pg][i] = under_bucket;
                  break;
                }
              }
              over_num++;
              under_num--;
              remap_count++;
              all_collide = false;
//              if (!over_num || !under_num) {
                break;
//              }
            }
          }
          for (auto pg : to_remove) {
            over_bucket_info.duplicate_pg.erase(pg);
          }
        }
        if (!all_collide) {
          over_it->second.erase(over_bucket_it);
          under_it->second.erase(under_bucket_it);
          if (over_num || under_num) {
            if (over_num) {
              change_map[over_num].insert(over_bucket);
            }
            if (under_num) {
              change_map[under_num].insert(under_bucket);
            }
          }
          if (over_it->second.empty()) {
            change_map.erase(over_it->first);
          }
          if (under_it->second.empty()) {
            change_map.erase(under_it->first);
          }
          remapped = true;
        } else {
          under_bucket_it++;
        }
      }
      if (remapped == false) {
        under_it++;
      }
    }
    if (remapped == false) {
      start++;
    }
  }
  return remap_count;
}

int pool_bucket::overfull_remap_step2(map<int, set<int>>& change_map, bool is_primary) {
  int remap_count = 0;                                                                                     
  if (!change_map.empty() && change_map.begin()->first < 0) { //collide
    int swap = 0;
    int max_bucket = buckets_map.rbegin()->first + 1;
    int start = change_map.begin()->first;
    while (!change_map.empty()) {
      auto over_it = change_map.lower_bound(start);
      auto under_it = change_map.rbegin();
      bool remapped = false;
      if (over_it == change_map.end() || over_it->first >= 0) {
        break;
      }
      while (!remapped &&under_it != change_map.rend() && under_it->first > 0) {
        auto over_bucket_it = over_it->second.begin();
        auto under_bucket_it = under_it->second.begin();

        bool mapped = false;
        while (!mapped && under_bucket_it != under_it->second.end()) {
          int over_bucket = *over_bucket_it;
          int under_bucket = *under_bucket_it;
          int over_num = over_it->first;
          int under_num = under_it->first;
          bucket& over_bucket_info = buckets_map[over_bucket];
          bucket& under_bucket_info = buckets_map[under_bucket];
          if (is_primary) {
            vector<int> to_remove;
            for (int pg : over_bucket_info.primary_pg) {
              int tmp_pg = invalid_id;
              int find_tmp_pg_retry = 0;
              do {
                int find_swap_retry = 0;
                do {
                  swap = (swap+1)%max_bucket;
                  find_swap_retry++;
                } while (find_swap_retry <= max_bucket &&
                         (!buckets_map.count(swap)
                            || buckets_map[swap].primary_pg_num-buckets_map[swap].primary_pg.size() > 0
                            || swap == over_bucket));
                if(find_swap_retry > max_bucket) {
                  break;
                }
                for (int swap_pg : buckets_map[swap].primary_pg) {
                  if (!check_collide(pg, over_bucket, swap) && !check_collide(swap_pg, swap, under_bucket)) {
                    tmp_pg = swap_pg;
                    break;
                  }
                }
                find_tmp_pg_retry++;
              } while (tmp_pg == invalid_id && find_tmp_pg_retry <= max_bucket);
              if(tmp_pg == invalid_id) { //find swap fail
                to_remove.push_back(pg);
                pgs_to_bucket[pg][0] = invalid_id;
                continue;
              }
              bucket& swap_info = buckets_map[swap];
              pgs_to_bucket[pg][0] = swap;
              pgs_to_bucket[tmp_pg][0] = under_bucket;
              to_remove.push_back(pg);
              swap_info.primary_pg.erase(tmp_pg);
              swap_info.primary_pg.insert(pg);
              under_bucket_info.primary_pg.insert(tmp_pg);
              //insert must after erase as the over_bucket and swap_osd maybe in same bucket

              over_num++;    
              under_num--;
              remap_count += 2;
              mapped = true;
//              if (!over_num || !under_num) {                           
                break;
//              }
            }
            for (auto pg : to_remove) {
              over_bucket_info.primary_pg.erase(pg);
            }
          } else {
            vector<int> to_remove;
            for (int pg : over_bucket_info.duplicate_pg) {
              int tmp_pg = invalid_id;
              int find_tmp_pg_retry = 0;
              do {
                int find_swap_retry = 0;
                do {
                  swap = (swap+1)%max_bucket;
                  find_swap_retry++;
                } while(find_swap_retry <= max_bucket &&
                        (!buckets_map.count(swap)
                          || buckets_map[swap].duplicate_pg_num-buckets_map[swap].duplicate_pg.size() > 0
                          || swap == over_bucket));
                if(find_swap_retry > max_bucket) {
                  break;
                }
                for (int swap_pg : buckets_map[swap].duplicate_pg) {
                  if (!check_collide(pg, over_bucket, swap) && !check_collide(swap_pg, swap, under_bucket)) {
                    tmp_pg = swap_pg;
                    break;
                  }
                }
              } while (tmp_pg == invalid_id && find_tmp_pg_retry <= max_bucket);
              if(tmp_pg == invalid_id) { //find swap fail
                to_remove.push_back(pg);
                int pos;
                for (pos = 0; pos < pgs_to_bucket[tmp_pg].size(); pos++) {
                  if (pgs_to_bucket[pg][pos] == over_bucket) {
                    break;
                  }
                }
                assert(pos < pgs_to_bucket[tmp_pg].size());
                pgs_to_bucket[pg][pos] = invalid_id;
                continue;
              }

              bucket& swap_info = buckets_map[swap];
              int pos = 0;
              for (pos = 0; pos < pgs_to_bucket[pg].size(); pos++) {
                if (pgs_to_bucket[pg][pos] == over_bucket) {
                  break;
                }
              }
              assert(pos < pgs_to_bucket[tmp_pg].size());
              pgs_to_bucket[pg][pos] = swap;
              for (pos = 0; pos < pgs_to_bucket[tmp_pg].size(); pos++) {
                if (pgs_to_bucket[tmp_pg][pos] == swap) {
                  break;
                }
              }
              assert(pos < pgs_to_bucket[tmp_pg].size());
              pgs_to_bucket[tmp_pg][pos] = under_bucket;

              to_remove.push_back(pg);
              swap_info.duplicate_pg.insert(pg);
              swap_info.duplicate_pg.erase(tmp_pg);
              under_bucket_info.duplicate_pg.insert(tmp_pg);
              //insert must after erase as the over_bucket and swap_osd maybe in same bucket
  
              over_num++;
              under_num--;
              remap_count += 2;
              mapped = true;
//              if (!over_num || !under_num) {                           
                break;
//              }
            }
            for (auto pg : to_remove) {
              over_bucket_info.duplicate_pg.erase(pg);
            }
          }
          if (mapped) {
            over_it->second.erase(over_bucket_it);
            under_it->second.erase(under_bucket_it);
            if (over_num || under_num) {
              if (over_num) {
                change_map[over_num].insert(over_bucket);
              }
              if (under_num) {
                change_map[under_num].insert(under_bucket);
              }
            }
            if (over_it->second.empty()) {
              change_map.erase(over_it->first);
            }
            if (under_it->second.empty()) {
              change_map.erase(under_it->first);
            }
            remapped = true;
          } else {
            under_bucket_it++;
          }
        }
        if (remapped == false) {
          under_it++;
        }
      }
      if (remapped == false) {
        start++;
      }
    }
  }
  return remap_count;
}

int pool_bucket::overfull_remap(map<int, set<int>>& change_map, bool is_primary) {
  int remap_count = 0;
  remap_count += overfull_remap_step1(change_map, is_primary);
  remap_count += overfull_remap_step2(change_map, is_primary); 
  return remap_count;
}

int pool_bucket::underfull_remap(map<int, set<int>>& change_map, bool is_primary) {
  int remap_count = 0;
  if (change_map.empty()) return remap_count;
  int balance_osd = 0;
  int max_bucket = buckets_map.rbegin()->first + 1;
  int start = change_map.rbegin()->first;
  while (!change_map.empty() && change_map.rbegin()->first > 1 && start > 1) {
    auto under_it = change_map.lower_bound(start);
    //auto under_it = change_map.rbegin();
    auto under_bucket_it = under_it->second.begin();
    int under_bucket = *under_bucket_it;
    int under_num = under_it->first;
    bucket& under_bucket_info = buckets_map[under_bucket];

    int balance_pg = invalid_id;
    if (is_primary) {
      int find_pg_retry = 0;
      do {
        int find_osd_retry = 0;
        do {
          balance_osd = (balance_osd+1)%max_bucket;
          find_osd_retry++;
        } while(find_osd_retry <= max_bucket 
                && (!buckets_map.count(balance_osd) 
                      || buckets_map[balance_osd].primary_pg.size() == 0 
                      || buckets_map[balance_osd].primary_pg_num > buckets_map[balance_osd].primary_pg.size()));
        if(find_osd_retry > max_bucket) {
          break;
        }
        for (int pg : buckets_map[balance_osd].primary_pg) {
          if (!check_collide(pg, balance_osd, under_bucket)) {
            balance_pg = pg;
            break;
          }
        }
        find_pg_retry++;
      } while (balance_pg == invalid_id && find_pg_retry <= max_bucket);
      if (balance_pg == invalid_id) {
        start--;   
        continue;
      }
      bucket& balance_bucket = buckets_map[balance_osd];
      balance_bucket.primary_pg.erase(balance_pg);
      under_bucket_info.primary_pg.insert(balance_pg);
      //insert must after erase as the under_bucket and balance_osd maybe in same bucket
      pgs_to_bucket[balance_pg][0] = under_bucket;
      under_num--;
      remap_count++;
    } else {
      int find_pg_retry = 0;
      do {
        int find_osd_retry = 0;
        do {
          balance_osd = (balance_osd+1)%max_bucket;
          find_osd_retry++;
        } while (find_osd_retry <= max_bucket && (!buckets_map.count(balance_osd) || buckets_map[balance_osd].duplicate_pg.size() == 0 ||
                buckets_map[balance_osd].duplicate_pg_num > buckets_map[balance_osd].duplicate_pg.size()));
        if (find_osd_retry > max_bucket) {
          break;
        }
        for (int pg : buckets_map[balance_osd].duplicate_pg) {
          if (!check_collide(pg, balance_osd, under_bucket)) {
            balance_pg = pg;
            break;
          }
        }
        find_pg_retry++;
      } while (balance_pg == invalid_id && find_pg_retry <= max_bucket);
      if (balance_pg == invalid_id) {
        start--;
        continue;
      }

      bucket& balance_bucket = buckets_map[balance_osd];
      balance_bucket.duplicate_pg.erase(balance_pg);
      under_bucket_info.duplicate_pg.insert(balance_pg);
      //insert must after erase as the under_bucket and balance_osd maybe in same bucket
      for (int i = 0; i < pgs_to_bucket[balance_pg].size(); i++) {
        if (pgs_to_bucket[balance_pg][i] == balance_osd) {
          pgs_to_bucket[balance_pg][i] = under_bucket;
          break;
        }
      }
      under_num--;
      remap_count++;
    }

    under_it->second.erase(under_bucket_it);
    if (under_it->second.empty()) {
      change_map.erase(under_it->first);
    }

    if(under_num) {
      change_map[under_num].insert(under_bucket);
    }
  }
  return remap_count;
}

void print_debug(map<int, set<int>>& change, const string& str) {
  cout << str << endl;
  for(auto item : change) {
    cout << item.first << "->" << endl;
    for(auto osd : item.second) {
      cout << osd << " ";
    }
    cout << endl;
  }
}

int pool_bucket::apply_map_change(map_change& change) {
  map<int, set<int>> duplicate_change; //change_num->item
  map<int, set<int>> primary_change; //change_num->item;
  int remap_count = 0;

  int ret = preprocess_map_change(change);
  if (ret < 0) { //error
    return remap_count;
  }
  remap_count += ret;

  adjust_crushmap_pg_target(primary_change, duplicate_change);
  dump_map();
  //print_debug(primary_change, string("primary_change:"));
  //print_debug(duplicate_change, string("duplicate_change:"));

  //adjust pg mappings
  remap_count += overfull_remap(primary_change, true);
  //print_debug(primary_change, string("primary_change:"));
  //print_debug(duplicate_change, string("duplicate_change:"));

  remap_count += overfull_remap(duplicate_change, false);
  //print_debug(primary_change, string("primary_change:"));
  //print_debug(duplicate_change, string("duplicate_change:"));

  //remap_count += underfull_remap(primary_change, true);
  //remap_count += underfull_remap(duplicate_change, false);

  for (auto p : change.crush_remove) {
    buckets_map.erase(p);
  }

  return remap_count;
}

int pool_bucket::preprocess_map_change(map_change& change) {
  int remap_count = 0;
  for (auto osd_bucket : change.crush_remove) {
    if (!buckets_map.count(osd_bucket)) {
      cerr << __func__ << " osd_bucket:" << osd_bucket << " does not in crushmap!" << endl;
      continue;
    }
    for (auto item : change.weight_change)  {
      if (item.first == osd_bucket) {
        cerr << __func__ << " osd_bucket:" << osd_bucket << " change multi times" << endl;
        return -1;
      }
    }
    change.weight_change.push_back({osd_bucket, 0});

    //clear the osd info in out_osd_pg_maps && out_osd_weight
    out_bucket_pg_maps.erase(osd_bucket);
    out_bucket_weight.erase(osd_bucket);
  }

  for (auto add_item : change.crush_add) {
    int osd_bucket = add_item.first;
    int weight = add_item.second;
    if (buckets_map.count(osd_bucket)) {
      cerr << __func__ << " osd_bucket:" << osd_bucket << " areadly in crushmap!" << endl;
      continue;
    }
    for (auto item : change.weight_change)  {
      if (item.first == osd_bucket) {
        cerr << __func__ << " osd_bucket:" << osd_bucket << " change multi times" << endl;
        return -1;
      }
    }
    change.weight_change.push_back({osd_bucket, weight});
  }

  for (auto osd_bucket : change.osdmap_remove) {
    if (!buckets_map.count(osd_bucket)) {
      cerr << __func__ << " osd_bucket:" << osd_bucket << " does not in crushmap!" << endl;
      continue;
    }

    for (auto item : change.weight_change)  {
      if (item.first == osd_bucket) {
        cerr << __func__ << " osd_bucket:" << osd_bucket << " change multi times" << endl;
        return -1;
      }
    }
    change.weight_change.push_back({osd_bucket, 0});
    bucket& b = buckets_map[osd_bucket];
    out_bucket_weight[osd_bucket] = b.weight;

    //store the pg info of out_osd into out_osd_pg_maps
    for (int pg : b.primary_pg) {
      out_bucket_pg_maps[osd_bucket].insert({pg, 0});
    }
    for (int pg : b.duplicate_pg) {
      int i = 1;
      for (; i < pool_size; i++) {
        if(pgs_to_bucket[pg][i] == osd_bucket) {
          break;
        }
      }
      assert(i < pool_size);
      out_bucket_pg_maps[osd_bucket].insert({pg, i});
    }
  }

  for (auto osd_bucket : change.osdmap_add) {
    if (!buckets_map.count(osd_bucket)) {
      cerr << __func__ << " osd_bucket:" << osd_bucket << " does not in crushmap!" << endl;
      continue;
    }
    bucket& b = buckets_map[osd_bucket];
    for (auto item : change.weight_change)  {
      if (item.first == osd_bucket) {
        cerr << __func__ << " osd_bucket:" << osd_bucket << " change multi times" << endl;
        return -1;
      }
    }
    change.weight_change.push_back({osd_bucket, out_bucket_weight[osd_bucket]});

    //remap the pgs back to in osd
    for (auto pg_info : out_bucket_pg_maps[osd_bucket]) {
      int pg = pg_info.first;
      int idx = pg_info.second;
      int from = pgs_to_bucket[pg][idx];
      pgs_to_bucket[pg][idx] = osd_bucket;
      //insert must after erase as the swap osds maybe in same bucket
      if (idx == 0) { //primary
        buckets_map[from].primary_pg.erase(pg);
        b.primary_pg.insert(pg);
      } else { //duplicate
        buckets_map[from].duplicate_pg.erase(pg);
        b.duplicate_pg.insert(pg);
      }
      remap_count++;
    }

    //clear the osd info in out_osd_pg_maps && out_osd_weight
    out_bucket_pg_maps.erase(osd_bucket);
    out_bucket_weight.erase(osd_bucket);
  }

  //reweight
  for (auto p : change.weight_change) {
    int osd_bucket = p.first;
    float weight = p.second;
    if(!buckets_map.count(osd_bucket)) {
      buckets_map[osd_bucket] = bucket(osd_bucket);
    }
    
    bucket& b = buckets_map[osd_bucket];
    root_weight += (weight-b.weight);
    b.weight = weight;
  }

  return remap_count;
}

void pool_bucket::correct_bucket_weight() {
  total_correct_weight = 0;
  int zero_bucket = 0;
  priority_queue<float> q;
  float weight_threshold = root_weight;
  float left_weight = root_weight;
  for (auto b : buckets_map) {
    if (b.second.weight == 0) {
      zero_bucket++;
    } else {
      q.push(b.second.weight);
    }
  }
  
  for (int i = 1; i < pool_size; i++) {
    left_weight -= q.top();
    float tmp = left_weight / (pool_size - i);
    //cout << __func__ << " i:" << i << " tmp:" << tmp << " top:" << q.top() << endl;
    if(q.top() <= tmp) {
      break;
    } else {
      weight_threshold = tmp;
    }
    q.pop();
  }
  //float weight_threshold = (root_weight - max_weight) / (pool_size - 1);
  //cout << __func__ << " weight_threshold:" << weight_threshold << endl;
  for (auto& b : buckets_map) {
    if(b.second.weight > weight_threshold) {
      b.second.correct_weight = weight_threshold;
      total_correct_weight += (b.second.weight - weight_threshold);
    } else {
      b.second.correct_weight = b.second.weight;
    }
  }
}
