#include "pool_pg_map.h"

struct node{
  int id;
  int pg_num;
  node(int id, int num) : id(id), pg_num(num) {}
  bool operator < (const node& a) const {
    return pg_num < a.pg_num; //大顶堆
  }
};

void pool_pg_map::gen_map(int osd_num, int osd_per_host) {
  int osd_id = 0;
  int bucket_id = -1;
  root_weight = 0;
  bucket_info b(bucket_id--);
  osd_info o;
  o.weight = 1;
  
  for (int i = 0; i < osd_num; i++) {
    if (i%osd_per_host == 0 && i > 0) {
      root_weight += b.weight;
      buckets_map[b.id] = b;
      b.id = bucket_id--;
      b.weight = 0;
      b.item.clear();
    }
    o.id = osd_id++;
    o.bucket_id = b.id;
    osds_map[o.id] = o;
    b.item.push_back(o.id);
    b.weight++;
  }
  root_weight += b.weight;
  buckets_map[b.id] = b;
}

void pool_pg_map::init_crushmap_pg_target() {
  int pool_primary_num = pool_pg_num;
  int pool_duplicate_num = (pool_size-1) * pool_pg_num;
  float primary_per_weight = 1.0 * pool_primary_num / root_weight;
  float duplicate_per_weight = 1.0 * pool_duplicate_num / root_weight;
  for (auto& b : buckets_map) {
    b.second.primary_pg_num = ceil(b.second.weight * primary_per_weight);
    b.second.duplicate_pg_num = ceil(b.second.weight * duplicate_per_weight);

    for (int i = 0; i < b.second.item.size(); i++) {
      int item = b.second.item[i];
      osd_info& o = osds_map[item];
      o.primary_pg_num = ceil(o.weight * primary_per_weight);
      o.duplicate_pg_num = ceil(o.weight * duplicate_per_weight);
    }
  }
}


void pool_pg_map::adjust_crushmap_pg_target(map<int, set<int>>& primary_change,                      
                                            map<int, set<int>>& duplicate_change) {
  int pool_primary_num = pool_pg_num;
  int pool_duplicate_num = (pool_size-1) * pool_pg_num;
  float primary_per_weight = 1.0 * pool_primary_num / root_weight;
  float duplicate_per_weight = 1.0 * pool_duplicate_num / root_weight;
  for (auto & b : buckets_map) {
    b.second.primary_pg_num = ceil(b.second.weight * primary_per_weight);
    b.second.duplicate_pg_num = ceil(b.second.weight * duplicate_per_weight);

    for (int i = 0; i < b.second.item.size(); i++) {
      int item = b.second.item[i];
      osd_info& o = osds_map[item];
      o.primary_pg_num = ceil(o.weight * primary_per_weight);
      o.duplicate_pg_num = ceil(o.weight * duplicate_per_weight);
      
      if (o.primary_pg_num != o.primary_pg.size()) {
        primary_change[o.primary_pg_num - o.primary_pg.size()].insert(item);
      }
      if (o.duplicate_pg_num != o.duplicate_pg.size()) {
        duplicate_change[o.duplicate_pg_num - o.duplicate_pg.size()].insert(item);
      }
    }
  }
}

void pool_pg_map::init_pg_mappings() {
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
      if (i == 0) { //primary
        t.push_back(primary_q.top()); primary_q.pop();
        out_bucket.push_back(t[i].id);
        buckets_map[t[i].id].primary_pg.insert(pg);
        t[i].pg_num--;
      } else {
        t.push_back(duplicate_q.top()); duplicate_q.pop();
        if (find(out_bucket.begin(), out_bucket.end(), t.back().id) == out_bucket.end()) {
          out_bucket.push_back(t.back().id);
          buckets_map[t.back().id].duplicate_pg.insert(pg);
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
  }
  
  //choose osd
  for (auto& b : buckets_map) {
    while (!primary_q.empty()) primary_q.pop();
    while (!duplicate_q.empty()) duplicate_q.pop();
    for (int i = 0; i < b.second.item.size(); i++) {
      osd_info& o = osds_map[b.second.item[i]];
      primary_q.push(node(o.id, o.primary_pg_num));
      duplicate_q.push(node(o.id, o.duplicate_pg_num));
    }
    for (auto pg : b.second.primary_pg) {
      node n = primary_q.top(); primary_q.pop();      
      if(pgs_to_osd[pg].empty()) {
        pgs_to_osd[pg].push_back(n.id);
      } else {
        pgs_to_osd[pg][0] = n.id;
      }
      osds_map[n.id].primary_pg.insert(pg);
      n.pg_num--;                      
      if (n.pg_num > 0) {               
        primary_q.push(n);                     
      }
    }

    for (auto pg : b.second.duplicate_pg) {
      node n = duplicate_q.top(); duplicate_q.pop();
      if (pgs_to_osd[pg].empty()) {
        pgs_to_osd[pg].push_back(-1);
      }
      pgs_to_osd[pg].push_back(n.id);
      osds_map[n.id].duplicate_pg.insert(pg);
      n.pg_num--;
      if (n.pg_num > 0) {
        duplicate_q.push(n);
      }
    }
  }
}

void pool_pg_map::dump_map() {
  for (auto b = buckets_map.rbegin(); b != buckets_map.rend(); b++) {
    cout << b->first << "(" << b->second.weight << ", " << b->second.primary_pg_num << ", "<< b->second.duplicate_pg_num << ")"<< endl;
    cout << "\t";
    for (auto& i : b->second.item) {
      osd_info& o = osds_map[i];
      cout << i << "[" << o.bucket_id << "](" << o.weight << ", " << o.primary_pg_num << ", " << o.duplicate_pg_num << ")" << " ";
    }
    cout << endl;
  }
}

bool pool_pg_map::check_map() {
  int total_osd_num = 0;
  int total_weight = 0;
  for (auto& b : buckets_map) {
    total_osd_num += b.second.item.size();
    total_weight += b.second.weight;
    int osd_weight = 0;
    for (int osd : b.second.item) {
      osd_weight += osds_map[osd].weight;
    }
    if (osd_weight != b.second.weight) {
      return false;
    }
  }
  if (total_weight != root_weight) {
    return false;
  }
  if (total_osd_num != osds_map.size()) {
    return false;
  }
  return true;
}

bool pool_pg_map::check_mapping_result() {
  if (!check_map()) {
    cout << __func__ << " check_map fail" << endl;
    return false;
  }
  for (auto& b : buckets_map) {
    int total = 0;
    for (int osd : b.second.item) {
      total += osds_map[osd].primary_pg.size();
      for (int pg : osds_map[osd].primary_pg) {
        if (b.second.primary_pg.find(pg) == b.second.primary_pg.end()) {
          cout << __func__ << " bucket:" << b.first << " check fail in primary with osd:"
               << osd << ", pg:" << pg << endl;
          return false;
        }
      }
    }
    if (total != b.second.primary_pg.size()) {
      cout << __func__ << " bucket:" << b.first << " check fail in primary, bucket primary num:" 
           << b.second.primary_pg.size() << ", osd primary num:" << total << endl;
      return false;
    }
    total = 0;
    for (int osd : b.second.item) {
      total += osds_map[osd].duplicate_pg.size();
      for (int pg :  b.second.duplicate_pg) {
        if (b.second.duplicate_pg.find(pg) ==  b.second.duplicate_pg.end()) {
          cout << __func__ << " bucket:" << b.first << " check fail in duplicate with osd:"
               << osd << ", pg:" << pg << endl;
          return false;
        }
      }
    }
    if (total != b.second.duplicate_pg.size()) {
      cout << __func__ << " bucket:" << b.first << " check fail in duplicate, bucket duplicate num:"
           << b.second.duplicate_pg.size() << ", osd duplicate num:" << total << endl;               
      return false;
    }
  }

  for (int pg = 0; pg < pool_pg_num; pg++) {
    vector<int> osds_bucket;
    for (int i = 0; i < pool_size && i < pgs_to_osd[pg].size(); i++) {
      if (find(osds_bucket.begin(), osds_bucket.end(), osds_map[pgs_to_osd[pg][i]].bucket_id) != osds_bucket.end()) {
        cout << __func__ << " pg:" << pg << " check fail int duplicate " << i << endl;
        return false;
      }
      osds_bucket.push_back(osds_map[pgs_to_osd[pg][i]].bucket_id);
    }
  }
  return true;
}

void pool_pg_map::dump_result() {
  for (int i = 0; i < pool_pg_num; i++) {
    //cout << i << ": ";
    for (int j = 0; j < pgs_to_osd[i].size(); j++) {
      cout << pgs_to_osd[i][j] << " ";
    }
    cout << endl;
  }
}

/*
 * remap the pgs in overfull osds by follow 2 step:
 *  1. remap pgs from overfull osds into underfull osds directly.
 *  2. remap pgs from overfull osds into underfull osds by swap osds.
 * param:
 *  @change_map: 
 *  @is_primary: remap for primary_pg or duplicate_pg.
 *  @return: return the remap item count.
 */
int pool_pg_map::overfull_remap(map<int, set<int>>& change_map, bool is_primary) {
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
      auto over_osd_it = over_it->second.begin();
      auto under_osd_it = under_it->second.begin();
      bool all_collide = true;
      while (all_collide && under_osd_it != under_it->second.end()) {
        int over_osd = *over_osd_it;                                                                  
        int under_osd = *under_osd_it;
        int over_num = over_it->first;
        int under_num = under_it->first;
        osd_info& over_osd_info = osds_map[over_osd];
        osd_info& under_osd_info = osds_map[under_osd];

        if (is_primary) {
          vector<int> to_remove;
          for (int pg : over_osd_info.primary_pg) {
            bool collide = false;
            for (int osd : pgs_to_osd[pg]) {
              if (osds_map[osd].bucket_id == under_osd_info.bucket_id && osds_map[osd].bucket_id != over_osd_info.bucket_id) {
                collide = true;  //same bucket
                break;
              }
            }
            if (!collide) {
              to_remove.push_back(pg);
              under_osd_info.primary_pg.insert(pg);
              //insert must after erase as the over_osd and under_osd maybe in same bucket
              buckets_map[over_osd_info.bucket_id].primary_pg.erase(pg); 
              buckets_map[under_osd_info.bucket_id].primary_pg.insert(pg);
              pgs_to_osd[pg][0] = under_osd;
              over_num++;    
              under_num--;
              remap_count++;
              all_collide = false;
              if (!over_num || !under_num) {
                break;
              }
            }
          }
          for (auto pg : to_remove) {
            over_osd_info.primary_pg.erase(pg);
          }
        } else {
          vector<int> to_remove;
          for (int pg : over_osd_info.duplicate_pg) {
            bool collide = false;
            for (int osd : pgs_to_osd[pg]) {
              if (osds_map[osd].bucket_id == under_osd_info.bucket_id && osds_map[osd].bucket_id != over_osd_info.bucket_id) {
                collide = true;
                break;
              }
            }
            if(!collide) {
              to_remove.push_back(pg);
              under_osd_info.duplicate_pg.insert(pg);
              //insert must after erase as the over_osd and under_osd maybe in same bucket
              buckets_map[over_osd_info.bucket_id].duplicate_pg.erase(pg);
              buckets_map[under_osd_info.bucket_id].duplicate_pg.insert(pg);
              for (int i = 0; i < pgs_to_osd[pg].size(); i++) {
                if (pgs_to_osd[pg][i] == over_osd) {
                  pgs_to_osd[pg][i] = under_osd;
                  break;
                }
              }
              over_num++;
              under_num--;
              remap_count++;
              all_collide = false;
              if (!over_num || !under_num) {
                break;
              }
            }
          }
          for (auto pg : to_remove) {
            over_osd_info.duplicate_pg.erase(pg);
          }
        }
        if (!all_collide) {
          over_it->second.erase(over_osd_it);
          under_it->second.erase(under_osd_it);
          if (over_num || under_num) {
            if (over_num) {
              change_map[over_num].insert(over_osd);
            }
            if (under_num) {
              change_map[under_num].insert(under_osd);
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
          under_osd_it++;
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

  if (!change_map.empty() && change_map.begin()->first < 0) { //collide
    int swap = 0;
    int max_osd = osds_map.rbegin()->first;
    while (true) {
      auto over_it = change_map.begin();
      auto under_it = change_map.rbegin();
      bool remapped = false;
      if (change_map.empty() || over_it->first >= 0) {
        break;
      }
      while (!remapped &&under_it != change_map.rend() && under_it->first > 0) {
        auto over_osd_it = over_it->second.begin();
        auto under_osd_it = under_it->second.begin();

        bool mapped = false;
        while (!mapped && under_osd_it != under_it->second.end()) {
          int over_osd = *over_osd_it;
          int under_osd = *under_osd_it;
          int over_num = over_it->first;
          int under_num = under_it->first;
          osd_info& over_osd_info = osds_map[over_osd];
          osd_info& under_osd_info = osds_map[under_osd];
          if (is_primary) {
            vector<int> to_remove;
            for (int pg : over_osd_info.primary_pg) {
              int tmp_pg = -1;
              do {
                do {
                  swap = (swap+1)%max_osd;
                } while (!osds_map.count(swap) || osds_map[swap].primary_pg_num-osds_map[swap].primary_pg.size() > 0 || swap == over_osd);
                for (int swap_pg : osds_map[swap].primary_pg) {
                  if (!check_collide(pg, over_osd, swap) && !check_collide(swap_pg, swap, under_osd)) {
                    tmp_pg = swap_pg;
                    break;
                  }
                }
              } while (tmp_pg == -1);
              osd_info& swap_info = osds_map[swap];
              pgs_to_osd[pg][0] = swap;
              pgs_to_osd[tmp_pg][0] = under_osd;
              to_remove.push_back(pg);
              swap_info.primary_pg.erase(tmp_pg);
              swap_info.primary_pg.insert(pg);
              under_osd_info.primary_pg.insert(tmp_pg);
              //insert must after erase as the over_osd and swap_osd maybe in same bucket
              buckets_map[over_osd_info.bucket_id].primary_pg.erase(pg);
              buckets_map[swap_info.bucket_id].primary_pg.erase(tmp_pg);
              buckets_map[swap_info.bucket_id].primary_pg.insert(pg);
              buckets_map[under_osd_info.bucket_id].primary_pg.insert(tmp_pg);
              //cout << pg << ":" << over_osd << "->" << swap << endl;
              //cout << tmp_pg << ":" << swap << "->" << under_osd << endl;

              over_num++;    
              under_num--;
              remap_count += 2;
              mapped = true;
              if (!over_num || !under_num) {                           
                break;
              }
            }
            for (auto pg : to_remove) {
              over_osd_info.primary_pg.erase(pg);
            }
          } else {
            vector<int> to_remove;
            for (int pg : over_osd_info.duplicate_pg) {
              int tmp_pg = -1;
              do {
                do {
                  swap = (swap+1)%max_osd;
                } while(!osds_map.count(swap) || osds_map[swap].duplicate_pg_num-osds_map[swap].duplicate_pg.size() > 0 || swap == over_osd);
                for (int swap_pg : osds_map[swap].duplicate_pg) {
                  if (!check_collide(pg, over_osd, swap) && !check_collide(swap_pg, swap, under_osd)) {
                    tmp_pg = swap_pg;
                    break;
                  }
                }
              } while (tmp_pg == -1);
              osd_info& swap_info = osds_map[swap];
              int pos = 0;
              for (pos = 0; pos < pgs_to_osd[pg].size(); pos++) {
                if (pgs_to_osd[pg][pos] == over_osd) {
                  break;
                }
              }
              assert(pos != 0);
              pgs_to_osd[pg][pos] = swap;
              for (pos = 0; pos < pgs_to_osd[tmp_pg].size(); pos++) {
                if (pgs_to_osd[tmp_pg][pos] == swap) {
                  break;
                }
              }
              assert(pos != 0);
              pgs_to_osd[tmp_pg][swap] = under_osd;

              to_remove.push_back(pg);
              swap_info.duplicate_pg.insert(pg);
              swap_info.duplicate_pg.erase(tmp_pg);
              under_osd_info.duplicate_pg.insert(tmp_pg);
              //insert must after erase as the over_osd and swap_osd maybe in same bucket
              buckets_map[over_osd_info.bucket_id].duplicate_pg.erase(pg);
              buckets_map[swap_info.bucket_id].duplicate_pg.erase(tmp_pg);
              buckets_map[swap_info.bucket_id].duplicate_pg.insert(pg);
              buckets_map[under_osd_info.bucket_id].duplicate_pg.insert(tmp_pg);
              
              over_num++;
              under_num--;
              remap_count += 2;
              mapped = true;
              if (!over_num || !under_num) {                           
                break;
              }
            }
            for (auto pg : to_remove) {
              over_osd_info.duplicate_pg.erase(pg);
            }
          }
          if (mapped) {
            over_it->second.erase(over_osd_it);
            under_it->second.erase(under_osd_it);
            if (over_num || under_num) {
              if (over_num) {
                change_map[over_num].insert(over_osd);
              }
              if (under_num) {
                change_map[under_num].insert(under_osd);
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
            under_osd_it++;
          }
        }
        if (remapped == false) {
          under_it++;
        }
      }
    }
  }
  return remap_count;
}

/* remap some pgs into underfull osds in case of:
 *  all overfull osds remapped finish.
 *  the underfull osds more than 1 pg
 *
 * param:
 *  @change_map: 
 *  @is_primary: remap for primary_pg or duplicate_pg.
 *  @return: return the remap item count.
 */
int pool_pg_map::underfull_remap(map<int, set<int>>& change_map,
                                bool is_primary) {
  int remap_count = 0;
  int balance_osd = 0;
  int max_osd = osds_map.rbegin()->first;;
  while (!change_map.empty() && change_map.rbegin()->first > 1) {
    auto under_it = change_map.rbegin();
    auto under_osd_it = under_it->second.begin();
    int under_osd = *under_osd_it;
    int under_num = under_it->first;
    osd_info& under_osd_info = osds_map[under_osd];

    under_it->second.erase(under_osd_it);
    if (under_it->second.empty()) {
      change_map.erase(under_it->first);
    }
    
    int balance_pg = -1;
    if (is_primary) {
      do {
        do {
          balance_osd = (balance_osd+1)%max_osd;
        } while(!osds_map.count(balance_osd) || osds_map[balance_osd].primary_pg.size() == 0 ||
                osds_map[balance_osd].primary_pg_num > osds_map[balance_osd].primary_pg.size());
        for (int pg : osds_map[balance_osd].primary_pg) {
          if (!check_collide(pg, balance_osd, under_osd)) {
            balance_pg = pg;
            break;
          }
        }
      } while (balance_pg == -1);
      osd_info& balance_osd_info = osds_map[balance_osd];
      balance_osd_info.primary_pg.erase(balance_pg);
      under_osd_info.primary_pg.insert(balance_pg);
      //insert must after erase as the under_osd and balance_osd maybe in same bucket
      buckets_map[balance_osd_info.bucket_id].primary_pg.erase(balance_pg);
      buckets_map[under_osd_info.bucket_id].primary_pg.insert(balance_pg);
      pgs_to_osd[balance_pg][0] = under_osd;
      under_num--;
      remap_count++;
    } else {
      do {
        do {
          balance_osd = (balance_osd+1)%max_osd;
        } while (!osds_map.count(balance_osd) || osds_map[balance_osd].duplicate_pg.size() == 0 ||
                osds_map[balance_osd].duplicate_pg_num > osds_map[balance_osd].duplicate_pg.size());
        for (int pg : osds_map[balance_osd].duplicate_pg) {
          if (!check_collide(pg, balance_osd, under_osd)) {
            balance_pg = pg;
            break;
          }
        }
      } while (balance_pg == -1);
      osd_info& balance_osd_info = osds_map[balance_osd];
      balance_osd_info.duplicate_pg.erase(balance_pg);
      under_osd_info.duplicate_pg.insert(balance_pg);
      //insert must after erase as the under_osd and balance_osd maybe in same bucket
      buckets_map[balance_osd_info.bucket_id].duplicate_pg.erase(balance_pg);
      buckets_map[under_osd_info.bucket_id].duplicate_pg.insert(balance_pg);
      for (int i = 0; i < pgs_to_osd[balance_pg].size(); i++) {
        if (pgs_to_osd[balance_pg][i] == balance_osd) {
          pgs_to_osd[balance_pg][i] = under_osd;
          break;
        }
      }
      under_num--;
      remap_count++;
    }

    if(under_num) {
      change_map[under_num].insert(under_osd);
    }
  }
  return remap_count;
}

int pool_pg_map::preprocess_map_change(map_change& change) {
  int remap_count = 0;
  for (auto crush_remove_item : change.osd_crush_remove) {
    int osd_bucket = crush_remove_item.first;
    int osd = crush_remove_item.second;
    if (!osds_map.count(osd)) {
      cerr << __func__ << " osd:" << osd << " does not in crushmap!" << endl;
      continue;
    }
    for (auto item : change.osd_weight_change)  {
      if (item.second.first == osd) {
        cerr << __func__ << " osd:" << osd << " change multi times" << endl;
        return -1;
      }
    }
    change.osd_weight_change.push_back({osd_bucket, {osd, 0}});

    //clear the osd info in out_osd_pg_maps && out_osd_weight
    out_osd_pg_maps.erase(osd);
    out_osd_weight.erase(osd);
  }

  for (auto crush_add_item : change.osd_crush_add) {
    int osd_bucket = crush_add_item.first;
    int osd = crush_add_item.second.first;
    int weight = crush_add_item.second.second;
    if (osds_map.count(osd)) {
      cerr << __func__ << " osd:" << osd << " areadly in crushmap!" << endl;
      continue;
    }
    for (auto item : change.osd_weight_change)  {
      if (item.second.first == osd) {
        cerr << __func__ << " osd:" << osd << " change multi times" << endl;
        return -1;
      }
    }
    change.osd_weight_change.push_back({osd_bucket, {osd, weight}});
  }

  for (auto osdmap_remove_item : change.osd_osdmap_remove) {
    int osd_bucket = osdmap_remove_item.first;
    int osd = osdmap_remove_item.second;
    if (!osds_map.count(osd)) {
      cerr << __func__ << " osd:" << osd << " does not in crushmap!" << endl;
      continue;
    }

    for (auto item : change.osd_weight_change)  {
      if (item.second.first == osd) {
        cerr << __func__ << " osd:" << osd << " change multi times" << endl;
        return -1;
      }
    }
    change.osd_weight_change.push_back({osd_bucket, {osd, 0}});
    osd_info& o = osds_map[osd];
    out_osd_weight[osd] = o.weight;

    //store the pg info of out_osd into out_osd_pg_maps
    for (int pg : o.primary_pg) {
      out_osd_pg_maps[osd].insert({pg, 0});
    }
    for (int pg : o.duplicate_pg) {
      int i = 1;
      for (; i < pool_size; i++) {
        if(pgs_to_osd[pg][i] == osd) {
          break;
        }
      }
      assert(i < pool_size);
      out_osd_pg_maps[osd].insert({pg, i});
    }
  }

  for (auto osdmap_add_item : change.osd_osdmap_add) {
    int osd_bucket = osdmap_add_item.first;
    int osd = osdmap_add_item.second;
    if (!osds_map.count(osd)) {
      cerr << __func__ << " osd:" << osd << " does not in crushmap!" << endl;
      continue;
    }
    osd_info& o = osds_map[osd];
    for (auto item : change.osd_weight_change)  {
      if (item.second.first == osd) {
        cerr << __func__ << " osd:" << osd << " change multi times" << endl;
        return -1;
      }
    }
    change.osd_weight_change.push_back({osd_bucket, {osd, out_osd_weight[osd]}});

    //remap the pgs back to in osd
    for (auto pg_info : out_osd_pg_maps[osd]) {
      int pg = pg_info.first;
      int idx = pg_info.second;
      int from = pgs_to_osd[pg][idx];
      pgs_to_osd[pg][idx] = osd;
      //insert must after erase as the swap osds maybe in same bucket
      if (idx == 0) { //primary
        osds_map[from].primary_pg.erase(pg);
        buckets_map[osds_map[from].bucket_id].primary_pg.erase(pg);
        o.primary_pg.insert(pg);
        buckets_map[o.bucket_id].primary_pg.insert(pg);
      } else { //duplicate
        osds_map[from].duplicate_pg.erase(pg);
        buckets_map[osds_map[from].bucket_id].duplicate_pg.erase(pg);
        o.duplicate_pg.insert(pg);
        buckets_map[o.bucket_id].duplicate_pg.insert(pg);
      }
      remap_count++;
    }

    //clear the osd info in out_osd_pg_maps && out_osd_weight
    out_osd_pg_maps.erase(osd);
    out_osd_weight.erase(osd);
  }

  //reweight
  for (auto p : change.osd_weight_change) {
    int osd_bucket = p.first;
    int osd = p.second.first;
    int weight = p.second.second;
    if(!osds_map.count(osd)) {
      osds_map[osd] = osd_info(osd);
      assert(find(buckets_map[osd_bucket].item.begin(), buckets_map[osd_bucket].item.end(), osd) == buckets_map[osd_bucket].item.end());
      buckets_map[osd_bucket].item.push_back(osd);
    }
    
    osd_info& o = osds_map[osd];
    buckets_map[osd_bucket].weight += (weight - o.weight);
    root_weight += (weight-o.weight);
    o.weight = weight;
    o.bucket_id = osd_bucket;
  }

  return remap_count;
}

int pool_pg_map::apply_map_change(map_change& change) {
  map<int, set<int>> duplicate_change; //change_num->item
  map<int, set<int>> primary_change; //change_num->item;
  int remap_count = 0;

  int ret = preprocess_map_change(change);
  if (ret < 0) { //error
    return remap_count;
  }
  remap_count += ret;

  adjust_crushmap_pg_target(primary_change, duplicate_change);
  
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
  

  //adjust pg mappings
  cout << __func__ << " 1" << endl;
  remap_count += overfull_remap(primary_change, true);
  cout << __func__ << " 2" << endl;
  remap_count += overfull_remap(duplicate_change, false);
  cout << __func__ << " 3" << endl;
  remap_count += underfull_remap(primary_change, true);
  cout << __func__ << " 4" << endl;
  remap_count += underfull_remap(duplicate_change, false);
  cout << __func__ << " 5" << endl;

  for (auto p : change.osd_crush_remove) {
    osds_map.erase(p.second);
    int pos = 0;
    for (pos = 0; pos < buckets_map[p.first].item.size(); pos++) {
      if (buckets_map[p.first].item[pos] == p.second) {
        break;
      }
    }
    assert(pos < buckets_map[p.first].item.size());
    buckets_map[p.first].item.erase(buckets_map[p.first].item.begin()+pos);
  }

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

  return remap_count;
}
