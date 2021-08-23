#include <iostream>
#include <unordered_map>
using namespace std;

int main()
{
  unordered_map<int,int> mymap;
  for(int i = 0; i < 64; i++) {
    mymap.insert({i, i});
  }
  unsigned nbuckets = mymap.bucket_count();
  cout << "mymap has " << nbuckets << " buckets : " << endl;
  for(unsigned i = 0; i<nbuckets; ++i)
        cout << "bucket # " << i << " has " << mymap.bucket_size(i) << " elements " << endl;
  return 0;
}

