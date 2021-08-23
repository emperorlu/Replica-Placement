#include <bits/stdc++.h>
#include <math.h>
using namespace std;

int main()
{
  int a = 900000;
  int b = 163840000*4;
  double c = 1.0*a/b;
  int d = ceil(c*65536);
  cout << a << " " << b << " " << c << " " << d << endl;
}

