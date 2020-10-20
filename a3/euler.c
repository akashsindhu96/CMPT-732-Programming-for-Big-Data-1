#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int main(int argc, const char* argv[] ) {
    unsigned long n = atoi(argv[1]);
    unsigned long count = 0;    
    double sum;
    
    srand(time(NULL));
    
    for (int i=0; i<n; i++) {
        sum = 0.0;
        while ( sum < 1.0 ) {
            sum += (double)rand()/RAND_MAX;
            count++;
        }
    }
    printf("%g\n", ((double)count)/((double)n));
}