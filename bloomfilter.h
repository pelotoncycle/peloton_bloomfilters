#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>

typedef struct {
  int fd;
  uint64_t capacity;
  double error_rate;
  uint64_t length;
  int probes;
  void *mmap;
  size_t mmap_size;
  uint64_t *bits;
  uint64_t *counter;
  uint64_t local_counter;
  int invert;
} bloomfilter_t;

uint64_t xxh64(uint64_t k1);
int peloton_bloomfilter_probes(double error_rate);
size_t peloton_bloomfilter_size(uint64_t capacity, double error_rate);
bloomfilter_t *peloton_shared_bloomfilter(int fd, uint64_t capacity, double error_rate);
bloomfilter_t *peloton_private_bloomfilter(uint64_t capacity, double error_rate);
void peloton_destroy_bloomfilter(bloomfilter_t *bloomfilter);
int peloton_add_to_bloomfilter(uint64_t hash, bloomfilter_t *bloomfilter);
uint64_t peloton_bloomfilter_len(bloomfilter_t *bloomfilter);
int peloton_test_bloomfilter(uint64_t hash, bloomfilter_t *bloomfilter);
void peloton_clear_bloomfilter(bloomfilter_t *bloomfilter);
uint64_t peloton_bloomfilter_population(bloomfilter_t *bloomfilter);


#endif //BLOOMFILTER_H
