#include "bloomfilter.h"
#include<fcntl.h>
#include<math.h>
#include<stddef.h>
#include<stdint.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<sys/file.h>
#include<sys/mman.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<unistd.h>

// A reduced complexity, sizeof(uint64_t) only implementation of XXHASH

#define PRIME_1 11400714785074694791ULL
#define PRIME_2 14029467366897019727ULL
#define PRIME_3  1609587929392839161ULL
#define PRIME_4  9650029242287828579ULL
#define PRIME_5  2870177450012600261ULL

#ifndef MAP_HASSEMAPHORE
#define MAP_HASSEMAPHORE 0
#endif

#ifdef __GNUC__
#define __atomic_or_fetch(X, Y, Z) __sync_or_and_fetch(X, Y)
#define __atomic_fetch_sub(X, Y, Z) __sync_fetch_and_sub(X, Y)
#endif

static inline uint64_t rotl(uint64_t x, uint64_t r) {
  return ((x >> (64 - r)) | (x << r));
}

inline uint64_t xxh64(uint64_t k1) {
  uint64_t h64;
  h64  = PRIME_5 + 8;

  k1 *= PRIME_2;
  k1 = rotl(k1, 31);
  k1 *= PRIME_1;
  h64 ^= k1;
  h64 = rotl(h64, 27) * PRIME_1 + PRIME_4;
  h64 ^= h64 >> 33;
  h64 *= PRIME_2;
  h64 ^= h64 >> 29;
  h64 *= PRIME_3;
  h64 ^= h64 >> 32;
  return h64;
}


int peloton_bloomfilter_probes(double error_rate) {
  if ((error_rate <= 0) || (error_rate >= 1))
    return -1;
  return (int)(ceil(log(1 / error_rate) / log(2)));
}


size_t peloton_bloomfilter_size(uint64_t capacity, double error_rate) {
  int probes = fabs(log(1 / error_rate));
  uint64_t bits = ceil(2 * capacity * fabs(log(error_rate))) / (log(2) * log(2));
  if (bits % 64)
    bits += 64 - bits % 64;
  return bits;
}


bloomfilter_t *peloton_shared_bloomfilter(int fd, uint64_t capacity, double error_rate) {
  bloomfilter_t *bloomfilter;
  char magicbuffer[25];
  uint64_t i;
  uint64_t zero=0;
  struct stat stats;
  if (-1 == peloton_bloomfilter_probes(error_rate))
    return NULL;
  if (!(bloomfilter = malloc(sizeof(bloomfilter_t))))
    return NULL;
  flock(fd, LOCK_EX);
  if (fstat(fd, &stats)) {
    flock(fd, LOCK_UN);
    free(bloomfilter);
    return NULL;
  }
  if (stats.st_size == 0) {
    bloomfilter->probes = peloton_bloomfilter_probes(error_rate);
    bloomfilter->length = (peloton_bloomfilter_size(capacity, error_rate) + 63) / 64;
    write(fd, "SharedMemory BloomFilter", 24);
    write(fd, &capacity, sizeof(uint64_t));
    write(fd, &error_rate, sizeof(uint64_t));
    write(fd, &capacity, sizeof(uint64_t));
    for(i=0; i< bloomfilter->length; ++i)
      write(fd, &zero, sizeof(uint64_t));
  } else {
    lseek(fd, 0, 0);
    read(fd, magicbuffer, 24);
    if (strncmp(magicbuffer, "Peloton Bloom Filter 0.0", 24)) {
      flock(fd, LOCK_UN);
      free(bloomfilter);
      return NULL;
    }

    if (read(fd, &bloomfilter->capacity, sizeof(uint64_t)) < sizeof(uint64_t)) {
      flock(fd, LOCK_UN);
      free(bloomfilter);
      return NULL;
    }

    if (read(fd, &bloomfilter->error_rate, sizeof(double)) < sizeof(double)) {
      flock(fd, LOCK_UN);
      free(bloomfilter);
      return NULL;
    }
    bloomfilter->probes = peloton_bloomfilter_probes(bloomfilter->error_rate);
    bloomfilter->length = (peloton_bloomfilter_size(bloomfilter->capacity, bloomfilter->error_rate) + 63) / 64;
  }
  flock(fd, LOCK_UN);
  bloomfilter->mmap_size = 24 + sizeof(double) + sizeof(uint64_t)*3 + bloomfilter->length * sizeof(uint64_t);
  bloomfilter->mmap = mmap(NULL,
                           bloomfilter->mmap_size,
                           PROT_READ | PROT_WRITE,
                           MAP_SHARED | MAP_HASSEMAPHORE,
                           fd,
                           0);
  if (!bloomfilter->mmap) {
    free(bloomfilter);
    return NULL;
  }
  madvise(bloomfilter->mmap, bloomfilter->mmap_size, MADV_RANDOM);
  bloomfilter->counter = bloomfilter->mmap + 24 + sizeof(double) + sizeof(uint64_t);
  bloomfilter->bits = bloomfilter->counter + sizeof(uint64_t);
  return bloomfilter;
}


bloomfilter_t *peloton_private_bloomfilter(uint64_t capacity, double error_rate) {
  bloomfilter_t *bloomfilter;
  int probes = peloton_bloomfilter_probes(error_rate);
  if (probes == -1)
    return NULL;

  if (!(bloomfilter = malloc(sizeof(bloomfilter_t))))
    return NULL;

  bloomfilter->fd = 0;
  bloomfilter->capacity = capacity;
  bloomfilter->error_rate = error_rate;
  bloomfilter->length = (peloton_bloomfilter_size(capacity, error_rate) + 63 ) / 64;
  bloomfilter->probes = probes;
  bloomfilter->mmap_size = 0;
  bloomfilter->mmap = NULL;
  if (!(bloomfilter->bits = calloc(sizeof(uint64_t), bloomfilter->length))) {
    free(bloomfilter);
    return NULL;
  }
  bloomfilter->counter = &bloomfilter->local_counter;

  bloomfilter->local_counter = capacity;
  bloomfilter->invert = 0;
  return bloomfilter;
}

void peloton_destroy_bloomfilter(bloomfilter_t *bloomfilter) {
  if (bloomfilter->mmap && bloomfilter->mmap_size) {
    munmap(bloomfilter->mmap, bloomfilter->mmap_size);
  } else {
    free(bloomfilter->bits);
  }
  free(bloomfilter);
}

uint64_t peloton_bloomfilter_len(bloomfilter_t *bloomfilter) {
  return __atomic_fetch_sub(bloomfilter->counter, (uint64_t) 0, 0);
}

int peloton_add_to_bloomfilter(uint64_t hash, bloomfilter_t *bloomfilter) {
  uint64_t *data = __builtin_assume_aligned(bloomfilter->bits, 16);
  int probes = bloomfilter->probes;
  size_t length = bloomfilter->length;
  size_t i;
  uint64_t added;

  added=(__atomic_fetch_sub(bloomfilter->counter, (uint64_t)1, 0));
  if (added == 0 || added > bloomfilter->capacity) {
    peloton_clear_bloomfilter(bloomfilter);
  }

  while (probes--) {
    __atomic_or_fetch(data + (hash >> 6) % length, 1<<(hash & 0x3f), 1);
    hash = xxh64(hash);
  }
  return !added;
}


int shared_memory_bloomfilter_(uint64_t hash, bloomfilter_t *bloomfilter) {
  uint64_t *data = __builtin_assume_aligned(bloomfilter->bits, 16);
  int probes = bloomfilter->probes;
  size_t length = bloomfilter->length;

  while (probes--) {
    if (!(1<<(hash & 0x3f) & __atomic_or_fetch(data + (hash >> 6) % length, 0, 1)))
      return 0;
    hash = xxh64(hash);
  }
  return 1;
}

inline void shared_memory_bloomfilter_clear(bloomfilter_t *bloomfilter) {
  size_t length = bloomfilter->length;
  size_t i;
  uint64_t *data = __builtin_assume_aligned(bloomfilter->bits, 16);
  for(i=0; i<length; ++i)
    data[i] = 0;
  *bloomfilter->counter = bloomfilter->capacity;
}

uint64_t shared_memory_bloomfilter_len(bloomfilter_t *bloomfilter) {
  size_t length = bloomfilter->length;
  size_t i;
  uint64_t *data = __builtin_assume_aligned(bloomfilter->bits, 16);
  uint64_t population = 0;
  for(i=0; i<length; ++i)
    population += __builtin_popcountll(data[i]);
  return population;
}


static PyMethodDef shared_memory_bloomfilter_methods[] = {
  {"add", (PyCFunction)shared_memory_bloomfilter_contains, METH_O, NULL},
  {"__len__", (PyCFunction)shared_memory_bloomfilter_len, METH_NOARGS, NULL},
  {"clear", (PyCFunction)shared_memory_bloomfilter_clear, METH_O, NULL},
  {"__contains__", (PyCFunction)pshared_memory_bloomfilter_contains, METH_O, NULL},
  {NULL, NULL}
};


PyTypeObject PelotonLeaderboard_Type = {
  PyVarObject_HEAD_INIT(&PyType_Type, 0)
  "SharedMemoryBloomfilter", /* tp_name */
  sizeof(SharedMemoryBloomfilterObject), /*tp_basicsize*/
  0, /*tp_itemsize */
  (destructor)shared_memory_bloomfilter_type_dealloc, /* tp_dealloc */
  0, /* tp_print */
  0, /*tp_getattr*/
  0, /*tp_setattr*/
  0, /*tp_cmp*/
  0, /*tp_repr*/
  0, /* tp_as_number */
  0, /* tp_as_sequence */
  0, /* tp_as_mapping */
  (hashfunc)PyObject_HashNotImplemented, /*tp_hash */
  0, /* tp_call */
  0, /* tp_str */
  PyObject_GenericGetAttr, /* tp_getattro */
  0, /* tp_setattro */
  0, /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_CHECKTYPES |
  Py_TPFLAGS_BASETYPE,	/* tp_flags */
  0, /* tp_doc */
  0, /* tp_traverse */
  0, /* tp_clear */
  0, /* tp_richcompare */
  offsetof(SharedMemoryBloomfilterObject, weakreflist), /* tp_weaklistoffset */
  0, /* tp_iter */
  0, /* tp_iternext */
  shared_memory_bloomfilter_methods, /* tp_methods */
  0, /* tp_members */
  0, /* tp_genset */
  0, /* tp_base */
  0, /* tp_dict */
  0,				/* tp_descr_get */
  0,				/* tp_descr_set */
  0,				/* tp_dictoffset */
  (initproc)peloton_leaderboard_init,		/* tp_init */
  PyType_GenericAlloc,		/* tp_alloc */
  leaderboard_new,			/* tp_new */
  PyObject_GC_Del,		/* tp_free */
};

static PyObject *
make_new_leaderboard(PyTypeObject *type, const char *path) {
  LeaderboardObject *so = PyObject_GC_New(LeaderboardObject, &PelotonLeaderboard_Type);;
  PyObject_GC_Track(so);
  if (!(so->lb = open_l(path))) {
    return NULL;
  }
  so->weakreflist = NULL;
  return (PyObject *)so;
}


static PyMethodDef shared_memory_bloomfiltermodule_methods[] = {
  {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC
initshared_memory_bloomfilter(void) {
  PyObject *m = Py_InitModule("shared_memory_bloomfilter", shared_memory_bloomfiltermodule_methods);
  Py_INCREF(&PelotonLeaderboard_Type);
  PyModule_AddObject(m, "Leaderboard", (PyObject *)&PelotonLeaderboard_Type);
};
