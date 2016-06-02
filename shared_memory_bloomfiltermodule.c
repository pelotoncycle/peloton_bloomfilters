#include "shared_memory_bloomfiltermodule.h"
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



typedef struct _shared_memory_bloomfilter_object SharedMemoryBloomfilterObject;
struct _shared_memory_bloomfilter_object {
  PyObject HEAD;
  bloomfilter_t *bf;
  PyObject *weakreflist;
};

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


inline int bloomfilter_probes(double error_rate) {
  if ((error_rate <= 0) || (error_rate >= 1))
    return -1;
  return (int)(ceil(log(1 / error_rate) / log(2)));
}


size_t bloomfilter_size(uint64_t capacity, double error_rate) {
  int probes = fabs(log(1 / error_rate));
  uint64_t bits = ceil(2 * capacity * fabs(log(error_rate))) / (log(2) * log(2));
  if (bits % 64)
    bits += 64 - bits % 64;
  return bits;
}

const char HEADER[] = "SharedMemory BloomFilter";


bloomfilter_t *create_bloomfilter(int fd, uint64_t capacity, double error_rate) {
  bloomfilter_t *bloomfilter;
  char magicbuffer[25];
  uint64_t i;
  uint64_t zero=0;
  struct stat stats;
  if (-1 == bloomfilter_probes(error_rate))
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
    bloomfilter->probes = bloomfilter_probes(error_rate);
    bloomfilter->length = (bloomfilter_size(capacity, error_rate) + 63) / 64;
    write(fd, HEADER, 24);
    write(fd, &capacity, sizeof(uint64_t));
    write(fd, &error_rate, sizeof(uint64_t));
    write(fd, &capacity, sizeof(uint64_t));
    for(i=0; i< bloomfilter->length; ++i)
      write(fd, &zero, sizeof(uint64_t));
  } else {
    lseek(fd, 0, 0);
    read(fd, magicbuffer, 24);
    if (strncmp(magicbuffer, HEADER, 24)) {
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
    bloomfilter->probes = bloomfilter_probes(bloomfilter->error_rate);
    bloomfilter->length = (bloomfilter_size(bloomfilter->capacity, bloomfilter->error_rate) + 63) / 64;
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



void shared_memory_bloomfilter_destroy(bloomfilter_t *bloomfilter) {
  if (bloomfilter->mmap && bloomfilter->mmap_size) {
    munmap(bloomfilter->mmap, bloomfilter->mmap_size);
  } else {
    free(bloomfilter->bits);
  }
  free(bloomfilter);
}

PyObject *
shared_memory_bloomfilter_len(SharedMemoryBloomfilterObject *smbo, PyObject *_) {
  bloomfilter_t *bloomfilter = smbo->bf;
  return PyInt_FromLong(bloomfilter->counter);
}

PyObject *
shared_memory_bloomfilter_add(SharedMemoryBloomfilterObject *smbo, PyObject *item) {
  uint64_t *data = __builtin_assume_aligned(bloomfilter->bits, 16);
  bloomfilter_t *bloomfilter = smbo->bf;
  int probes = bloomfilter->probes;
  size_t length = bloomfilter->length;
  uint64_t hash = PyObject_Hash(item);
  if (hash == (uint64_t)(-1))
    return NULL;

  uint64_t cleared=!(__atomic_fetch_sub(bloomfilter->counter, (uint64_t)1, 0));
  if (cleared || added > bloomfilter->capacity) {
    Py_DECREF(shared_memory_bloomfilter_clear(smbo, NULL);
  }

  while (probes--) {
    __atomic_or_fetch(data + (hash >> 6) % length, 1<<(hash & 0x3f), 1);
    hash = xxh64(hash);
  }
  return PyBool_FromLong(cleared);
}


PyObject *
shared_memory_bloomfilter_contains(SharedMemoryBloomfilterObject *smbo, PyObject *item) {
  bloomfilter_t *bloomfilter = smbo->bf;
  uint64_t *data = __builtin_assume_aligned(bloomfilter->bits, 16);
  int probes = bloomfilter->probes;
  size_t length = bloomfilter->length;
  uint64_t hash = PyObject_Hash(item);
  if (hash == (uint64_t)(-1))
    return NULL;

  while (probes--) {
    if (!(1<<(hash & 0x3f) & __atomic_or_fetch(data + (hash >> 6) % length, 0, 1)))
       Py_RETURN_FALSE;
    hash = xxh64(hash);
  }
  Py_RETURN_TRUE;
}

inline PyObject *
shared_memory_bloomfilter_clear(SharedMemoryBloomfilterObject *smbo, PyObject *_) {
  struct bloomfilter_t *bf = smbo->bf;
  size_t length = bf->length;
  size_t i;
  uint64_t *data = __builtin_assume_aligned(bf->bits, 16);
  for(i=0; i<length; ++i)
    data[i] = 0;
  *bf->counter = bf->capacity;
  Py_RETURN_NONE;
}

PyObject *
shared_memory_bloomfilter_population(SharedMemoryBloomfilterObject *smbo, PyObject *_) {
  size_t length = smbo->bf->length;
  size_t i;
  uint64_t *data = __builtin_assume_aligned(smbo->bf->bits, 16);
  uint64_t population = 0;
  for(i=0; i<length; ++i)
    population += __builtin_popcountll(data[i]);
  return PyInt_FromLong(population);
}


static PyMethodDef shared_memory_bloomfilter_methods[] = {
  {"add", (PyCFunction)shared_memory_bloomfilter_add, METH_O, NULL},
  {"__len__", (PyCFunction)shared_memory_bloomfilter_len, METH_NOARGS, NULL},
  {"clear", (PyCFunction)shared_memory_bloomfilter_clear, METH_O, NULL},
  {"__contains__", (PyCFunction)shared_memory_bloomfilter_contains, METH_O, NULL},
  {"population", (PyCFunction)shared_memory_bloomfilter_population, METH_NOARGS, NULL},
  {NULL, NULL}
};


PyTypeObject SharedMemoryBloomfilterType = {
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
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_CHECKTYPES |
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
  (initproc)shared_memory_bloomfilter_init,		/* tp_init */
  PyType_GenericAlloc,		/* tp_alloc */
  shared_memory_bloomfilter_new,			/* tp_new */
  PyObject_GC_Del,		/* tp_free */
};

static void shared_memory_bloomfilter_type_dealloc(SharedMemoryBloomfilterObject *smbo) {
    Py_TRASHCAN_SAFE_BEGIN(smbo);
  if (smbo->weakreflist != NULL)
    PyObject_ClearWeakRefs((PyObject *)smbo);
  shared_memory_bloomfilter_destroy(smbo->bf);

  Py_TRASHCAN_SAFE_END(smbo);
}

static PyObject *
shared_memory_bloomfilter_new(PyTypeObject *type, PyObject *args, PyObject **kwargs) {

  int fd;
  uint64_t capacity;
  double error_rate;
  PyArg_ParseTupleAndKeywords
    (args,
     kwargs,
     "i|ld",
     {"fp", "capacity", "error_rate"},
     &fd,
     &capacity,
     &error_rate);
  			      
			      
  SharedMemoryBloomfilterObject *smbo = PyObject_GC_New(SharedMemoryBloomfilterObject, &SharedMemoryBloomfilterType);;
  
  if (!(smbo->bf= create_bloomfilter(fd, capacity, error_rate))) {
    return NULL;
  }
  so->weakreflist = NULL;
  return (PyObject *)smbo;
}


static PyMethodDef shared_memory_bloomfiltermodule_methods[] = {
  {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC
initshared_memory_bloomfilter(void) {
  PyObject *m = Py_InitModule("shared_memory_bloomfilter", shared_memory_bloomfiltermodule_methods);
  Py_INCREF(&SharedMemoryBloomfilterType);
  PyModule_AddObject(m, "SharedMemoryBloomFilter", (PyObject *)&SharedMemoryBloomfilterType);
};
