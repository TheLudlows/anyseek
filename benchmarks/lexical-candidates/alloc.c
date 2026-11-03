// glibc-only allocation request counter. Used in a separate, untimed run.
// Counts successful malloc/calloc/realloc requests, not live memory or RSS.
#define _GNU_SOURCE
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <sys/mman.h>
#include <unistd.h>
extern void *__libc_malloc(size_t);
extern void *__libc_calloc(size_t, size_t);
extern void *__libc_realloc(void *, size_t);
static _Atomic uint64_t *counters;
__attribute__((constructor)) static void init(void) {
    const char *path = getenv("ISSUE153_ALLOC_FILE");
    if (!path) return;
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (fd < 0 || ftruncate(fd, 16)) _exit(98);
    void *p = mmap(0, 16, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (p == MAP_FAILED) _exit(98);
    counters = p;
}
static void count(void *p, size_t n) {
    if (p && counters) {
        atomic_fetch_add_explicit(&counters[0], 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&counters[1], n, memory_order_relaxed);
    }
}
void *malloc(size_t n) { void *p = __libc_malloc(n); count(p,n); return p; }
void *calloc(size_t n,size_t s) { void *p = __libc_calloc(n,s); count(p,n*s); return p; }
void *realloc(void *old,size_t n) { void *p = __libc_realloc(old,n); count(p,n); return p; }
