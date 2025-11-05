#ifndef UTILS_H
#define UTILS_H

#include <fcntl.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

typedef int8_t i8;
typedef int16_t i16;
typedef int32_t i32;
typedef int64_t i64;

#define IS_POW_2(n) (((n) > 0) && (((n) & ((n) - 1)) == 0))
#define MIN(x, y) ((y) ^ (((x) ^ (y)) & -((x) < (y))))
#define MAX(x, y) ((x) ^ (((x) ^ (y)) & -((x) < (y))))

void logger(FILE *f, const char *tag, const char *format, ...);
i32 open_relative(const char *path, i32 flag, mode_t mode);

// Atomics
typedef _Atomic(u8) atomic_u8;
typedef _Atomic(u16) atomic_u16;
typedef _Atomic(u32) atomic_u32;
typedef _Atomic(u64) atomic_u64;

typedef _Atomic(i8) atomic_i8;
typedef _Atomic(i16) atomic_i16;
typedef _Atomic(i32) atomic_i32;
typedef _Atomic(i64) atomic_i64;

enum MemOrderAlias {
    RELAXED = memory_order_relaxed,
    CONSUME = memory_order_consume,
    ACQUIRE = memory_order_acquire,
    RELEASE = memory_order_release,
    ACQ_REL = memory_order_acq_rel,
    SEQ_CST = memory_order_seq_cst,
};

#define LOAD(t, o) atomic_load_explicit((t), (o))
#define STORE(t, v, o) atomic_store_explicit((t), (v), (o))
#define CMPXCHG(t, e, v, os, of) atomic_compare_exchange_strong_explicit((t), (e), (v), (os), (of))
#define WCMPXCHG(t, e, v, os, of) atomic_compare_exchange_weak_explicit((t), (e), (v), (os), (of))
#define XCHG(t, v, o) atomic_exchange_explicit((t), (v), (o))
#define FADD(t, v, o) atomic_fetch_add_explicit((t), (v), (o))
#define FSUB(t, v, o) atomic_fetch_sub_explicit((t), (v), (o))
#define FAND(t, v, o) atomic_fetch_and_explicit((t), (v), (o))
#define FOR(t, v, o) atomic_fetch_or_explicit((t), (v), (o))
#define FXOR(t, v, o) atomic_fetch_xor_explicit((t), (v), (o))

// Endian aware load/store
u16 load16le(const u8 *src);
u32 load32le(const u8 *src);
u64 load64le(const u8 *src);
u16 load16be(const u8 *src);
u32 load32be(const u8 *src);
u64 load64be(const u8 *src);

void store16le(u16 val, u8 *dest);
void store32le(u32 val, u8 *dest);
void store64le(u64 val, u8 *dest);
void store16be(u16 val, u8 *dest);
void store32be(u32 val, u8 *dest);
void store64be(u64 val, u8 *dest);

#endif /* ifndef UTILS_H */
