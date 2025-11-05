#include "utils.h"

#include <stdarg.h>
#include <stdio.h>
#include <time.h>

void logger(FILE *f, const char *tag, const char *format, ...) {
#ifdef LOGGING
    struct timespec tp;
    clock_gettime(CLOCK_MONOTONIC, &tp);
    fprintf(f, "%ld.%ld [%s] ", tp.tv_sec, tp.tv_nsec, tag);
    va_list args;
    va_start(args, format);
    vfprintf(f, format, args);
    va_end(args);
#endif
}

/* LOAD */
u16 load16le(const u8 *src) { return (u16) (src[0] << 0) | (u16) (src[1] << 8); }

u32 load32le(const u8 *src) {
    return ((u32) src[0] << 0) | ((u32) src[1] << 8) | ((u32) src[2] << 16) | ((u32) src[3] << 24);
}

u64 load64le(const u8 *src) {
    return ((u64) src[0] << 0) | ((u64) src[1] << 8) | ((u64) src[2] << 16) | ((u64) src[3] << 24) |
           ((u64) src[4] << 32) | ((u64) src[5] << 40) | ((u64) src[6] << 48) | ((u64) src[7] << 56);
}

u16 load16be(const u8 *src) { return (u16) (src[0] << 8) | (u16) (src[1] << 0); }

u32 load32be(const u8 *src) {
    return ((u32) src[0] << 24) | ((u32) src[1] << 16) | ((u32) src[2] << 8) | ((u32) src[3] << 0);
}

u64 load64be(const u8 *src) {
    return ((u64) src[0] << 56) | ((u64) src[1] << 48) | ((u64) src[2] << 40) | ((u64) src[3] << 32) |
           ((u64) src[4] << 24) | ((u64) src[5] << 16) | ((u64) src[6] << 8) | ((u64) src[7] << 0);
}

/* STORE */
void store16le(const u16 val, u8 *dest) {
    dest[0] = (val >> 0);
    dest[1] = (val >> 8);
}

void store32le(const u32 val, u8 *dest) {
    dest[0] = (val >> 0);
    dest[1] = (val >> 8);
    dest[2] = (val >> 16);
    dest[3] = (val >> 24);
}

void store64le(const u64 val, u8 *dest) {
    dest[0] = (val >> 0);
    dest[1] = (val >> 8);
    dest[2] = (val >> 16);
    dest[3] = (val >> 24);
    dest[4] = (val >> 32);
    dest[5] = (val >> 40);
    dest[6] = (val >> 48);
    dest[7] = (val >> 56);
}

void store16be(const u16 val, u8 *dest) {
    dest[0] = (val >> 8);
    dest[1] = (val >> 0);
}

void store32be(const u32 val, u8 *dest) {
    dest[0] = (val >> 24);
    dest[1] = (val >> 16);
    dest[2] = (val >> 8);
    dest[3] = (val >> 0);
}

void store64be(const u64 val, u8 *dest) {
    dest[0] = (val >> 56);
    dest[1] = (val >> 48);
    dest[2] = (val >> 40);
    dest[3] = (val >> 32);
    dest[4] = (val >> 24);
    dest[5] = (val >> 16);
    dest[6] = (val >> 8);
    dest[7] = (val >> 0);
}
