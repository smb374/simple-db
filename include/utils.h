#ifndef UTILS_H
#define UTILS_H

#include <stdint.h>

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

typedef int8_t i8;
typedef int16_t i16;
typedef int32_t i32;
typedef int64_t i64;

#define MIN(x, y) ((y) ^ (((x) ^ (y)) & -((x) < (y))))
#define MAX(x, y) ((x) ^ (((x) ^ (y)) & -((x) < (y))))

#define MAX_NAME 64

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
