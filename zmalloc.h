/* zmalloc - total amount of allocated memory aware version of malloc()
 */

#ifndef _ZMALLOC_H
#define _ZMALLOC_H

void *zmalloc(size_t size);
void *zrealloc(void *ptr, size_t size);
void zfree(void *ptr);
char *zstrdup(const char *s);
size_t zmalloc_used_memory(void);
void zmalloc_enable_thread_safeness(void);

#endif /* _ZMALLOC_H */
