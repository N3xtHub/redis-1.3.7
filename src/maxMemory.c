
/* ============================ Maxmemory directive  ======================== */

/* Try to free one object form the pre-allocated objects free list.
 * This is useful under low mem conditions as by default we take 1 million
 * free objects allocated. On success REDIS_OK is returned, otherwise
 * REDIS_ERR. */
static int tryFreeOneObjectFromFreelist(void) {
    robj *o;

    if (server.vm_enabled) pthread_mutex_lock(&server.obj_freelist_mutex);
    if (listLength(server.objfreelist)) {
        listNode *head = listFirst(server.objfreelist);
        o = listNodeValue(head);
        listDelNode(server.objfreelist,head);
        if (server.vm_enabled) pthread_mutex_unlock(&server.obj_freelist_mutex);
        zfree(o);
        return REDIS_OK;
    }
}

/* This function gets called when 'maxmemory' is set on the config file to limit
 * the max memory used by the server, and we are out of memory.
 * This function will try to, in order:
 *
 * - Free objects from the free list
 * - Try to remove keys with an EXPIRE set
 *
 * It is not possible to free enough memory to reach used-memory < maxmemory
 * the server will start refusing commands that will enlarge even more the
 * memory usage.
 */
static void freeMemoryIfNeeded(void) {
    while (server.maxmemory && zmalloc_used_memory() > server.maxmemory) {
        int j, k, freed = 0;

        if (tryFreeOneObjectFromFreelist() == REDIS_OK) continue;
        for (j = 0; j < server.dbnum; j++) {
            int minttl = -1;
            robj *minkey = NULL;
            struct dictEntry *de;

            if (dictSize(server.db[j].expires)) {
                freed = 1;
                /* From a sample of three keys drop the one nearest to
                 * the natural expire */
                for (k = 0; k < 3; k++) {
                    time_t t;

                    de = dictGetRandomKey(server.db[j].expires);
                    t = (time_t) dictGetEntryVal(de);
                    if (minttl == -1 || t < minttl) {
                        minkey = dictGetEntryKey(de);
                        minttl = t;
                    }
                }
                deleteKey(server.db+j,minkey);
            }
        }
        if (!freed) return; /* nothing to free... */
    }
}
